#include "../../include/lsm/engine.h"
#include "../../include/lsm/transaction.h"
#include "../../include/utils/files.h"
#include "../../include/utils/set_operation.h"
#include "spdlog/spdlog.h"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace tiny_lsm {

inline std::string isolation_level_to_string(const IsolationLevel &level) {
  switch (level) {
  case IsolationLevel::READ_UNCOMMITTED:
    return "READ_UNCOMMITTED";
  case IsolationLevel::READ_COMMITTED:
    return "READ_COMMITTED";
  case IsolationLevel::REPEATABLE_READ:
    return "REPEATABLE_READ";
  case IsolationLevel::SERIALIZABLE:
    return "SERIALIZABLE";
  default:
    return "UNKNOWN";
  }
}

// *********************** TranContext ***********************
TranContext::TranContext(uint64_t tranc_id, std::shared_ptr<LSMEngine> engine,
                         std::shared_ptr<TranManager> tranManager,
                         const enum IsolationLevel &isolation_level)
: engine_(engine), tranManager_(tranManager), tranc_id_(tranc_id), isolation_level_(isolation_level)
{
  operations.push_back(Record::createRecord(tranc_id_));
}

void TranContext::put(const std::string &key, const std::string &value) {

  if (!rollback_map_.contains(key)) {
    // 查找这个 key 的 value 和 tranc_id
    // 在这个事务开始前的 key 数据
    auto res = engine_->get(key, 0);
    if (!res.has_value()) rollback_map_[key] = std::nullopt;
    else rollback_map_[key] = res.value();
  }

  if (value == "") {
    operations.push_back(Record::deleteRecord(tranc_id_, key));
  } else {
    operations.push_back(Record::putRecord(tranc_id_, key, value));
  }

  // 对于 Read Uncommited ，直接修改数据库
  if (isolation_level_ == IsolationLevel::READ_UNCOMMITTED) {
      // 对于读未提交的 kv 引擎，put 操作是直接写入引擎中的
      engine_->put(key, value, tranc_id_);
      return;
  }

  temp_map_[key] = value;
}

void TranContext::remove(const std::string &key) {
  put(key, "");
}

std::optional<std::string> TranContext::get(const std::string &key) {

  // 就近先在本事务 put 的数据进行查找
  // Read Uncommited 不是在这里查找，但是因为其 put 不会对 temp_map 产生影响，所以直接查
  if (temp_map_.contains(key)) {
    return temp_map_[key];
  }

  switch (isolation_level_) {
    case IsolationLevel::READ_UNCOMMITTED: {
      // 读未提交，读的时候就直接从引擎中读最新的就行
      // 这里查询用 tranc_id = 0 来表示不开启事务，这样就会直接查询到最新的数据
      auto res = engine_->get(key, 0);
      if (!res.has_value()) return std::nullopt;
      return res.value().first;
    }
    case IsolationLevel::READ_COMMITTED: {
      // 读已提交，读的是当前“最大已提交事务”的可见版本
      // 注意：数据库可能是不同隔离级别的并发，所以需要考虑 RU 的情况
      auto res = engine_->get(key, tranManager_->get_max_finished_tranc_id());
      if (!res.has_value()) return std::nullopt;
      return res.value().first;
    }
    case IsolationLevel::REPEATABLE_READ: {
      // 可重复读实现了快照读
      if (read_map_.contains(key)) {
        if (!read_map_[key].has_value()) return std::nullopt;
        return read_map_[key].value().first;
      }
      auto res = engine_->get(key, tranManager_->get_max_finished_tranc_id());
      read_map_[key] = res;
      if (!res.has_value()) return std::nullopt;
      return res.value().first;
    }
    case IsolationLevel::SERIALIZABLE: {
      // 乐观序列化隔离保证了 commit 的先后顺序
      if (read_map_.contains(key)) {
        if (!read_map_[key].has_value()) return std::nullopt;
        return read_map_[key].value().first;
      }
      auto res = engine_->get(key, tranManager_->get_max_finished_tranc_id());
      read_map_[key] = res;
      if (!res.has_value()) return std::nullopt;
      return res.value().first;
      break;
    }
  }
  return {};
}

bool TranContext::commit(bool test_fail) {

  operations.push_back(Record::commitRecord(tranc_id_));

  // 把这个事务的 operations 写入 WAL 中
  tranManager_->write_to_wal(operations);

  if (isolation_level_ == IsolationLevel::READ_UNCOMMITTED) {
    // RU 不需要冲突检测，因为它允许其他事务直接对数据库修改
    tranManager_->update_max_finished_tranc_id(tranc_id_);
    tranManager_->add_ready_to_flush_tranc_id(tranc_id_, TransactionState::COMMITTED);
    return true;
  }

  // 实现冲突检测: 自己修改的数据，其现在在库中的数据是否和我事务开始前的相同？
  // 需要回滚：我更新的数据被更大的事务更新了，我再进行更新则会发生覆盖，因此我需要回滚

  // 注意：在实现冲突检测时，是不能对数据库进行修改的
  MemTable& memtable = engine_->memtable;
  // 对 组件 加锁
  // 这里需要使用侵入式加锁，虽然组件本身内部有锁，但是智能保证单次操作加锁，我们这里需要实现对整个流程加锁
  std::unique_lock<std::shared_mutex> wlock1(memtable.frozen_mtx);
  std::unique_lock<std::shared_mutex> wlock2(memtable.cur_mtx);
  std::shared_lock<std::shared_mutex> slock(engine_->ssts_mtx);

  // 1. 写集写写冲突检测（基于版本号，忽略可见性）
  for (const auto &entry: temp_map_) {
    const auto &k = entry.first;
    // 先查 memtable
    auto mem_res = engine_->memtable.get_(k, 0);
    if (mem_res.is_valid() && mem_res.get_tranc_id() > tranc_id_) {
      tranManager_->add_ready_to_flush_tranc_id(tranc_id_, TransactionState::ABORTED);
      return false;
    }
    // 如有必要再查 sst（仅当磁盘上可能存在更大事务号）
    if (tranManager_->get_max_flushed_tranc_id() > tranc_id_) {
      auto sst_res = engine_->sst_get_(k, 0);
      if (sst_res.has_value()) {
        if (sst_res->second > tranc_id_) {
          tranManager_->add_ready_to_flush_tranc_id(tranc_id_, TransactionState::ABORTED);
          return false;
        }
      }
    }
  }

  // 2. 读集校验（仅 RR 需要）
  // RR 需要自己读的所有数据相同，且这个数据没有在事务执行时被修改，若被修改，则回滚
  if (isolation_level_ == IsolationLevel::REPEATABLE_READ) {
    for (const auto &kv: read_map_) {
      const auto &k = kv.first;
      // 自己事务更改的键跳过
      if (temp_map_.contains(k)) continue;

      // 构造当前“原始”可见（忽略可见性）值：优先 memtable，再 sst
      std::optional<std::pair<std::string, uint64_t>> cur;
      auto mem_res = engine_->memtable.get_(k, 0);
      if (mem_res.is_valid()) {
        const auto &val = mem_res.get_value();
        if (val.size() == 0) {
          cur = std::nullopt; // tombstone
        } else {
          cur = std::make_pair(val, mem_res.get_tranc_id());
        }
      } else {
        auto sst_res = engine_->sst_get_(k, 0);
        if (sst_res.has_value()) {
          if (sst_res->first.size() == 0) cur = std::nullopt; // tombstone
          else cur = sst_res;
        } else {
          cur = std::nullopt;
        }
      }

      if (cur != kv.second) {
        tranManager_->add_ready_to_flush_tranc_id(tranc_id_, TransactionState::ABORTED);
        return false;
      }
    }
  }

  // 3. 冲突检测成功，应用写集到 memtable（已持有写锁，使用无锁接口）
  isCommited = true;
  tranManager_->update_max_finished_tranc_id(tranc_id_);
  tranManager_->add_ready_to_flush_tranc_id(tranc_id_, TransactionState::COMMITTED);
  if (!test_fail) {
    for (const auto &entry: temp_map_) {
      engine_->memtable.put_(entry.first, entry.second, tranc_id_);
    }
    // 标记一个空写以推进快照边界（与查询语义保持一致）
    // 标记空写：使提交水位对齐，保证了数据库中至少出现一次本事务的 tranc_id
    engine_->memtable.put_("", "", tranc_id_);
  }

  return true;
}

bool TranContext::abort() {

  operations.push_back(Record::rollbackRecord(tranc_id_));
  // abort 用于回滚事务，具体来说就是你在这个事务内修改了啥都得回去
  if (isolation_level_ == IsolationLevel::READ_UNCOMMITTED) {
    for (auto kvs: rollback_map_) {
      if (kvs.second.has_value()) {
        engine_->put(kvs.first, kvs.second.value().first, kvs.second.value().second);
      } else {
        // 之前不存在，需要移除当前事务写入
        engine_->remove(kvs.first, tranc_id_);
      }
    }
    isAborted = true;
    return true;
  }
  // 非 RU 下未真正写入引擎，直接标记终止
  isAborted = true;
  return true;
}

enum IsolationLevel TranContext::get_isolation_level() {
  return isolation_level_;
}

// *********************** TranManager ***********************
TranManager::TranManager(std::string data_dir) : data_dir_(data_dir) {
  // 初始化时读取持久化的事务状态信息

  auto file_path = get_tranc_id_file_path();
  if (std::filesystem::exists(file_path)) {
    tranc_id_file_ = FileObj::open(file_path, false);
    read_tranc_id_file();
  } else {
    tranc_id_file_ = FileObj::open(file_path, true);
    flushedTrancIds_.insert(0);
  }
}

std::string TranManager::get_wal_dir() {
  if (data_dir_.empty()) {
    data_dir_ = "./";
  }
  return data_dir_;
  return data_dir_ + "/wal";
}

void TranManager::init_new_wal() {
  // 初始化 wal
  // 因为在初始化的时候已经把 wal 目录中所有操作重放了，因此需要把文件清除掉
  for (auto &entry: std::filesystem::directory_iterator(get_wal_dir())) {
    if (entry.path().filename().string().find("wal.")) {
      std::filesystem::remove(entry.path());
    }
  }
  wal = std::make_shared<WAL>(get_wal_dir(), 128, get_max_flushed_tranc_id(), 1, 4096);
  flushedTrancIds_.clear();
  flushedTrancIds_.insert(nextTransactionId_.load() - 1);
}

void TranManager::set_engine(std::shared_ptr<LSMEngine> engine) {
  engine_ = std::move(engine);
}

TranManager::~TranManager() { write_tranc_id_file(); }

void TranManager::write_tranc_id_file() {
  // 持久化事务状态信息
  mutex_.lock();
  // 清空原来的文件信息
  tranc_id_file_ = FileObj::open(get_tranc_id_file_path(), true);
  // 把元数据持久化
  // 持久化用 memcpy 直接读写 std::atomic<uint64_t>，以及用 sizeof(std::atomic<uint64_t>)
  // 作为字节数是不可靠/不可移植的；应该 load()/store() 到普通 uint64_t 再序列化。
  // 存储信息：next事务id、最大已提交事务id、所有的已持久化事务id
  size_t bytes = sizeof(uint64_t) * 2 + sizeof(uint64_t) + sizeof(uint64_t) * flushedTrancIds_.size();
  std::vector<uint8_t> file_content(bytes);
  size_t offset = 0;

  uint64_t nxt_tranc_id = nextTransactionId_.load();
  memcpy(file_content.data() + offset, &nxt_tranc_id, sizeof(uint64_t));
  offset += sizeof(uint64_t);

  uint64_t max_tranc_id = max_finished_tranc_id.load();
  memcpy(file_content.data() + offset, &max_tranc_id, sizeof(uint64_t));
  offset += sizeof(uint64_t);

  uint64_t size_flushed = flushedTrancIds_.size();
  memcpy(file_content.data() + offset, &size_flushed, sizeof(uint64_t));
  offset += sizeof(uint64_t);

  for (auto &tranc_id: flushedTrancIds_) {
    memcpy(file_content.data() + offset, &tranc_id, sizeof(uint64_t));
    offset += sizeof(uint64_t);
  }

  tranc_id_file_.write(0, file_content);
  tranc_id_file_.sync();
  mutex_.unlock();
}

void TranManager::read_tranc_id_file() {
  // 读取持久化的事务状态信息
  // 因为这个函数只有在构造函数才会用，所以不需要加锁
  // 下一个事务 id 在最前面
  nextTransactionId_ = tranc_id_file_.read_uint64(0);
  // 后面存最大已提交的 事务id
  max_finished_tranc_id = tranc_id_file_.read_uint64(sizeof(uint64_t));
  // 后面存储所有持久化 id
  uint64_t size = tranc_id_file_.read_uint64(sizeof(uint64_t) * 2);
  size_t offset = 3 * sizeof(uint64_t);

  for (int i = 0; i < size; i ++ ) {
    uint64_t flushed_id = tranc_id_file_.read_uint64(offset);
    offset += sizeof(uint64_t);
    flushedTrancIds_.insert(flushed_id);
  }
}

// 最大已持久化事务id
uint64_t TranManager::get_max_flushed_tranc_id() {
  return *flushedTrancIds_.rbegin();
}

// 连续的最大的已持久化的事务id
uint64_t TranManager::get_checkpoint_tranc_id() {
  return *flushedTrancIds_.begin();
}

std::set<uint64_t>& TranManager::get_flushed_tranc_ids() {
  return flushedTrancIds_;
}


void TranManager::add_flushed_tranc_id(uint64_t tranc_id) {
  std::unique_lock<std::mutex> slock(mutex_);
  std::vector<uint64_t> need_remove;
  for (auto &[readyId, state]: readyToFlushTrancIds_) {
    if (readyId < tranc_id && state == TransactionState::ABORTED) {
      // aborted 没有落盘数据，可当作是 flush 的，帮助填补检查点前缀的空
      need_remove.push_back(readyId);
      flushedTrancIds_.insert(readyId);
    } else if (readyId == tranc_id) {
      need_remove.push_back(readyId);
      flushedTrancIds_.insert(readyId);
      // 后面更大的 id 之后处理
      break;
    }
  }

  for (auto id : need_remove) {
    readyToFlushTrancIds_.erase(id);
  }

  flushedTrancIds_ = compressSet<uint64_t>(flushedTrancIds_);
}

void TranManager::add_ready_to_flush_tranc_id(uint64_t tranc_id, TransactionState state) {
  std::unique_lock<std::mutex> slock(mutex_);
  readyToFlushTrancIds_[tranc_id] = state;
}

uint64_t TranManager::get_max_finished_tranc_id() {
  return max_finished_tranc_id;
}

void TranManager::update_max_finished_tranc_id(uint64_t tranc_id) {
  std::unique_lock<std::mutex> slock(mutex_);
  uint64_t id = max_finished_tranc_id.load();
  id = std::max(id, tranc_id);
  max_finished_tranc_id.store(id);
}

uint64_t TranManager::getNextTransactionId() {
  return nextTransactionId_.fetch_add(1, std::memory_order_relaxed);
}

std::shared_ptr<TranContext>
TranManager::new_tranc(const IsolationLevel &isolation_level) {
  // 事务上下文分配
  mutex_.lock();
  uint64_t next_tranc_id = getNextTransactionId();
  std::shared_ptr<TranContext> new_tranc_handler
           = std::make_shared<TranContext>(next_tranc_id, engine_, shared_from_this(), isolation_level);
  activeTrans_[next_tranc_id] = new_tranc_handler;
  mutex_.unlock();
  return new_tranc_handler;
}

std::string TranManager::get_tranc_id_file_path() {
  if (data_dir_.empty()) {
    data_dir_ = "./";
  }
  return data_dir_ + "/tranc_id";
}


std::map<uint64_t, std::vector<Record>> TranManager::check_recover() {
  return WAL::recover(get_wal_dir(), get_max_flushed_tranc_id());
}

bool TranManager::write_to_wal(const std::vector<Record> &records) {
  // 把内存中的 records 写入磁盘
  try {
    wal->log(records, true);
  } catch (const std::exception &e) {
    return false;
  }
  return true;
}

// void TranManager::flusher() {
//   while (flush_thread_running_.load()) {
//     std::this_thread::sleep_for(std::chrono::seconds(1));
//     write_tranc_id_file();
//   }
// }
} // namespace tiny_lsm
