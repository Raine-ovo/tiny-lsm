#include "../../include/lsm/engine.h"
#include "../../include/config/config.h"
#include "../../include/consts.h"
#include "../../include/logger/logger.h"
#include "../../include/lsm/level_iterator.h"
#include "../../include/sst/concact_iterator.h"
#include "../../include/sst/sst.h"
#include "../../include/sst/sst_iterator.h"
#include "spdlog/spdlog.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <utility>
#include <vector>
#include "../../include/utils/debug.h"

namespace tiny_lsm {

// *********************** LSMEngine ***********************
LSMEngine::LSMEngine(std::string path, bool has_blockCache) : data_dir(path) {
  // 初始化日志
  init_spdlog_file();

  // 引擎初始化

  // debug("begin initialize LSM Engine");
  // 不存在改目录，需要创建
  if (!std::filesystem::exists(path)) {
    std::filesystem::create_directory(path);
  }

  // 初始化缓存池
  if (has_blockCache) {
    block_cache = std::make_shared<BlockCache>(TomlConfig::getInstance().getLsmBlockCacheCapacity(),
          TomlConfig::getInstance().getLsmBlockCacheHistoryCapacity(), TomlConfig::getInstance().getLsmBlockCacheK());
  }

  // 读取该目录下所有 sst 文件
  for (const auto &entry: std::filesystem::directory_iterator(path)) {
    // debug("into sst file");
    // 不是普通文件
    if (!entry.is_regular_file()) continue;
    std::string filename = entry.path().filename().string();
    // SST文件命名格式： sst_{id}.{level}
    if (!filename.starts_with("sst_")) continue;
    // 提取出 id 和 level
    size_t dot_pos = filename.find('.');
    if (dot_pos == std::string::npos || dot_pos == filename.length() - 1) continue;
    std::string str_id = filename.substr(4, dot_pos - 4);
    std::string str_level = filename.substr(dot_pos + 1);
    if (str_id.empty() || str_level.empty()) continue;
    size_t id = std::stoull(str_id), level = std::stoull(str_level);
    // debug("id and level: ", id, level);

    // 更新元数据，注意加锁
    std::unique_lock<std::shared_mutex> slock(ssts_mtx);
    cur_max_level = std::max(cur_max_level, level);
    next_sst_id = std::max(next_sst_id, id + 1);
    level_sst_ids[level].push_back(id);
    std::shared_ptr<SST> sst = SST::open(id, FileObj::open(entry.path().string(), false), block_cache);
    ssts[id] = sst;
  }

  // 更新 level__sst_ids 的 sst 的顺序
  for (auto &[level, sst_list]: level_sst_ids) {
    std::sort(sst_list.begin(), sst_list.end());
    if (level == 0) {
      // level = 0 的 ssts 属于 unsorted ，以 id 大为新
      std::reverse(sst_list.begin(), sst_list.end());
    }
  }

  // debug("end initialize LSM Engine");
}

LSMEngine::~LSMEngine() = default;

std::optional<std::pair<std::string, uint64_t>>
LSMEngine::get(const std::string &key, uint64_t tranc_id) {

  // MemTable
  auto iter = memtable.get(key, tranc_id);
  if (iter.is_valid()) {
    // debug("find in metatable: ", iter.get_value());
    if (iter.get_value().size() > 0) return std::make_pair(iter.get_value(), iter.get_tranc_id());
    return std::nullopt;
  }

  // 缓存池是针对 Block 数据的，因此在读取 block 之前需要先判断是否在缓存池中

  // SST
  std::shared_lock<std::shared_mutex> slock(ssts_mtx);
  // Level = 0（可能重叠）
  for (const auto &sst_id: level_sst_ids[0]) {
    const auto &sst_ptr = ssts[sst_id];
    if (!(key >= sst_ptr->get_first_key() && key <= sst_ptr->get_last_key())) {
      continue;
    }
    auto iter = sst_ptr->get(key, tranc_id);
    if (iter.is_valid()) {
      if ((*iter).second.size() > 0) return std::make_pair((*iter).second, iter.get_tranc_id());
      return std::nullopt;
    }
  }

  // level >= 1 (SST 不重叠且严格升序，使用二分查找)
  for (const auto &[level, dq]: level_sst_ids) {
    if (level == 0 || dq.empty()) continue;
    size_t l = 0, r = dq.size();
    while (l < r) {
      size_t mid = l + (r - l) / 2;
      const auto &sst_ptr = ssts[dq[mid]];
      if (key < sst_ptr->get_first_key()) { r = mid; continue; }
      else if (key > sst_ptr->get_last_key()) { l = mid + 1; continue; }
      auto res = sst_ptr->get(key, tranc_id);
      if (res.is_valid()) {
        if (res.value().size() == 0) return std::nullopt;
        return std::make_pair(res.value(), res.get_tranc_id());
      }
      break;
    }
  }

  // debug("can't find the key: ", key);
  return std::nullopt;
}

std::vector<
    std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
LSMEngine::get_batch(const std::vector<std::string> &keys, uint64_t tranc_id) {
  std::vector<std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>> results(keys.size());
  for (int i = 0; i < results.size(); i ++ ) {
    auto &entry = results[i];
    entry.first = keys[i];
    entry.second = get(keys[i], tranc_id);
  }
  return results;
}

std::optional<std::pair<std::string, uint64_t>>
LSMEngine::sst_get_(const std::string &key, uint64_t tranc_id) {
  // sst 内部查询

    // MemTable
  auto iter = memtable.get(key, tranc_id);
  if (!iter.is_end()) return std::make_pair(iter.get_value(), iter.get_tranc_id());

  // SST
  std::shared_lock<std::shared_mutex> slock(ssts_mtx);
  // Level = 0
  for (size_t sst_id: level_sst_ids[0]) {
    const auto &sst_ptr = ssts[sst_id];
    if (!(key >= sst_ptr->get_first_key() && key <= sst_ptr->get_last_key())) {
      continue;
    }
    auto iter = sst_ptr->get(key, tranc_id);
    if (iter.is_valid()) return std::make_pair((*iter).second, iter.get_tranc_id());
  }

  // level >= 1 (SST 不重叠且严格升序，使用二分查找)
  for (const auto &[level, dq]: level_sst_ids) {
    if (level == 0 || dq.empty()) continue;
    size_t l = 0, r = dq.size();
    while (l < r) {
      size_t mid = l + (r - l) / 2;
      const auto &sst_ptr = ssts[dq[mid]];
      if (key < sst_ptr->get_first_key()) { r = mid; continue; }
      else if (key > sst_ptr->get_last_key()) { l = mid + 1; continue; }
      auto res = sst_ptr->get(key, tranc_id);
      if (res.is_valid()) {
        return std::make_pair(res.value(), res.get_tranc_id());
      }
      break;
    }
  }

  // debug("can't find the key: ", key);
  return std::nullopt;
}

uint64_t LSMEngine::put(const std::string &key, const std::string &value,
                        uint64_t tranc_id) {
  // 由于 put 操作可能触发 flush
  // 如果触发了 flush 则返回新刷盘的 sst 的 id
  // 在没有实现  flush 的情况下，返回 0


  // debug("Put start");
  // 首先 put 进 memtable
  memtable.put(key, value, tranc_id);
  // put 后 memtable 可能超出阈值，需要把最久的冻结 skiplist 刷成 SST 文件
  if (memtable.get_total_size() > TomlConfig::getInstance().getLsmTolMemSizeLimit()) {
    int new_sst_id = flush();
    full_compact(0);
    return new_sst_id;
  }
  // debug("Put end");
  return 0;
}

uint64_t LSMEngine::put_batch(
    const std::vector<std::pair<std::string, std::string>> &kvs,
    uint64_t tranc_id) {
  // 批量插入
  // 由于 put 操作可能触发 flush
  // 如果触发了 flush 则返回新刷盘的 sst 的 id
  // 在没有实现  flush 的情况下，返回 0

  memtable.put_batch(kvs, tranc_id);
    // put 后 memtable 可能超出阈值，需要把最久的冻结 skiplist 刷成 SST 文件
  if (memtable.get_total_size() > TomlConfig::getInstance().getLsmTolMemSizeLimit()) {
    int new_sst_id = flush();
    full_compact(0);
    return new_sst_id;
  }
  return 0;
}

uint64_t LSMEngine::remove(const std::string &key, uint64_t tranc_id) {
  // 在 LSM 中，删除实际上是插入一个空值
  // 由于 put 操作可能触发 flush
  // 如果触发了 flush 则返回新刷盘的 sst 的 id
  // 在没有实现  flush 的情况下，返回 0

  return put(key, "", tranc_id);
}

uint64_t LSMEngine::remove_batch(const std::vector<std::string> &keys,
                                 uint64_t tranc_id) {
  // 批量删除
  // 在 LSM 中，删除实际上是插入一个空值
  // 由于 put 操作可能触发 flush
  // 如果触发了 flush 则返回新刷盘的 sst 的 id
  // 在没有实现  flush 的情况下，返回 0
  std::vector<std::pair<std::string, std::string>> kvs(keys.size());
  for (int i = 0; i < kvs.size(); i ++ )
    kvs[i] = std::make_pair(keys[i], "");

  return put_batch(kvs, tranc_id);
}

void LSMEngine::clear() {
  memtable.clear();
  level_sst_ids.clear();
  ssts.clear();
  // 清空当前文件夹的所有内容
  try {
    for (const auto &entry : std::filesystem::directory_iterator(data_dir)) {
      if (!entry.is_regular_file()) {
        continue;
      }
      std::filesystem::remove(entry.path());

      spdlog::info("LSMEngine--"
                   "clear file {} successfully.",
                   entry.path().string());
    }
  } catch (const std::filesystem::filesystem_error &e) {
    // 处理文件系统错误
    spdlog::error("Error clearing directory: {}", e.what());
  }
}

uint64_t LSMEngine::flush() {
  // 刷盘形成sst文件
  // 把 MemTable 中的最旧的跳表刷成 SST（可以是活跃跳表）
  if (memtable.get_total_size() == 0) return 0;

  // 因为在 build 过程需要进行持久化写入到磁盘的行为，因此需要对 sst 加写锁
  std::unique_lock<std::shared_mutex> slock(ssts_mtx);

  SSTBuilder sstbuilder = SSTBuilder{static_cast<size_t>(TomlConfig::getInstance().getLsmBlockSize()), true};
  size_t sst_id = next_sst_id;
  ++ next_sst_id;
  int target_level = 0;
  std::string file_path = get_sst_path(sst_id, target_level);

  std::shared_ptr<SST> new_sst = memtable.flush_last(sstbuilder, file_path, sst_id, block_cache);

  // 把这个 sst 控制结构加入到 engine 数据结构中
  level_sst_ids[0].push_front(sst_id);
  ssts[sst_id] = new_sst;

  return sst_id;
}

std::string LSMEngine::get_sst_path(size_t sst_id, size_t target_level) {
  // sst的文件路径格式为: data_dir/sst_<sst_id>，sst_id格式化为32位数字
  std::stringstream ss;
  ss << data_dir << "/sst_" << std::setfill('0') << std::setw(32) << sst_id
     << '.' << target_level;
  return ss.str();
}

std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
LSMEngine::lsm_iters_monotony_predicate(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  // 谓词查询
  // 构造 memtable 和 sst 迭代器
  auto mem_iter = memtable.iters_monotony_predicate(tranc_id, predicate, true);

  std::vector<SearchItem> items;
  for (const auto& [level, ids]: level_sst_ids) {
    for (const auto& sst_id: ids) {
      auto sst = ssts[sst_id];
      auto res = sst_iters_monotony_predicate(sst, tranc_id, predicate);
      if (!res.has_value()) continue;
      auto &[it_begin, it_end] = res.value();
      for (auto it = it_begin; it != it_end && it.is_valid(); ++ it) {
        // 这里注意，我们实现 HeapIterator 用的 SearchItem 时，idx 指的是 SkipList 的 id ，越小越新
        // 这里我们进行复用，用的是 sst id，这个 id 是越大越新的，因此需要取反
        items.emplace_back(it.key(), it.value(), -sst_id,
                      level, it.get_tranc_id());
      }
    }
  }

  HeapIterator sst_iter{items, tranc_id, true};

  if (!mem_iter.has_value() && !sst_iter.is_valid()) return std::nullopt;

  if (!mem_iter.has_value()) {
    return std::make_pair(
      TwoMergeIterator{std::make_shared<HeapIterator>(), std::make_shared<HeapIterator>(sst_iter), tranc_id},
      TwoMergeIterator{std::make_shared<HeapIterator>(), std::make_shared<HeapIterator>(), tranc_id});
  } else {
    auto mem_iter_begin = mem_iter.value().first;
    auto mem_iter_end = mem_iter.value().second;
    return std::make_pair(
      TwoMergeIterator{std::make_shared<HeapIterator>(mem_iter_begin), std::make_shared<HeapIterator>(sst_iter), tranc_id},
      TwoMergeIterator{std::make_shared<HeapIterator>(mem_iter_end), std::make_shared<HeapIterator>(), tranc_id}
    );
  }
}

Level_Iterator LSMEngine::begin(uint64_t tranc_id) {
  return Level_Iterator{shared_from_this(), tranc_id};
}

Level_Iterator LSMEngine::end() {
  return Level_Iterator{};
}

void LSMEngine::full_compact(size_t src_level) {
  // 负责完成整个 full compact
  // ? 需要控制`Compact`流程需要递归地进行

  // 如果 src_level 并没有超出阈值，直接返回
  if (level_sst_ids[src_level].size() < TomlConfig::getInstance().getLsmSstLevelRatio())
    return;

  // src_level 数据量大，从这个 level 开始递归进行压缩
  size_t final_level = src_level;
  while (final_level <= cur_max_level &&
         level_sst_ids[final_level].size() >=
             TomlConfig::getInstance().getLsmSstLevelRatio()) {
    ++final_level;
  }

  // final_level 是第一个不需要压缩的层（或 cur_max_level+1）。需要将数据逐层向上合并。
  cur_max_level = std::max(cur_max_level, final_level);

  for (size_t lvl = final_level; lvl > src_level; ) {
    --lvl; // 从 final_level-1 开始往下到 src_level
    size_t x = lvl, y = lvl + 1;
    std::vector<size_t> ids_x(level_sst_ids[x].begin(), level_sst_ids[x].end()),
        ids_y(level_sst_ids[y].begin(), level_sst_ids[y].end());
    auto new_ssts = x ? full_common_compact(ids_x, ids_y, y)
                      : full_l0_l1_compact(ids_x, ids_y);
    // 更新元数据：删除旧 SST，增加新的 SST，更新 level ids 排序
    // 删除 x, y level 的所有 sst
    for (auto id: ids_x) ssts[id]->del_sst(), ssts.erase(id);
    for (auto id: ids_y) ssts[id]->del_sst(), ssts.erase(id);
    level_sst_ids[x].clear();
    level_sst_ids[y].clear();
    // 把新的 sst 给 y level
    for (auto &sst: new_ssts) {
      size_t sst_id = sst->get_sst_id();
      level_sst_ids[y].push_back(sst_id);
      ssts[sst_id] = sst;
    }
    // 对 y level 按 id 排序，方便查找
    std::sort(level_sst_ids[y].begin(), level_sst_ids[y].end());
  }
}

std::vector<std::shared_ptr<SST>>
LSMEngine::full_l0_l1_compact(std::vector<size_t> &l0_ids,
                              std::vector<size_t> &l1_ids) {
  // 负责完成 l0 和 l1 的 full compact

  // level 0
  std::vector<SstIterator> vec_iters(l0_ids.size());
  for (int i = 0; i < l0_ids.size(); i ++ ) {
    vec_iters[i] = ssts[l0_ids[i]]->begin(0);
  }
  auto [it_1, it__] = SstIterator::merge_sst_iterator(vec_iters, 0, true);

  // level 1
  std::vector<std::shared_ptr<SST>> _ssts(l1_ids.size());
  for (int i = 0; i < l1_ids.size(); i ++ ) {
    _ssts[i] = ssts[l1_ids[i]];
  }

  auto it_2 = ConcactIterator{_ssts, 0};

  TwoMergeIterator iter{std::make_shared<HeapIterator>(it_1), std::make_shared<ConcactIterator>(it_2), 0};

  // 前面完成了这两层 iter 的构建
  // 现在是构造 sst

  return  gen_sst_from_iter(iter, get_sst_size(1), 1);
}

std::vector<std::shared_ptr<SST>>
LSMEngine::full_common_compact(std::vector<size_t> &lx_ids,
                               std::vector<size_t> &ly_ids, size_t level_y) {
  // 负责完成其他相邻 level 的 full compact

  std::vector<std::shared_ptr<SST>> ssts_x(lx_ids.size()), ssts_y(ly_ids.size());
  for (int i = 0; i < lx_ids.size(); i ++ ) ssts_x[i] = ssts[lx_ids[i]];
  for (int i = 0; i < ly_ids.size(); i ++ ) ssts_y[i] = ssts[ly_ids[i]];

  std::shared_ptr<ConcactIterator> iter_x = std::make_shared<ConcactIterator>(ssts_x, 0);
  std::shared_ptr<ConcactIterator> iter_y = std::make_shared<ConcactIterator>(ssts_y, 0);

  TwoMergeIterator iter{iter_x, iter_y, 0};

  return gen_sst_from_iter(iter, LSMEngine::get_sst_size(level_y), level_y);
}

std::vector<std::shared_ptr<SST>>
LSMEngine::gen_sst_from_iter(BaseIterator &iter, size_t target_sst_size,
                             size_t target_level) {
  // 实现从迭代器构造新的 SST
  std::vector<std::shared_ptr<SST>> new_ssts;
  SSTBuilder builder = SSTBuilder{static_cast<size_t>(TomlConfig::getInstance().getLsmBlockSize()), true, target_level};
  while (!iter.is_end()) {
    auto kv = *iter;
    builder.add(kv.first, kv.second, iter.get_tranc_id());
    ++iter;
    if (builder.estimated_size() >= target_sst_size) {
      // 把 builder build 成一个 sst
      size_t new_sst_id = next_sst_id ++;
      auto new_sst = builder.build(new_sst_id, get_sst_path(new_sst_id, target_level), block_cache);
      new_ssts.push_back(new_sst);
      builder = SSTBuilder{static_cast<size_t>(TomlConfig::getInstance().getLsmBlockSize()), true};
    }
  }
  // 构建最后一个未满的 SST（如果有数据）
  try {
    size_t new_sst_id = next_sst_id;
    // 如果没有任何数据，build 会抛出异常，忽略即可
    auto tail_sst = builder.build(new_sst_id, get_sst_path(new_sst_id, target_level), block_cache);
    ++next_sst_id;
    new_ssts.push_back(tail_sst);
  } catch (...) {
    // 无数据则不生成尾部 SST
  }
  return new_ssts;
}

size_t LSMEngine::get_sst_size(size_t level) {
  if (level == 0) {
    return TomlConfig::getInstance().getLsmPerMemSizeLimit();
  } else {
    return TomlConfig::getInstance().getLsmPerMemSizeLimit() *
           static_cast<size_t>(std::pow(
               TomlConfig::getInstance().getLsmSstLevelRatio(), level));
  }
}

// *********************** LSM ***********************
LSM::LSM(std::string path)
    : engine(std::make_shared<LSMEngine>(path)),
      tran_manager_(std::make_shared<TranManager>(path)) {
  // 控制WAL重放与组件的初始化

  tran_manager_->set_engine(engine);

  // 检查是否需要重放 WAL 日志
  spdlog::info("LSM::LSM: start WAL recovery for dir '{}'/wal", path);
  std::map<uint64_t, std::vector<Record>> rec = tran_manager_->check_recover();
  spdlog::info("LSM::LSM: recovered {} transactions from WAL", rec.size());
  // 把重放的日志应用给 LSMEngine
  // debug("rec size:", rec.size());
  for (auto &[tranc_id, records]: rec) {
    spdlog::info("LSM::LSM: apply tranc_id {} with {} records, last_op={} (0=create 1=commit 2=rollback 3=put 4=delete)",
                 tranc_id, records.size(), static_cast<int>(records.back().getOperationType()));
    // 把这些 records 进行重放
    if (tran_manager_->get_flushed_tranc_ids().contains(tranc_id)) {
      // 如果已经持久化了就不管
      continue;
    }
    // 判断最后一条记录是不是 commit
    if (records.back().getOperationType() != OperationType::COMMIT) {
      // 没有提交成功，略过
      continue;
    }
    for (const Record &record: records) {
      debug("tranc_id:", tranc_id);
      if (record.getOperationType() == OperationType::PUT) {
        debug("in put:");
        record.print();
        engine->put(record.getKey(), record.getValue(), record.getTrancId());
        if (engine->get(record.getKey(), 0) == std::nullopt) {
          debug("put but not found!!!!");
        } else {
          debug("put and then can found!!!");
        }
      } else if (record.getOperationType() == OperationType::DELETE) {
        debug("in remove:");
        record.print();
        engine->remove(record.getKey(), record.getTrancId());
      }
    }
  }

  // 初始化 WAL 组件
  tran_manager_->init_new_wal();
}

// LSM 析构时会把所有在内存中的数据进行持久化
LSM::~LSM() {
  flush_all();
  tran_manager_->write_tranc_id_file();
}

// trade_off 默认为 false，即默认开启事务
std::optional<std::string> LSM::get(const std::string &key, bool tranc_off) {
  auto tranc_id = tranc_off ? 0 : tran_manager_->getNextTransactionId();
  auto res = engine->get(key, tranc_id);

  if (res.has_value()) {
    return res.value().first;
  }
  return std::nullopt;
}

std::vector<std::pair<std::string, std::optional<std::string>>>
LSM::get_batch(const std::vector<std::string> &keys) {
  // 1. 获取事务ID
  auto tranc_id = tran_manager_->getNextTransactionId();

  // 2. 调用 engine 的批量查询接口
  auto batch_results = engine->get_batch(keys, tranc_id);

  // 3. 构造最终结果
  std::vector<std::pair<std::string, std::optional<std::string>>> results;
  for (const auto &[key, value] : batch_results) {
    if (value.has_value()) {
      results.emplace_back(key, value->first); // 提取值部分
    } else {
      results.emplace_back(key, std::nullopt); // 键不存在
    }
  }

  return results;
}

void LSM::put(const std::string &key, const std::string &value,
              bool tranc_off) {
  auto tranc_id = tranc_off ? 0 : tran_manager_->getNextTransactionId();
  engine->put(key, value, tranc_id);
}

void LSM::put_batch(
    const std::vector<std::pair<std::string, std::string>> &kvs) {
  auto tranc_id = tran_manager_->getNextTransactionId();
  engine->put_batch(kvs, tranc_id);
}
void LSM::remove(const std::string &key) {
  auto tranc_id = tran_manager_->getNextTransactionId();
  engine->remove(key, tranc_id);
}

void LSM::remove_batch(const std::vector<std::string> &keys) {
  auto tranc_id = tran_manager_->getNextTransactionId();
  engine->remove_batch(keys, tranc_id);
}

void LSM::clear() { engine->clear(); }

void LSM::flush() { auto max_tranc_id = engine->flush(); }

void LSM::flush_all() {
  while (engine->memtable.get_total_size() > 0) {
    auto max_tranc_id = engine->flush();
    tran_manager_->add_ready_to_flush_tranc_id(max_tranc_id, TransactionState::COMMITTED);
  }
}

LSM::LSMIterator LSM::begin(uint64_t tranc_id) {
  return engine->begin(tranc_id);
}

LSM::LSMIterator LSM::end() { return engine->end(); }

std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
LSM::lsm_iters_monotony_predicate(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  return engine->lsm_iters_monotony_predicate(tranc_id, predicate);
}

// 开启一个事务
std::shared_ptr<TranContext>
LSM::begin_tran(const IsolationLevel &isolation_level) {
  return tran_manager_->new_tranc(isolation_level);
}

void LSM::set_log_level(const std::string &level) { reset_log_level(level); }
} // namespace tiny_lsm
