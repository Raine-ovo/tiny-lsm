#include "../../include/memtable/memtable.h"
#include "../../include/config/config.h"
#include "../../include/consts.h"
#include "../../include/iterator/iterator.h"
#include "../../include/skiplist/skiplist.h"
#include "../../include/sst/sst.h"
#include "spdlog/spdlog.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <sys/types.h>
#include <utility>
#include <vector>

namespace tiny_lsm {

class BlockCache;

// MemTable implementation using PIMPL idiom
MemTable::MemTable() : frozen_bytes(0) {
  current_table = std::make_shared<SkipList>();
}
MemTable::~MemTable() = default;

// 不上锁版本，并发性由上层保证
void MemTable::put_(const std::string &key, const std::string &value,
                    uint64_t tranc_id) {

  // 1. 写入活跃跳表
  this->current_table->put(key, value, tranc_id);
  // 2. 检测阈值，活跃跳表数据量超出阈值则变为冻结跳表，并初始化一份新的活跃跳表
  if (this->current_table->get_size() > TomlConfig::getInstance().getLsmPerMemSizeLimit()) {
    this->frozen_cur_table_();
    // 在内存中创建一个新的 SkipList 并用智能指针管理
    this->current_table = std::make_shared<SkipList>();
  }
}

void MemTable::put(const std::string &key, const std::string &value,
                   uint64_t tranc_id) {

  std::unique_lock<std::shared_mutex> lock_put(this->cur_mtx);
  this->current_table->put(key, value, tranc_id);
  if (this->current_table->get_size() > TomlConfig::getInstance().getLsmPerMemSizeLimit()) {
    std::unique_lock<std::shared_mutex> lock_frozen(this->frozen_mtx);
    this->frozen_cur_table_();
    this->current_table = std::make_shared<SkipList>();
  }
}

// 有锁版本的批量 Put
void MemTable::put_batch(
    const std::vector<std::pair<std::string, std::string>> &kvs,
    uint64_t tranc_id) {
  std::unique_lock<std::shared_mutex> lock_put(this->cur_mtx);
  for (auto &[k, v] : kvs) {
    this->current_table->put(k, v, tranc_id);
  }
  if (this->current_table->get_size() > TomlConfig::getInstance().getLsmPerMemSizeLimit()) {
    std::unique_lock<std::shared_mutex> lock_frozen(this->frozen_mtx);
    this->frozen_cur_table_();
    this->current_table = std::make_shared<SkipList>();
  }
}

SkipListIterator MemTable::cur_get_(const std::string &key, uint64_t tranc_id) {
  // 检查当前活跃的memtable
  return this->current_table->get(key, tranc_id);
}

SkipListIterator MemTable::frozen_get_(const std::string &key,
                                       uint64_t tranc_id) {

  // 因为冻结跳表的链接顺序是新->旧，因此需要从头开始遍历
  for (auto skiplist : this->frozen_tables) {
    SkipListIterator result = skiplist->get(key, tranc_id);
    if (result.is_valid()) return result;
  }
  return SkipListIterator{};
}

SkipListIterator MemTable::get(const std::string &key, uint64_t tranc_id) {
  // 注意并发控制
  std::shared_lock<std::shared_mutex> lock(this->cur_mtx);
  // 优先查询活跃跳表
  SkipListIterator result = this->cur_get_(key, tranc_id);
  if (result.is_valid()) return result;
  // 查询冻结跳表
  std::shared_lock<std::shared_mutex> lock2(this->frozen_mtx);
  result = this->frozen_get_(key, tranc_id);
  return result;
}

SkipListIterator MemTable::get_(const std::string &key, uint64_t tranc_id) {
  // 优先查询活跃跳表
  SkipListIterator result = this->cur_get_(key, tranc_id);
  if (result.is_valid()) return result;
  // 查询冻结跳表
  result = this->frozen_get_(key, tranc_id);
  return result;
}

std::vector<
    std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
MemTable::get_batch(const std::vector<std::string> &keys, uint64_t tranc_id) {
  spdlog::trace("MemTable--get_batch with {} keys", keys.size());

  // 存储返回结果
  std::vector<
    std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>
  > results;
  // 使用 reserve 让内存预留大小，但不直接创建元素，减小开销
  results.reserve(keys.size());

  // 1. 首先在活跃跳表中查找, 注意加锁
  std::shared_lock<std::shared_mutex> lock_cur_get(this->cur_mtx);
  for (const std::string& key: keys) {
    SkipListIterator cur_result = this->cur_get_(key, tranc_id);
    if (cur_result.is_valid()) results.emplace_back(
                          std::make_pair(key,
                                                std::make_pair(
                                                      cur_result.get_value(), tranc_id)));
    else results.emplace_back(std::make_pair(key, std::nullopt));
  }

  // 判断是不是都在活跃跳表，如果都是就返回
  if (std::all_of(results.begin(), results.end(), [](const auto& kv) -> bool {
    return kv.second != std::nullopt;
  })) {
    return results;
  }

  // 因为不用活跃跳表的锁了，释放了优化并发
  lock_cur_get.unlock();
  // 存在部分应该去冻结跳表中查询
  std::shared_lock<std::shared_mutex> lock_frozen(this->frozen_mtx);
  for (int i = 0; i < keys.size(); i ++ ) {
    const std::string& key = keys[i];
    SkipListIterator cur_result = this->frozen_get_(key, tranc_id);
    if (cur_result.is_valid()) {
      results[i] = std::make_pair(key, std::make_pair(cur_result.get_value(), tranc_id));
    }
  }

  return results;
}

// 注意 LSM 实现 remove 是用追加写的方式，写入 key 的 value = 空
void MemTable::remove_(const std::string &key, uint64_t tranc_id) {
  this->put_(key, "", tranc_id);
}

void MemTable::remove(const std::string &key, uint64_t tranc_id) {
  this->put(key, "", tranc_id);
}

void MemTable::remove_batch(const std::vector<std::string> &keys,
                            uint64_t tranc_id) {
  std::vector<std::pair<std::string, std::string>> kvs;
  kvs.reserve(keys.size());
  for (const std::string& key: keys) {
    kvs.emplace_back(std::make_pair(key, ""));
  }
  this->put_batch(kvs, tranc_id);
}

void MemTable::clear() {
  spdlog::info("MemTable--clear(): Clearing all tables");

  std::unique_lock<std::shared_mutex> lock1(cur_mtx);
  std::unique_lock<std::shared_mutex> lock2(frozen_mtx);
  frozen_tables.clear();
  current_table->clear();
}

// 将最老的 memtable 写入 SST, 并返回控制类
std::shared_ptr<SST>
MemTable::flush_last(SSTBuilder &builder, std::string &sst_path, size_t sst_id,
                     std::shared_ptr<BlockCache> block_cache) {
  spdlog::debug("MemTable--flush_last(): Starting to flush memtable to SST{}",
                sst_id);

  // 由于 flush 后需要移除最老的 memtable, 因此需要加写锁
  std::unique_lock<std::shared_mutex> lock(frozen_mtx);

  uint64_t max_tranc_id = 0;
  uint64_t min_tranc_id = UINT64_MAX;

  if (frozen_tables.empty()) {
    // 如果当前表为空，直接返回nullptr
    if (current_table->get_size() == 0) {
      spdlog::debug(
          "MemTable--flush_last(): Current table is empty, returning null");

      return nullptr;
    }
    // 将当前表加入到frozen_tables头部
    frozen_tables.push_front(current_table);
    frozen_bytes += current_table->get_size();
    // 创建新的空表作为当前表
    current_table = std::make_shared<SkipList>();
  }

  // 将最老的 memtable 写入 SST
  std::shared_ptr<SkipList> table = frozen_tables.back();
  frozen_tables.pop_back();
  frozen_bytes -= table->get_size();

  std::vector<std::tuple<std::string, std::string, uint64_t>> flush_data =
      table->flush();
  for (auto &[k, v, t] : flush_data) {
    max_tranc_id = std::max(t, max_tranc_id);
    min_tranc_id = std::min(t, min_tranc_id);
    builder.add(k, v, t);
  }
  auto sst = builder.build(sst_id, sst_path, block_cache);

  spdlog::info("MemTable--flush_last(): SST{} built successfully at '{}'",
               sst_id, sst_path);

  return sst;
}

void MemTable::frozen_cur_table_() {
  this->frozen_bytes += this->current_table->get_size();
  this->frozen_tables.push_front(this->current_table);

}

void MemTable::frozen_cur_table() {
  std::unique_lock<std::shared_mutex> lock(this->frozen_mtx);
  this->frozen_cur_table_();
}

size_t MemTable::get_cur_size() {
  std::shared_lock<std::shared_mutex> slock(cur_mtx);
  return current_table->get_size();
}

size_t MemTable::get_frozen_size() {
  std::shared_lock<std::shared_mutex> slock(frozen_mtx);
  return frozen_bytes;
}

size_t MemTable::get_total_size() {
  std::shared_lock<std::shared_mutex> slock1(cur_mtx);
  std::shared_lock<std::shared_mutex> slock2(frozen_mtx);
  return frozen_bytes + current_table->get_size();
}

/*
  * MemTabel 的迭代器向外部暴露的元素依然是我们所存储的 kv 元素
  * 那么这里就有一个问题，MemTabel 中存储多个 SkipList ，因此 key 是可能重复的
  * 我们要想一个方法来有序返回 kv 迭代器

  * 对于两个 skiplist ，可以使用双指针来有序返回
  * 对于多个 skiplist ，我们可以使用堆来保存所有的 kv 元素，以 key 为排序元素

  * 这里还有一个问题，对于重复的 key 对应的 kv ，它表示了多次对 key 的操作，我们只能选择其一来返回
  * 由于 skiplist 本身就是按照新->旧排序，因此我们只需要返回最新的 skiplist 的对应 key 的 kv 即可
*/
HeapIterator MemTable::begin(uint64_t tranc_id, bool compound_iter) {
  // 需要读内容
  std::shared_lock<std::shared_mutex> slock(this->cur_mtx);
  std::shared_lock<std::shared_mutex> slock2(this->frozen_mtx);

  std::vector<SearchItem> kvs;
  int id = 0; // id 是 skiplist 的 id
  int max_trac_id = 0;
  // 把所有跳表的信息放进去

  // 活跃跳表
  ++ id;
  for (SkipListIterator it = this->current_table->begin(); it != this->current_table->end(); ++it ) {
    SearchItem item = SearchItem{it.get_key(), it.get_value(), id, 0, it.get_tranc_id()};
    // max_trac_id = std::max(static_cast<uint64_t>(max_trac_id), it.get_tranc_id());
    kvs.emplace_back(item);
  }

  // 冻结跳表
  for (auto frozen_tabel = this->frozen_tables.begin(); frozen_tabel != this->frozen_tables.end(); ++ frozen_tabel) {
    ++ id;
    for (auto it = (*frozen_tabel)->begin(); it != (*frozen_tabel)->end(); ++ it) {
      SearchItem item = SearchItem{it.get_key(), it.get_value(), id, 0, it.get_tranc_id()};
      max_trac_id = std::max(static_cast<uint64_t>(max_trac_id), it.get_tranc_id());
      kvs.emplace_back(item);
    }
  }

  return HeapIterator{kvs, tranc_id, compound_iter};
}

HeapIterator MemTable::end() {

  std::shared_lock<std::shared_mutex> slock(this->cur_mtx);
  std::shared_lock<std::shared_mutex> slock2(this->frozen_mtx);
  return HeapIterator{};
}

HeapIterator MemTable::iters_preffix(const std::string &preffix,
                                     uint64_t tranc_id) {

  std::vector<SearchItem> result_vec;

  std::shared_lock<std::shared_mutex> slock(this->cur_mtx);
  int tabel_id = 0;
  // 查询活跃跳表
  SkipListIterator begin_iter = this->current_table->begin_preffix(preffix);
  SkipListIterator end_iter = this->current_table->end_preffix(preffix);
  for (SkipListIterator iter = begin_iter; iter != end_iter; ++ iter) {
    result_vec.emplace_back(SearchItem{iter.get_key(), iter.get_value(), tabel_id, 0, tranc_id});
  }

  // 查询冻结列表
  slock.unlock();
  std::shared_lock<std::shared_mutex> slock2(this->frozen_mtx);
  for (auto tb = this->frozen_tables.begin(); tb != this->frozen_tables.end(); ++ tb) {
    tabel_id ++ ;
    begin_iter = (*tb)->begin_preffix(preffix);
    end_iter = (*tb)->end_preffix(preffix);
    for (SkipListIterator iter = begin_iter; iter != end_iter; ++ iter) {
      result_vec.emplace_back(SearchItem{iter.get_key(), iter.get_value(), tabel_id, 0, tranc_id});
    }
  }

  return HeapIterator{result_vec, tranc_id};
}

std::optional<std::pair<HeapIterator, HeapIterator>>
MemTable::iters_monotony_predicate(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate, bool compound_iter) {

  std::vector<SearchItem> result_vec;

  std::shared_lock<std::shared_mutex> slock(this->cur_mtx);
  int tabel_id = 0;
  // 查询活跃跳表
  std::optional<std::pair<SkipListIterator, SkipListIterator>> opt = this->current_table->iters_monotony_predicate(predicate);

  if (opt.has_value()) {
    auto begin_iter = opt.value().first;
    auto end_iter = opt.value().second;

    for (SkipListIterator iter = begin_iter; iter != end_iter; ++ iter) {
      result_vec.emplace_back(SearchItem{iter.get_key(), iter.get_value(), tabel_id, 0, iter.get_tranc_id()});
    }
  }

  // 查询冻结列表
  slock.unlock();
  std::shared_lock<std::shared_mutex> slock2(this->frozen_mtx);
  for (auto tb = this->frozen_tables.begin(); tb != this->frozen_tables.end(); ++ tb) {
    tabel_id ++ ;
    opt = (*tb)->iters_monotony_predicate(predicate);
    if (!opt.has_value()) continue;
    auto begin_iter = opt.value().first;
    auto end_iter = opt.value().second;
    for (SkipListIterator iter = begin_iter; iter != end_iter; ++ iter) {
      result_vec.emplace_back(SearchItem{iter.get_key(), iter.get_value(), tabel_id, 0, iter.get_tranc_id()});
    }
  }

  if (result_vec.empty()) {
    return std::nullopt;
  }
  return std::make_pair<HeapIterator, HeapIterator>(HeapIterator{result_vec, tranc_id, compound_iter}, HeapIterator{});
}
} // namespace tiny_lsm
