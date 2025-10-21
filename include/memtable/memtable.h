#pragma once

#include "../iterator/iterator.h"
#include "../skiplist/skiplist.h"
#include <cstddef>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>

namespace tiny_lsm {

class BlockCache;
class SST;
class SSTBuilder;
class TranContext;

class MemTable {
  friend class TranContext;
  friend class HeapIterator;

private:
  void put_(const std::string &key, const std::string &value,
            uint64_t tranc_id);

  SkipListIterator get_(const std::string &key, uint64_t tranc_id);

  SkipListIterator cur_get_(const std::string &key, uint64_t tranc_id);

  SkipListIterator frozen_get_(const std::string &key, uint64_t tranc_id);

  void remove_(const std::string &key, uint64_t tranc_id);
  void frozen_cur_table_(); // _ 表示不需要锁的版本

public:
  MemTable();
  ~MemTable();

  void put(const std::string &key, const std::string &value, uint64_t tranc_id);
  void put_batch(const std::vector<std::pair<std::string, std::string>> &kvs,
                 uint64_t tranc_id);

  SkipListIterator get(const std::string &key, uint64_t tranc_id);
  std::vector<
      std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
  get_batch(const std::vector<std::string> &keys, uint64_t tranc_id);
  void remove(const std::string &key, uint64_t tranc_id);
  void remove_batch(const std::vector<std::string> &keys, uint64_t tranc_id);

  void clear();
  /*
    当 MemTabel 数据量达到阈值后，会调用 flush_last 函数，把最旧的一个 SkipList 持久化存储到
    SSTabel 中，形成一个 level=0 的 SST
  */ 
  std::shared_ptr<SST> flush_last(SSTBuilder &builder, std::string &sst_path,
                                  size_t sst_id,
                                  std::shared_ptr<BlockCache> block_cache);
  void frozen_cur_table(); // 冻结当前 tabel 中的活跃跳表
  size_t get_cur_size(); // 获取活跃跳表的大小，用于检查阈值并更新为冻结跳表
  size_t get_frozen_size(); // 获取冻结跳表大小，当大小大于阈值，会调用 flush last 把最旧的冻结跳表持久化到 SST
  size_t get_total_size(); // 总大小
  HeapIterator begin(uint64_t tranc_id, bool compound_iter=false);
  HeapIterator iters_preffix(const std::string &preffix, uint64_t tranc_id);

  std::optional<std::pair<HeapIterator, HeapIterator>>
  iters_monotony_predicate(uint64_t tranc_id,
                           std::function<int(const std::string &)> predicate, bool compound_iter=false);

  HeapIterator end();

private:
  // 活动跳表
  std::shared_ptr<SkipList> current_table;
  // 冻结跳表
  // 为了实现按时间顺序查找，可以把时间最新的放在 list head 位置，最旧的放在 tail 位置
  std::list<std::shared_ptr<SkipList>> frozen_tables;
  size_t frozen_bytes;
  std::shared_mutex frozen_mtx; // 冻结表的锁
  std::shared_mutex cur_mtx;    // 活跃表的锁
};
} // namespace tiny_lsm