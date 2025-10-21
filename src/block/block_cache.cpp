#include "../../include/block/block_cache.h"
#include "../../include/block/block.h"
#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace tiny_lsm {
BlockCache::BlockCache(size_t capacity, size_t history_capacity, size_t k)
    : capacity_(capacity), history_capacity_(history_capacity), k_(k) {}

BlockCache::~BlockCache() = default;

std::shared_ptr<Block> BlockCache::get(int sst_id, int block_id) {
  // 查询一个 Block
  mutex_.lock();
  ++ total_requests_;
  if (!cache_map_.contains({sst_id, block_id})) {
    mutex_.unlock();
    return nullptr;
  }
  auto iter = cache_map_[{sst_id, block_id}];
  ++ hit_requests_;
  update_access_count(iter);
  mutex_.unlock();
  return iter->cache_block;
}

void BlockCache::put(int sst_id, int block_id, std::shared_ptr<Block> block) {
  // 插入一个 Block

  // 第一次插入的时候 访问次数为 1
  // 缓存池是临界资源，记得加上互斥锁
  mutex_.lock();
  if (cache_map_.contains({sst_id, block_id})) {
    mutex_.unlock();
    return;
  }
  cache_list_less_k.push_front(CacheItem{sst_id, block_id, block, 1});
  cache_map_[{sst_id, block_id}] = cache_list_less_k.begin();
  // 历史访问池超出容量，则淘汰末尾
  if (cache_list_less_k.size() > history_capacity_) {
    auto last = cache_list_less_k.back();
    cache_map_.erase({last.sst_id, last.block_id});
    cache_list_less_k.pop_back();
  }
  mutex_.unlock();
}

double BlockCache::hit_rate() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return total_requests_ == 0
             ? 0.0
             : static_cast<double>(hit_requests_) / total_requests_;
}

void BlockCache::update_access_count(std::list<CacheItem>::iterator it) {
  // 更新统计信息
  it->access_count += 1;
  if (it->access_count == k_) {
    cache_list_greater_k.splice(cache_list_greater_k.begin(), cache_list_less_k, it);
    // 如果缓存池超出阈值，需要淘汰末尾最旧没有访问的
    if (cache_list_greater_k.size() > capacity_) {
      const auto &last = cache_list_greater_k.back();
      cache_map_.erase({last.sst_id, last.block_id});
      cache_list_greater_k.pop_back();
    }
  } else if (it->access_count > k_) {
    cache_list_greater_k.splice(cache_list_greater_k.begin(), cache_list_greater_k, it);
  } else {
    cache_list_less_k.splice(cache_list_less_k.begin(), cache_list_less_k, it);
  }
}
} // namespace tiny_lsm
