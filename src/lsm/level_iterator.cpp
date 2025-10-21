#include "../../include/lsm/level_iterator.h"
#include "../../include/lsm/engine.h"
#include "../../include/sst/concact_iterator.h"
#include "../../include/sst/sst.h"
#include <memory>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

namespace tiny_lsm {
Level_Iterator::Level_Iterator(std::shared_ptr<LSMEngine> engine,
                               uint64_t max_tranc_id)
    : engine_(engine), max_tranc_id_(max_tranc_id), rlock_(engine_->ssts_mtx) {

  // level 迭代器包含 MemTable 和各个 SST level 的层迭代器
  iter_vec.reserve(1 + engine_->cur_max_level + 1); // cur_max_level 是记录的 id

  auto mem_iter = engine_->memtable.begin(max_tranc_id_, true);
  iter_vec.push_back(std::make_shared<HeapIterator>(mem_iter));

  // 因为 level 0 的 SST 是未排序且有重叠的，因此需要构造为 heap iter
  std::vector<SstIterator> level_0_vec;
  iter_vec.reserve(engine_->level_sst_ids[0].size());
  for (auto id: engine_->level_sst_ids[0]) {
    auto sst_iter = engine_->ssts[id]->begin(max_tranc_id_);
    level_0_vec.push_back(sst_iter);
  }
  auto [heap_begin, heap_end] = SstIterator::merge_sst_iterator(level_0_vec, max_tranc_id_, true);

  iter_vec.push_back(std::make_shared<HeapIterator>(heap_begin));

  // level >= 1 层的 SST 是严格升序且不重复的，因此可以使用 concat iter 来作为层迭代器
  for (int now_level = 1; now_level <= engine_->cur_max_level; now_level ++ ) {
    std::vector<std::shared_ptr<SST>> now_level_vec;
    now_level_vec.reserve(engine_->level_sst_ids[now_level].size());

    for (auto id: engine_->level_sst_ids[now_level]) {
      now_level_vec.push_back(engine_->ssts[id]);
    }

    iter_vec.push_back(std::make_shared<ConcactIterator>(now_level_vec, max_tranc_id));
  }

  // 这里多个迭代器其实就相当于多指针算法了，迭代器从最小 key 开始迭代到 end
  // 作为初始的点，需要找到一个合法的 key ，因为这里可能存在非法即 remove 值
  while (!is_end()) {
    auto [idx, key] = get_min_key_idx();
    if (idx == static_cast<size_t>(-1)) { cached_value = std::nullopt; break; }
    cur_idx_ = idx;
    update_current();
    if (!cached_value.has_value()) break;
    if (cached_value->second.size() == 0) {
      skip_key(cached_value->first);
      continue;
    }
    // 找到第一个合法的键值对
    break;
  }
}

std::pair<size_t, std::string> Level_Iterator::get_min_key_idx() const {
  //获取**当前**存储了最小 key 的迭代器在 iter_vec 中的索引和具体的 key
  size_t min_idx = static_cast<size_t>(-1);
  std::string min_key;
  for (size_t idx = 0; idx < iter_vec.size(); idx ++ ) {
    if (!(*iter_vec[idx]).is_valid()) continue;
    const std::string &key = (**iter_vec[idx]).first;
    if (min_idx == static_cast<size_t>(-1) || key < min_key) {
      min_key = key;
      min_idx = idx;
    }
  }
  return std::make_pair(min_idx, min_key);
}

void Level_Iterator::skip_key(const std::string &key) {
  // 跳过 key 相同的部分(即被当前激活的迭代器覆盖的写入记录)
  for (size_t idx = 0; idx < iter_vec.size(); ++ idx) {
    while (iter_vec[idx]->is_valid() && (**iter_vec[idx]).first == key) {
      ++ (*iter_vec[idx]);
    }
  }
}

void Level_Iterator::update_current() const {
  //更新当前值 cached_value

  // 如果是 end ，则返回 nullptr
  if (!engine_ || !iter_vec[cur_idx_]->is_valid()) {
    cached_value = std::nullopt;
  } else {
    // 否则更新 cache
    cached_value = **iter_vec[cur_idx_];
  }
}

BaseIterator &Level_Iterator::operator++() {
  if (cached_value.has_value()) {
    skip_key(cached_value->first);
  }
  while (!is_end()) {
    auto [idx, min_key] = get_min_key_idx();
    cur_idx_ = idx;
    update_current();
    if (!cached_value.has_value()) break;
    if (cached_value->second.size() == 0) {
      skip_key(min_key);
      continue;
    } else {
      break;
    }
  }
  return *this;
}

bool Level_Iterator::operator==(const BaseIterator &other) const {
  // 迭代器相等语义：两者都为 end 返回 true；两者都有效时，比较当前键值是否相同
  if (other.get_type() != IteratorType::LevelIterator) return false;
  const Level_Iterator &o = static_cast<const Level_Iterator &>(other);
  const bool a_end = this->is_end();
  const bool b_end = o.is_end();
  if (a_end && b_end) return true;
  if (a_end != b_end) return false;
  // 两者都有效，比较当前键值
  auto a_kv = **iter_vec[cur_idx_];
  auto b_kv = **o.iter_vec[o.cur_idx_];
  return a_kv.first == b_kv.first && a_kv.second == b_kv.second;
}

bool Level_Iterator::operator!=(const BaseIterator &other) const {
  return !(other == *this);
}

BaseIterator::value_type Level_Iterator::operator*() const {
  update_current();
  return *cached_value;
}

IteratorType Level_Iterator::get_type() const {
  return IteratorType::LevelIterator;
}

uint64_t Level_Iterator::get_tranc_id() const { return max_tranc_id_; }

bool Level_Iterator::is_end() const {
  for (auto &iter : iter_vec) {
    if ((*iter).is_valid()) {
      return false;
    }
  }
  return true;
}

bool Level_Iterator::is_valid() const { return !is_end(); }

BaseIterator::pointer Level_Iterator::operator->() const {
  update_current();
  return cached_value.has_value() ? &(*cached_value) : nullptr;
}
} // namespace tiny_lsm
