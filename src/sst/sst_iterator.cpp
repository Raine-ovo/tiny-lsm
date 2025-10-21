#include "../../include/sst/sst_iterator.h"
#include "../../include/sst/sst.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>

namespace tiny_lsm {

// predicate返回值:
//   0: 谓词
//   >0: 不满足谓词, 需要向右移动
//   <0: 不满足谓词, 需要向左移动
std::optional<std::pair<SstIterator, SstIterator>> sst_iters_monotony_predicate(
    std::shared_ptr<SST> sst, uint64_t tranc_id,
    std::function<int(const std::string &)> predicate) {

  // 返回一个左闭右开的迭代区间

  // predicate 的满足范围是 SST 的一个连续 key 区间
  // 实现思路：1. 找到第一个满足谓词的 block 2. 找到最后一个满足谓词的 block
  // 第一个满足谓词，即：第一个 <= 0 的
  // 最后一个满足谓词的 block 的后一个，即第一个 < 0 的

  // <= 0
  auto maybe_exist_or_back = [&](uint32_t block_idx) -> bool {
    const std::string &first_key = sst->meta_entries[block_idx].first_key;
    const std::string &last_key = sst->meta_entries[block_idx].last_key;
    return predicate(first_key) >= 0 && predicate(last_key) <= 0 || predicate(first_key) < 0;
  };

  // >= 0
  auto maybe_exist_or_forward = [&](uint32_t block_idx) -> bool {
    const std::string &first_key = sst->meta_entries[block_idx].first_key;
    const std::string &last_key = sst->meta_entries[block_idx].last_key;
    return predicate(first_key) >= 0 && predicate(last_key) <= 0 || predicate(last_key) > 0;
  };

  if (sst->meta_entries.empty()) return std::nullopt;
  SstIterator begin_iter = SstIterator{sst, tranc_id};
  SstIterator end_iter = SstIterator{sst, tranc_id};

  // 第一个满足谓词的 block
  uint32_t l = 0, r = sst->meta_entries.size() - 1;
  while (l < r) {
    uint32_t mid = l + (r - l) / 2;
    if (maybe_exist_or_back(mid)) r = mid;
    else l = mid + 1;
  }

  // 查找这个 block 中的 idx，创建 blockiter
  auto lr_opt = sst->read_block(r)->get_monotony_predicate_iters(tranc_id, predicate);
  if (!lr_opt.has_value()) return std::nullopt;
  begin_iter.m_block_idx = r;
  begin_iter.m_block_it = lr_opt.value().first;

  // 最后一个满足谓词的 block, 即最后一个满足 >= 0 的
  l = 0, r = sst->meta_entries.size() - 1;
  while (l < r) {
    uint32_t mid = l + (r - l + 1) / 2;
    if (maybe_exist_or_forward(mid)) l = mid;
    else r = mid - 1;
  }

  // 查找这个 block 中的 idx，创建 blockiter
  lr_opt = sst->read_block(r)->get_monotony_predicate_iters(tranc_id, predicate);
  if (!lr_opt.has_value()) return std::nullopt;
  end_iter.m_block_idx = r;
  end_iter.m_block_it = lr_opt.value().second;

  return std::make_pair(begin_iter, end_iter);
}

SstIterator::SstIterator(std::shared_ptr<SST> sst, uint64_t tranc_id)
    : m_sst(sst), m_block_idx(0), m_block_it(nullptr), max_tranc_id_(tranc_id) {
  if (m_sst) {
    seek_first();
  }
}

SstIterator::SstIterator(std::shared_ptr<SST> sst, const std::string &key,
                         uint64_t tranc_id)
    : m_sst(sst), m_block_idx(0), m_block_it(nullptr), max_tranc_id_(tranc_id) {
  if (m_sst) {
    seek(key);
  }
}

void SstIterator::set_block_idx(size_t idx) { m_block_idx = idx; }
void SstIterator::set_block_it(std::shared_ptr<BlockIterator> it) {
  m_block_it = it;
}

void SstIterator::seek_first() {
  // 这里的第一个 key 指的是 SST 文件中的第一个 key
  // 实现方式就是定位 key 所在的 block 对应 iter
  m_block_idx = 0; // 第一个 block
  m_block_it = std::make_shared<BlockIterator>(m_sst->read_block(m_block_idx), 0, max_tranc_id_);
}

void SstIterator::seek(const std::string &key) {
  // 首先判断这个 SST 中是否存在这个 key
  std::optional<uint32_t> idx = m_sst->find_block_idx(key);
  if (!idx.has_value()) {
    // 不存在则返回无效迭代器
    m_sst = nullptr;
    m_block_it = nullptr;
    return;
    // throw std::runtime_error("Sst seek failed: can't find the correct block with expected key");
  }
  m_block_idx = idx.value();
  std::shared_ptr<Block> block = m_sst->read_block(m_block_idx);
  // 从 block 获取 key 的 entry idx
  auto key_idx = block->get_idx_binary(key, max_tranc_id_);
  if (!key_idx.has_value()) {
    // 将迭代器置为无效（对应 end 语义）
    m_block_it = nullptr;
    m_sst = nullptr;
  } else {
    m_block_it = std::make_shared<BlockIterator>(block, key_idx.value(), max_tranc_id_);
  }
}

std::string SstIterator::key() {
  if (!m_block_it) {
    throw std::runtime_error("Iterator is invalid");
  }
  return (*m_block_it)->first;
}

std::string SstIterator::value() {
  if (!m_block_it) {
    throw std::runtime_error("Iterator is invalid");
  }
  return (*m_block_it)->second;
}

BaseIterator &SstIterator::operator++() {

  // 保护空指针，防止对空迭代器递增
  if (!m_block_it) {
    return *this;
  }

  // 如果当前 block 查完了
  ++(*m_block_it);
  if (m_block_it->is_end()) {
    // 更换 block
    ++m_block_idx;
    if (m_sst && m_block_idx < m_sst->num_blocks()) {
      m_block_it = std::make_shared<BlockIterator>(m_sst->read_block(m_block_idx), 0, max_tranc_id_);
    } else {
      // 已经到达 SST 末尾
      m_block_it = nullptr;
    }
  }
  return *this;
}

bool SstIterator::operator==(const BaseIterator &other) const {
  if (other.get_type() != IteratorType::SstIterator) return false;

  const SstIterator &other_iter = static_cast<const SstIterator&>(other);
  if (m_sst != other_iter.m_sst || m_block_idx != other_iter.m_block_idx) {
    return false;
  }

  // 两者 block 迭代器都为空，视为相等（均为 end）
  if (!m_block_it && !other_iter.m_block_it) {
    return true;
  }
  // 仅一方为空，不相等
  if (!m_block_it || !other_iter.m_block_it) {
    return false;
  }

  // 比较底层 BlockIterator 的位置
  return *m_block_it == *other_iter.m_block_it;
}

bool SstIterator::operator!=(const BaseIterator &other) const {
  return !(*this == other);
}

SstIterator::value_type SstIterator::operator*() const {
  update_current();
  if (!cached_value.has_value()) return {};
  return *cached_value;
}

IteratorType SstIterator::get_type() const { return IteratorType::SstIterator; }

uint64_t SstIterator::get_tranc_id() const { return max_tranc_id_; }
bool SstIterator::is_end() const { return !m_block_it || !m_sst || m_block_idx == m_sst->num_blocks(); }

bool SstIterator::is_valid() const {
  return m_block_it && m_sst && !m_block_it->is_end();
}
SstIterator::pointer SstIterator::operator->() const {
  update_current();
  return &(*cached_value);
}

void SstIterator::update_current() const {
  if (m_block_it && !m_block_it->is_end()) {
    cached_value = *(*m_block_it);
  } else {
    cached_value.reset();
  }
}

// 合并为 Heap Iter 给上层组件使用
// 这里有个问题，如果是给 MemTable 使用，可以直接跳过 remove 操作，
// 因为只有一个 iter ，最新的跳过，后面相同 key 也跳过，没问题，后面也不会再有这个 key
// 但是对于 compact_l0_l1 或者 level_iterator ，他们使用了组合 iter ，如果直接 skip remove
// 操作，会导致优先级低的 iter 的旧 put 操作被 "复活"
// 因此这里加一个 skip 变量，表示是否需要跳过 remove 操作
std::pair<HeapIterator, HeapIterator>
SstIterator::merge_sst_iterator(std::vector<SstIterator> iter_vec,
                                uint64_t tranc_id, bool compound_iter) {
  if (iter_vec.empty()) {
    return std::make_pair(HeapIterator(), HeapIterator());
  }

  // 如果是组合 iter ，则不应该跳过 remove
  HeapIterator it_begin(!compound_iter); // compaction: 保留 tombstone
  for (auto &iter : iter_vec) {
    while (iter.is_valid() && !iter.is_end()) {
      it_begin.items.emplace(
          iter.key(), iter.value(), -iter.m_sst->get_sst_id(), 0,
          tranc_id); // ! 此处的level暂时没有作用, 都作用于同一层的比较
      ++iter;
    }
  }
  return std::make_pair(it_begin, HeapIterator());
}
} // namespace tiny_lsm
