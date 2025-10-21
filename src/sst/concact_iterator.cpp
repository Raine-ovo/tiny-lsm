#include "../../include/sst/concact_iterator.h"
#include <stdexcept>

namespace tiny_lsm {

ConcactIterator::ConcactIterator(std::vector<std::shared_ptr<SST>> ssts,
                                 uint64_t tranc_id)
    : ssts(ssts), cur_iter(nullptr, tranc_id), cur_idx(0),
      max_tranc_id_(tranc_id) {
  if (!this->ssts.empty()) {
    cur_iter = ssts[0]->begin(max_tranc_id_);
  }
}

BaseIterator &ConcactIterator::operator++() {
  if (cur_idx >= ssts.size()) return *this;

  ++cur_iter;
  // 向后推进到下一个非空的 sst
  while ((cur_iter.is_end() || !cur_iter.is_valid()) && cur_idx + 1 < ssts.size()) {
    ++cur_idx;
    cur_iter = ssts[cur_idx]->begin(max_tranc_id_);
  }
  return *this;
}

bool ConcactIterator::operator==(const BaseIterator &other) const {
  if (other.get_type() != IteratorType::ConcactIterator) return false;

  const ConcactIterator& other_iter = static_cast<const ConcactIterator&>(other);
  return other_iter.cur_idx == cur_idx && other_iter.ssts == ssts && other_iter.cur_iter == cur_iter;
}

bool ConcactIterator::operator!=(const BaseIterator &other) const {
  return !(other == *this);
}

ConcactIterator::value_type ConcactIterator::operator*() const {
  if (!is_valid()) return {};

  return *cur_iter;
}

IteratorType ConcactIterator::get_type() const {
  return IteratorType::ConcactIterator;
}

uint64_t ConcactIterator::get_tranc_id() const { return max_tranc_id_; }

bool ConcactIterator::is_end() const {
  // 当 cur_idx 已经越界，或者在最后一个 sst 上且其迭代器结束
  if (ssts.empty()) return true;
  if (cur_idx >= ssts.size()) return true;
  if (cur_idx == ssts.size() - 1) return cur_iter.is_end() || !cur_iter.is_valid();
  return false;
}

bool ConcactIterator::is_valid() const {
  if (ssts.empty()) return false;
  if (cur_idx >= ssts.size()) return false;
  return cur_iter.is_valid() && !cur_iter.is_end();
}

ConcactIterator::pointer ConcactIterator::operator->() const {
  return cur_iter.operator->();
}

std::string ConcactIterator::key() { return cur_iter.key(); }

std::string ConcactIterator::value() { return cur_iter.value(); }
} // namespace tiny_lsm
