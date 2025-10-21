#include "../../include/lsm/two_merge_iterator.h"
#include <linux/limits.h>

namespace tiny_lsm {

TwoMergeIterator::TwoMergeIterator() {}

TwoMergeIterator::TwoMergeIterator(std::shared_ptr<BaseIterator> it_a,
                                   std::shared_ptr<BaseIterator> it_b,
                                   uint64_t max_tranc_id)
    : it_a(std::move(it_a)), it_b(std::move(it_b)),
      max_tranc_id_(max_tranc_id) {
  // 先跳过不可见的事务
  skip_by_tranc_id();
  skip_it_b();              // 跳过与 it_a 重复的 key
  choose_a = choose_it_a(); // 决定使用哪个迭代器
}

bool TwoMergeIterator::choose_it_a() {
  // 优先选择 it_a；当 key 相等时也优先 it_a
  if (it_a == nullptr || it_a->is_end()) return false;
  if (it_b == nullptr || it_b->is_end()) return true;
  skip_it_b();
  return ((**it_a).first <= (**it_b).first);
}

void TwoMergeIterator::skip_it_b() {
  if (!it_a->is_end() && !it_b->is_end() && (**it_a).first == (**it_b).first) {
    ++(*it_b);
  }
}

void TwoMergeIterator::skip_by_tranc_id() {
  if (max_tranc_id_ == 0) return ;
  while (!it_a->is_end() && it_a->get_tranc_id() > max_tranc_id_) ++ (*it_a);
  while (!it_b->is_end() && it_b->get_tranc_id() > max_tranc_id_) ++ (*it_b);
}

BaseIterator &TwoMergeIterator::operator++() {
  if (is_end()) return *this;
  choose_a = choose_it_a();
  if (choose_a) {
    ++(*it_a);
  } else {
    ++(*it_b);
  }
  // 自增后跳过不可见 & 重复 key
  skip_by_tranc_id();
  skip_it_b();
  return *this;
}

bool TwoMergeIterator::operator==(const BaseIterator &other) const {
  if (other.get_type() != IteratorType::TwoMergeIterator) return false;

  const TwoMergeIterator &other_iter = static_cast<const TwoMergeIterator &>(other);
  const bool a_end = this->is_end();
  const bool b_end = other_iter.is_end();
  if (a_end && b_end) return true;
  if (a_end != b_end) return false;

  // 两者都有效，比较当前键值
  auto a_kv = this->operator*();
  auto b_kv = other_iter.operator*();
  return a_kv.first == b_kv.first && a_kv.second == b_kv.second;
}

bool TwoMergeIterator::operator!=(const BaseIterator &other) const {
  return !(other == *this);
}

BaseIterator::value_type TwoMergeIterator::operator*() const {
  update_current();
  if (!current) return {};
  return *current;
}

IteratorType TwoMergeIterator::get_type() const {
  return IteratorType::TwoMergeIterator;
}

uint64_t TwoMergeIterator::get_tranc_id() const { return max_tranc_id_; }

bool TwoMergeIterator::is_end() const {
  if (it_a == nullptr && it_b == nullptr) {
    return true;
  }
  if (it_a == nullptr) {
    return it_b->is_end();
  }
  if (it_b == nullptr) {
    return it_a->is_end();
  }
  return it_a->is_end() && it_b->is_end();
}

bool TwoMergeIterator::is_valid() const {
  if (it_a == nullptr && it_b == nullptr) {
    return false;
  }
  if (it_a == nullptr) {
    return it_b->is_valid();
  }
  if (it_b == nullptr) {
    return it_a->is_valid();
  }
  return it_a->is_valid() || it_b->is_valid();
}

TwoMergeIterator::pointer TwoMergeIterator::operator->() const {
  update_current();
  return current ? current.get() : nullptr;
}

void TwoMergeIterator::update_current() const {
  if (!is_valid()) {
    current.reset();
    return;
  }

  if (!current) current = std::make_shared<value_type>();

  bool use_a = false;
  if (it_a && !it_a->is_end()) {
    if (!it_b || it_b->is_end()) {
      use_a = true;
    } else {
      use_a = ((**it_a).first <= (**it_b).first);
    }
  } else {
    use_a = false;
  }

  if (use_a) {
    *current = (**it_a);
  } else {
    *current = (**it_b);
  }
}
} // namespace tiny_lsm
