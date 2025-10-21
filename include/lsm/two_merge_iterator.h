#pragma once

#include "../iterator/iterator.h"
#include "../sst/sst_iterator.h"

#include <memory>

namespace tiny_lsm {

// 双路归并迭代器，将两个已经有序的迭代器合并为逻辑上仍然有序的迭代器
class TwoMergeIterator : public BaseIterator {
private:
  // 看成两个有序数组，merge 合并后必须仍然是有序数组
  std::shared_ptr<BaseIterator> it_a;
  std::shared_ptr<BaseIterator> it_b;
  // 这就类似双指针算法了，choose_a 作用就是告诉我们现在应该选择哪个迭代器指向的元素
  bool choose_a = false;
  mutable std::shared_ptr<value_type> current; // 用于存储当前元素
  uint64_t max_tranc_id_ = 0;

  void update_current() const;

public:
  TwoMergeIterator();
  TwoMergeIterator(std::shared_ptr<BaseIterator> it_a,
                   std::shared_ptr<BaseIterator> it_b, uint64_t max_tranc_id);
  bool choose_it_a();
  // 跳过当前不可见事务的id (如果开启了事务功能)
  void skip_by_tranc_id();
  void skip_it_b();

  virtual BaseIterator &operator++() override;
  virtual bool operator==(const BaseIterator &other) const override;
  virtual bool operator!=(const BaseIterator &other) const override;
  virtual value_type operator*() const override;
  virtual IteratorType get_type() const override;
  virtual uint64_t get_tranc_id() const override;
  virtual bool is_end() const override;
  virtual bool is_valid() const override;

  pointer operator->() const;
};
} // namespace tiny_lsm