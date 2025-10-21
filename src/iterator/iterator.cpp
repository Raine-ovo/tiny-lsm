#include "../../include/iterator/iterator.h"
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace tiny_lsm {

// *************************** SearchItem ***************************
// 注意，小的是新的
/*
  * 排序依据：
  *  1. 先对 key 排序，key 小则新
  *  2. tranc_id 越大的越新
  *  3. SlipList 新旧
*/
bool operator<(const SearchItem &a, const SearchItem &b) {
  // 为了让“更新/较新”的记录优先被选中（在小根堆顶），
  // 当 key 相同时：优先 tranc_id 更大的；若相同再比较 idx_，idx_ 更小的（更“新”的来源）优先。
  if (a.key_ != b.key_) return a.key_ < b.key_;
  if (a.tranc_id_ != b.tranc_id_) return a.tranc_id_ > b.tranc_id_;
  return a.idx_ < b.idx_;
}

bool operator>(const SearchItem &a, const SearchItem &b) {
  return !(a < b || a == b);
}

bool operator==(const SearchItem &a, const SearchItem &b) {
  return a.key_ == b.key_ && a.idx_ == b.idx_ && a.tranc_id_ == b.tranc_id_;
}

// *************************** HeapIterator ***************************
HeapIterator::HeapIterator(bool skip_delete) : skip_delete_(skip_delete) {}

HeapIterator::HeapIterator(std::vector<SearchItem> item_vec,
                           uint64_t max_tranc_id)
    : max_tranc_id_(max_tranc_id), items(item_vec.begin(), item_vec.end()) {

  // 注意构造函数用的 vec 是从 MemTable 中的多个 skiplist 获取的

  // 构建完堆后，需要进行初始化过滤，因为 item_vec 是从 skiplist 提取的，而多个 skiplist 会有重复 key
  // 注意我们构建的堆一定是有效的 kv ，对于追加写的删除操作要过滤（除非是作为组合 iter 使用，过滤删除操作可能会导致被删除的 kv "复活"）
  while (!top_value_legal()) {
    skip_by_tranc_id(); // 跳过不可见事务

    // 这里有一个小细节：链表中相同 key 键，事务 id 是从大到小排序的，
    // 由于已经去除了不可见事务，因此剩下的同键 kv ，他们都是可见的，不需要判断事务可见性
    while (!items.empty() && items.top().value_.empty()) {
      // 顶部元素 value 不存在，说明这个 key 是被删除的，不是我们存储的 kv，需要过滤
      std::string del_key = this->items.top().key_;
      while (!this->items.empty() && this->items.top().key_ == del_key) {
        this->items.pop();
      }
    }
  }
}

HeapIterator::HeapIterator(std::vector<SearchItem> item_vec,
                           uint64_t max_tranc_id, bool compound_iter)
    : tiny_lsm::HeapIterator(item_vec, max_tranc_id) {
      skip_delete_ = !compound_iter;
    }

HeapIterator::pointer HeapIterator::operator->() const {

  // 返回一个指针指向这个迭代器指向的元素，在这个堆迭代器中，就是返回 current 即堆顶元素的指针
  // 注意 heap 可能修改过，所以每次返回指针的时候，都要更新缓存的堆顶元素的指针
  this->update_current();
  return this->current.get(); // get 把 shared_ptr 转化为普通指针类型，也就是 pointer 类型
}

HeapIterator::value_type HeapIterator::operator*() const {

  // 这是重载解引用符号
  this->update_current();
  return *this->current;
}

/*
  * HeapIterator 的自增逻辑
  *  1. 自增后的 key 值不能和上一个 key 相同，不然会覆盖
  *  2. 自增后的 kv 不能是删除标记，即 value 不能为空
*/
BaseIterator &HeapIterator::operator++() {

  if (this->items.empty()) {
    return *this;
  }

  // 迭代器自增
  SearchItem old_item = this->items.top();
  this->items.pop();

  // 查询的是新的 key
  // 自增后的 key 不能是之前相同的 key ，需要跳过
  while (!this->items.empty() && this->items.top().key_ == old_item.key_) this->items.pop();

  // 到了新的 key，有可能事务不可见或者是 remove 操作
  // 处理自增后 current 不合法情况
  if (items.empty()) return *this;

  while (!this->top_value_legal()) {
    skip_by_tranc_id();
    // 删除所有这个 key
    while (!this->items.empty() && this->items.top().value_.empty()) {
      std::string del_key = items.top().key_;
      while (!this->items.empty() && items.top().key_ == del_key)
        this->items.pop();
    }
  }
  return *this;
}

bool HeapIterator::operator==(const BaseIterator &other) const {
  if (other.get_type() != IteratorType::HeapIterator) return false;

  const HeapIterator& other_heap = static_cast<const HeapIterator&>(other);

  if (this->items.empty() && other_heap.items.empty()) return true;
  else if (this->items.empty() || other_heap.items.empty()) return false;

  return this->items.top().key_ == other_heap.items.top().key_
          && this->items.top().value_ == other_heap.items.top().value_;
}

bool HeapIterator::operator!=(const BaseIterator &other) const {
  return !(*this == other);
}

bool HeapIterator::top_value_legal() const {
  // 被删除的值是不合法
  // 不允许访问的事务创建或更改的键值对不合法
  if (this->items.empty()) return true;
  bool is_deleted = (skip_delete_ && this->items.top().value_.empty());
  bool is_invalid_tranc = (max_tranc_id_ != 0 && this->items.top().tranc_id_ > max_tranc_id_);
  return !is_deleted && !is_invalid_tranc;
}

void HeapIterator::skip_by_tranc_id() {
  if (max_tranc_id_ == 0) return;
  while (!items.empty() && items.top().tranc_id_ > max_tranc_id_) items.pop();
}

bool HeapIterator::is_end() const { return items.empty(); }
bool HeapIterator::is_valid() const { return !items.empty(); }

void HeapIterator::update_current() const {
  // current 缓存了当前键值对的值, 你实现 -> 重载时可能需要
  // 更新当前缓存值
  if (this->items.empty()) {
    this->current.reset(); // 释放智能指针对象，这个指针变为空
  } else {
    this->current = std::make_shared<value_type>(this->items.top().key_, this->items.top().value_);
  }
}

IteratorType HeapIterator::get_type() const {
  return IteratorType::HeapIterator;
}

uint64_t HeapIterator::get_tranc_id() const { return max_tranc_id_; }
} // namespace tiny_lsm
