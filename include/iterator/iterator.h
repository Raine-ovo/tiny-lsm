#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

namespace tiny_lsm {

enum class IteratorType {
  SkipListIterator,
  MemTableIterator,
  SstIterator,
  HeapIterator,
  TwoMergeIterator,
  ConcactIterator,
  LevelIterator,
  Undefined,
};

// 使用抽象类定义模板的接口，规定迭代器必须实现的基本功能
class BaseIterator {
public:
  using value_type = std::pair<std::string, std::string>;
  using pointer = value_type *;
  using reference = value_type &;

  virtual BaseIterator& operator++() = 0;
  virtual bool operator==(const BaseIterator &other) const = 0;
  virtual bool operator!=(const BaseIterator &other) const = 0;
  virtual value_type operator*() const = 0;
  virtual IteratorType get_type() const = 0; // 当前是哪个类
  virtual uint64_t get_tranc_id() const = 0;
  virtual bool is_end() const = 0;
  virtual bool is_valid() const = 0;
};

class SstIterator;
// *************************** SearchItem ***************************
// 定义堆的节点的数据结构
struct SearchItem {
  std::string key_;
  std::string value_;
  uint64_t tranc_id_;
  int idx_;
  int level_; // 来自sst的level

  SearchItem() = default;
  SearchItem(std::string k, std::string v, int i, int l, uint64_t tranc_id)
      : key_(std::move(k)), value_(std::move(v)), idx_(i), level_(l),
        tranc_id_(tranc_id) {}
};

bool operator<(const SearchItem &a, const SearchItem &b);
bool operator>(const SearchItem &a, const SearchItem &b);
bool operator==(const SearchItem &a, const SearchItem &b);

// *************************** HeapIterator ***************************
class HeapIterator : public BaseIterator {
  friend class SstIterator;

public:
  HeapIterator() = default;
  // 新增：控制是否跳过删除标记（默认 true 表示跳过删除；false 表示保留删除标记）
  explicit HeapIterator(bool skip_delete);
  HeapIterator(std::vector<SearchItem> item_vec, uint64_t max_tranc_id);
  HeapIterator(std::vector<SearchItem> item_vec, uint64_t max_tranc_id, bool compound_iter);
  pointer operator->() const;
  virtual value_type operator*() const override;
  BaseIterator &operator++() override;
  BaseIterator operator++(int) = delete;
  virtual bool operator==(const BaseIterator &other) const override;
  virtual bool operator!=(const BaseIterator &other) const override;

  virtual IteratorType get_type() const override;
  virtual uint64_t get_tranc_id() const override;
  virtual bool is_end() const override;
  virtual bool is_valid() const override;

private:
  bool top_value_legal() const;
  // bool top_value_deleted() const;
  // bool top_value_invalied_tranc() const;

  // 跳过当前不可见事务的id (如果开启了事务功能)
  void skip_by_tranc_id();

  void update_current() const;

private:
  std::priority_queue<SearchItem, std::vector<SearchItem>,
                      std::greater<SearchItem>>
      items; // 小根堆，顶元素是最小的（即最新的）
  mutable std::shared_ptr<value_type> current; // 用于存储当前元素
  uint64_t max_tranc_id_ = 0;
  bool skip_delete_ = true; // 默认跳过删除标记
};
} // namespace tiny_lsm