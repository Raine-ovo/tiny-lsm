#include "../../include/block/block_iterator.h"
#include "../../include/block/block.h"
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>

class Block;

namespace tiny_lsm {
BlockIterator::BlockIterator(std::shared_ptr<Block> b, size_t index,
                             uint64_t tranc_id)
    : block(b), current_index(index), tranc_id_(tranc_id),
      cached_value(std::nullopt) {
  skip_by_tranc_id();
}

BlockIterator::BlockIterator(std::shared_ptr<Block> b, const std::string &key,
                             uint64_t tranc_id)
    : block(b), tranc_id_(tranc_id), cached_value(std::nullopt) {
  // 创建迭代器时直接移动到指定的key位置, 借助之前实现的 Block 类的成员函数

  if (!block) {
    return;
  }

  auto idx_opt = block->get_idx_binary(key, tranc_id_);
  if (idx_opt.has_value()) {
    current_index = idx_opt.value();
  } else {
    current_index = block->offsets.size();
  }
}

// BlockIterator::BlockIterator(std::shared_ptr<Block> b, uint64_t tranc_id)
//     : block(b), current_index(0), tranc_id_(tranc_id),
//       cached_value(std::nullopt) {
//   skip_by_tranc_id();
// }

BlockIterator::pointer BlockIterator::operator->() const {
  update_current();
  if (!cached_value.has_value()) return nullptr;
  return &(cached_value.value());
}

BlockIterator &BlockIterator::operator++() {
  // 注意 block 中的数据是 skiplist 中的

  // 如果不是最后一个
  if (block && current_index < block->offsets.size()) {
    // SkipList 在加入事务机制后会出现多个相同 key，需要去重，我们只需要迭代出相同 key 的第一个可见事务即可
    // 先把旧的 key 全部过滤
    std::string old_key = block->get_key_at(block->get_offset_at(current_index));
    while (current_index < block->offsets.size() && block->get_key_at(block->get_offset_at(current_index)) == old_key) {
      ++ current_index;
    }
    // 现在是新的 key 了，注意去除前面的不可见事务
    skip_by_tranc_id();
  }
  return *this;
}

bool BlockIterator::operator==(const BlockIterator &other) const {
  // 智能指针使用 get() 获取对象原始指针，nullptr 也可以使用
  return block.get() == other.block.get() && current_index == other.current_index;
}

bool BlockIterator::operator!=(const BlockIterator &other) const {
  return !(*this == other);
}

BlockIterator::value_type BlockIterator::operator*() const {
  update_current();
  if (!cached_value.has_value()) return {};
  return cached_value.value();
}

bool BlockIterator::is_end() { return !block || current_index == block->offsets.size(); }

// const 成员函数只能调用同样被 const 修饰的成员函数或变量
void BlockIterator::update_current() const {

  if (!block || current_index >= block->offsets.size()) {
    cached_value.reset();
    return;
  }

  size_t offset = block->get_offset_at(current_index);
  cached_value = std::make_pair(block->get_key_at(offset), block->get_value_at(offset));
}

void BlockIterator::skip_by_tranc_id() {
  // 跳过事务ID
  if (!block || tranc_id_ == 0) return;
  while (current_index < block->offsets.size() &&
         block->get_tranc_id_at(block->get_offset_at(current_index)) > tranc_id_)
  {
    ++ current_index;
  }
}
} // namespace tiny_lsm
