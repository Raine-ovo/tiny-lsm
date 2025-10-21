#include "../../include/block/block.h"
#include "../../include/block/block_iterator.h"
#include "../../include/config/config.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <utility>
#include <vector>
#include <algorithm>
#include "../../include/utils/debug.h"

namespace tiny_lsm {
Block::Block(size_t capacity) : capacity(capacity) {}

std::vector<uint8_t> Block::encode() {
  // 编码单个类实例形成一段字节数组

  // 计算总需要的字节数
  // 对于 c++ 的容器， sizeof 返回的是容器这个结构的大小，而不是实际存储元素的字节大小
  // 比如 std::vector 返回的是这个结构本身的大小，包括指针、容量、大小等
  size_t encoded_bytes = data.size() * sizeof(uint8_t) +
                          offsets.size() * sizeof(uint16_t) + sizeof(uint16_t); // 元素数量用 2B 表示

  // 依次把 data、offsets、num 编码
  std::vector<uint8_t> encoded(encoded_bytes, 0);

  // 1. data
  memcpy(encoded.data(), data.data(), data.size() * sizeof(uint8_t));

  // 2. offsets
  size_t offset_pos = data.size() * sizeof(uint8_t);
  memcpy(encoded.data() + offset_pos, offsets.data(), offsets.size() * sizeof(uint16_t));

  // 3. num elements
  size_t num_pos = offset_pos + offsets.size() * sizeof(uint16_t);
  uint16_t num_elements = offsets.size();
  memcpy(encoded.data() + num_pos, &num_elements, sizeof(uint16_t));

  return encoded;
}

std::shared_ptr<Block> Block::decode(const std::vector<uint8_t> &encoded,
                                     bool with_hash) {
  // 解码字节数组形成类实例

  // debug("decode start");

  size_t encoded_bytes = encoded.size() * sizeof(uint8_t);

  std::shared_ptr<Block> block = std::make_shared<Block>();
  // 依次解码 num、offsets、data

  if (encoded_bytes <= sizeof(uint16_t)) {
    debug("Decoded data too small");
    throw std::runtime_error("Encoded data too small");
  }

  // 1. 先解码 num
  size_t num_pos = encoded_bytes - sizeof(uint16_t);
  // 存在 hash 校验 ，因为 hash 值在最后面，因此要先读取
  if (with_hash) {
    if (with_hash && encoded_bytes <= sizeof(uint16_t) + sizeof(uint32_t)) {
      // 数据异常，使用 throw 来抛出异常
      debug("Decoded data too small");
      throw std::runtime_error("Encoded data too small");
    }

    num_pos -= sizeof(uint32_t);
    uint32_t hash;
    memcpy(&hash, encoded.data() + encoded_bytes - sizeof(uint32_t), sizeof(uint32_t));

    // 计算 encoded 数据的 hash
    uint32_t compute_hash = std::hash<std::string_view>{}(
          std::string_view(reinterpret_cast<const char*>(encoded.data()), encoded_bytes - sizeof(uint32_t)));

    if (hash != compute_hash) {
      debug("Decoded data too small");
      throw std::runtime_error("Block hash verification failed.");
    }
  }

  uint16_t num_elements;
  memcpy(&num_elements, encoded.data() + num_pos, sizeof(uint16_t));

  // 2. 有了 num 后可以解码 offsets
  // 解码前先校验
  size_t required_size = sizeof(uint16_t) + num_elements * sizeof(uint16_t);
  if (encoded_bytes < required_size) {
    // 数据异常，使用 throw 来抛出异常
    debug("Decoded data too small");
    throw std::runtime_error("Invalid encode data size");
  }
  size_t offsets_pos = num_pos - num_elements * sizeof(uint16_t);
  // ! 注意现在 offsets 是空的 vector ，需要先扩容才能写入
  block->offsets.resize(num_elements); // 参数是元素个数
  memcpy(block->offsets.data(), encoded.data() + offsets_pos, num_elements * sizeof(uint16_t));

  // 3. 最后就可以用剩下的 bytes 解码 data
  block->data.resize(offsets_pos);
  memcpy(block->data.data(), encoded.data() , offsets_pos);

  // debug("decode finished");
  return block;
}

std::string Block::get_first_key() {
  if (data.empty() || offsets.empty()) {
    return "";
  }

  // 读取第一个key的长度（前2字节）
  uint16_t key_len;
  memcpy(&key_len, data.data(), sizeof(uint16_t));

  // 读取key
  std::string key(reinterpret_cast<char *>(data.data() + sizeof(uint16_t)),
                  key_len);
  return key;
}

size_t Block::get_offset_at(size_t idx) const {
  if (idx > offsets.size()) {
    throw std::runtime_error("idx out of offsets range");
  }
  return offsets[idx];
}

/*
  上层 SST 调用 add_entry 接口来讲一个 entry 加入到 block 中
*/
bool Block::add_entry(const std::string &key, const std::string &value,
                      uint64_t tranc_id, bool force_write) {
  // 添加一个键值对到block中
  // 返回值说明：
  // true: 成功添加
  // false: block已满, 拒绝此次添加

  // offsets 存储的是字节偏移量，因为 data 是 vector<uint8_t> ，因此元素个数实际上就算字节量
  size_t data_bytes = data.size() * sizeof(uint8_t);
  size_t entry_bytes = sizeof(uint16_t) * 2 + key.size() + value.size() + sizeof(uint64_t);

  // sizeof(uint16_t) 是 offsets 的新增数据量
  if (!force_write && cur_size() + entry_bytes + sizeof(uint16_t) > capacity) {
    // debug("block write failed: ", entry_bytes + sizeof(uint16_t), capacity);
    return false;
  }

  // 1. data
  data.resize(data_bytes + entry_bytes);

  // key_len
  uint16_t key_len = key.size();
  memcpy(data.data() + data_bytes, &key_len, sizeof(uint16_t));
  // key
  size_t key_pos = data_bytes + sizeof(uint16_t);
  memcpy(data.data() + key_pos, key.data(), key_len);
  // value_len
  uint16_t value_len = value.size();
  size_t value_len_pos = key_pos + key_len;
  memcpy(data.data() + value_len_pos, &value_len, sizeof(uint16_t));
  // value
  size_t value_pos = value_len_pos + sizeof(uint16_t);
  memcpy(data.data() + value_pos, value.data(), value_len);
  // tranc_id
  size_t trac_id_pos = value_pos + value_len;
  memcpy(data.data() + trac_id_pos, &tranc_id, sizeof(uint64_t));

  // 2. offsets
  offsets.push_back(data_bytes);

  return true;
}

// 从指定偏移量获取entry的key
/*
  注意，这里的 offset 是从 offsets 数组获取的，因此这里的 offset 表示的是这个 entry 的 offset
*/
std::string Block::get_key_at(size_t offset) const {
  uint16_t key_len;
  memcpy(&key_len, data.data() + offset, sizeof(uint16_t));
  return std::string(reinterpret_cast<const char*>(data.data() + offset + sizeof(uint16_t)), key_len);
}

// 从指定偏移量获取entry的value
std::string Block::get_value_at(size_t offset) const {
  uint16_t key_len, value_len;
  memcpy(&key_len, data.data() + offset, sizeof(uint16_t));
  size_t value_len_pos = offset + sizeof(uint16_t) + key_len;
  memcpy(&value_len, data.data() + value_len_pos, sizeof(uint16_t));
  return std::string(reinterpret_cast<const char*>(data.data() + value_len_pos + sizeof(uint16_t)), value_len);
}

uint64_t Block::get_tranc_id_at(size_t offset) const {
  // 从指定偏移量获取entry的tranc_id
  uint16_t key_len, value_len;
  uint64_t trac_id;

  memcpy(&key_len, data.data() + offset, sizeof(uint16_t));

  size_t value_len_pos = offset + sizeof(uint16_t) + key_len;
  memcpy(&value_len, data.data() + value_len_pos, sizeof(uint16_t));

  size_t tranc_id_pos = value_len_pos + sizeof(uint16_t) + value_len;

  memcpy(&trac_id, data.data() + tranc_id_pos, sizeof(uint64_t));
  return trac_id;
}

// 比较指定偏移量处的key与目标key
int Block::compare_key_at(size_t offset, const std::string &target) const {
  std::string key = get_key_at(offset);
  return key.compare(target);
}

// 相同的key连续分布, 且相同的key的事务id从大到小排布
// 这里的逻辑是找到最接近 tranc_id 的键值对的索引位置
int Block::adjust_idx_by_tranc_id(size_t idx, uint64_t tranc_id) {
  if (idx >= offsets.size()) return -1;

  // 对于一个从大到小的序列，找到第一个 <= tranc_id 的索引
  // idx 处的 key 和目标 key 相同
  std::string key = get_key_at(get_offset_at(idx));

  // 没有加入事务机制时，block 内部的 key 是唯一的，或者对于相同 key 取最大事务版本
  // tranc_id == 0 表示这个 get 不启用事务，按照普通读取即可，也就是读取最大事务（同键最左位置）
  if (tranc_id == 0) {
    while (idx > 0 && get_key_at(get_offset_at(idx - 1)) == key) {
      --idx;
    }
    return static_cast<int>(idx);
  }

  uint64_t ori_tranc_id = get_tranc_id_at(get_offset_at(idx));


  if (ori_tranc_id <= tranc_id) {
    // 1. 如果 idx 应该往 trac id 大的走（保持 <= tranc_id 的最左位置）
    while (idx > 0 &&
           get_key_at(get_offset_at(idx-1)) == key &&
           get_tranc_id_at(get_offset_at(idx-1)) <= tranc_id)
      -- idx;
    return static_cast<int>(idx);
  }
  else {
    // 2. 如果 idx 应该往 tranc id 小的走
    while (idx + 1 < offsets.size() &&
           get_key_at(get_offset_at(idx+1)) == key)
    {
      ++ idx;
      if (get_tranc_id_at(get_offset_at(idx)) <= tranc_id) return static_cast<int>(idx);
    }
    return -1;
  }
}

bool Block::is_same_key(size_t idx, const std::string &target_key) const {
  if (idx >= offsets.size()) {
    return false; // 索引超出范围
  }
  return get_key_at(offsets[idx]) == target_key;
}

// 使用二分查找获取value
// 要求在插入数据时有序插入
std::optional<std::string> Block::get_value_binary(const std::string &key,
                                                   uint64_t tranc_id) {
  auto idx = get_idx_binary(key, tranc_id);
  if (!idx.has_value()) {
    return std::nullopt;
  }

  return get_value_at(offsets[*idx]);
}

// block 的数据是有序的
std::optional<size_t> Block::get_idx_binary(const std::string &key,
                                            uint64_t tranc_id) {
  // 使用二分查找获取key对应的索引
  if (offsets.empty()) {
    return std::nullopt;
  }

  int l = 0, r = offsets.size() - 1;
  while (l <= r) {
    int mid = l + (r - l) / 2;
    int result = compare_key_at(offsets[mid], key);
    if (result == 0) {
      // 找到可见事务
      int may = adjust_idx_by_tranc_id(mid, tranc_id);
      if (may == -1) return std::nullopt;
      return may;
    } else if (result < 0) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  return std::nullopt;
}

std::optional<
    std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
Block::iters_preffix(uint64_t tranc_id, const std::string &preffix) {
  // 获取前缀匹配的区间迭代器

  // b 是否是 a 的前缀
  auto is_preffix = [](const std::string& a, const std::string& b) -> bool {
    if (b.size() > a.size()) return false;
    return std::equal(b.begin(), b.end(), a.begin());
  };

  BlockIterator begin = this->begin(tranc_id);

  for (; begin != this->end(); ++begin) {
    if (is_preffix((*begin).first, preffix)) {
      break;
    }
  }

  if (begin == this->end()) {
    return std::nullopt;
  }

  BlockIterator end = begin;
  while (end != this->end() && is_preffix((*end).first, preffix)) ++end;

  return std::make_pair(std::make_shared<BlockIterator>(begin), std::make_shared<BlockIterator>(end));
}

// 返回第一个满足谓词的位置和最后一个满足谓词的位置
// 如果不存在, 范围nullptr
// 谓词作用于key, 且保证满足谓词的结果只在一段连续的区间内, 例如前缀匹配的谓词
// 返回的区间是闭区间, 开区间需要手动对返回值自增
// predicate返回值:
//   0: 满足谓词
//   >0: 不满足谓词, 需要向右移动
//   <0: 不满足谓词, 需要向左移动
std::optional<
    std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
Block::get_monotony_predicate_iters(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  // 使用二分查找获取满足谓词的区间迭代器

  if (offsets.empty()) return std::nullopt;

  auto key_at_index = [&](size_t idx) -> const std::string {
    return get_key_at(get_offset_at(idx));
  };

  // 寻找左边界：第一个使 predicate(key) <= 0 的位置
  size_t n = offsets.size();
  size_t l = 0, r = n;
  while (l < r) {
    size_t mid = l + (r - l) / 2;
    int ret = predicate(key_at_index(mid));
    if (ret > 0) {
      l = mid + 1; // 需要向右
    } else {
      r = mid;     // <=0 往左收缩
    }
  }
  size_t begin_index = l;

  // 验证是否存在满足谓词的元素
  if (begin_index >= n || predicate(key_at_index(begin_index)) != 0) {
    return std::nullopt;
  }

  // 寻找右边界：第一个使 predicate(key) < 0 的位置（半开区间）
  l = begin_index;
  r = n;
  while (l < r) {
    size_t mid = l + (r - l) / 2;
    int ret = predicate(key_at_index(mid));
    if (ret < 0) {
      r = mid; // 右侧区域
    } else {
      l = mid + 1; // 仍在 0 或 1 区域，继续右移
    }
  }
  size_t end_index = l; // 半开区间 [begin_index, end_index)

  return std::make_pair(
      std::make_shared<BlockIterator>(shared_from_this(), begin_index, tranc_id),
      std::make_shared<BlockIterator>(shared_from_this(), end_index, tranc_id));
}

Block::Entry Block::get_entry_at(size_t offset) const {
  Entry entry;
  entry.key = get_key_at(offset);
  entry.value = get_value_at(offset);
  entry.tranc_id = get_tranc_id_at(offset);
  return entry;
}

size_t Block::size() const { return offsets.size(); }

size_t Block::cur_size() const {
  return data.size() + offsets.size() * sizeof(uint16_t) + sizeof(uint16_t);
}

bool Block::is_empty() const { return offsets.empty(); }

BlockIterator Block::begin(uint64_t tranc_id) {
  // 因为在 SST 创建该对象的时候用的就是 shared_ptr ，因此不用担心这里是第一次使用智能指针导致抛出异常
  return BlockIterator(shared_from_this(), 0, tranc_id);
}

BlockIterator Block::end() {
  // 左闭右开
  return BlockIterator(shared_from_this(), offsets.size(), 0);
}
} // namespace tiny_lsm
