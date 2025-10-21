// include/utils/bloom_filter.cpp

#include "../..//include/utils/bloom_filter.h"
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>
#include "../../include/utils/debug.h"

namespace tiny_lsm {

BloomFilter::BloomFilter() {};

// 构造函数，初始化布隆过滤器
// expected_elements: 预期插入的元素数量
// false_positive_rate: 允许的假阳性率
BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate)
    : expected_elements_(expected_elements),
      false_positive_rate_(false_positive_rate) {
  // return;

  // 初始化数组长度
  // debug(expected_elements, false_positive_rate);
  // 这里注意不要写成 -expected_elements ，因为这个数值是无符号的，直接取负会环绕成大正数
  num_bits_ = static_cast<size_t>(std::ceil(expected_elements * std::log(1.0 / false_positive_rate) / std::pow(std::log(2), 2)));
  num_hashes_ = static_cast<size_t>(std::log2(1 / false_positive_rate));
  // 初始化位数组
  // debug(num_bits_, num_hashes_);
  bits_.resize(num_bits_);
}

void BloomFilter::add(const std::string &key) {
  // return;
  // 添加一个记录到布隆过滤器中
  size_t hash1_value = hash1(key);
  bits_[hash1_value] = true;
  size_t hash2_value = hash2(key);
  bits_[hash2_value] = true;
  for (int i = 3; i <= num_hashes_; i ++ ) {
    size_t hash_i_value = hash(key, i);
    bits_[hash_i_value] = true;
  }
}

//  如果key可能存在于布隆过滤器中，返回true；否则返回false
bool BloomFilter::possibly_contains(const std::string &key) const {
  // return false;
  // 检查一个记录是否可能存在于布隆过滤器中
  size_t hash1_value = hash1(key);
  if (!bits_[hash1_value]) return false;
  size_t hash2_value = hash2(key);
  if (!bits_[hash2_value]) return false;
  for (int i = 3; i <= num_hashes_; i ++ ) {
    size_t hash_i_value = hash(key, i);
    if (!bits_[hash_i_value]) return false;
  }
  return true;
}

// 清空布隆过滤器
void BloomFilter::clear() { bits_.assign(bits_.size(), false); }

size_t BloomFilter::hash1(const std::string &key) const {
  std::hash<std::string> hasher;
  return hasher(key) % num_bits_;
}

size_t BloomFilter::hash2(const std::string &key) const {
  std::hash<std::string> hasher;
  return hasher(key + "salt") % num_bits_;
}

size_t BloomFilter::hash(const std::string &key, size_t idx) const {
  // 计算哈希值
  // ? idx 标识这是第几个哈希函数
  return (hash1(key) + idx * hash2(key)) % num_bits_;
}

// 编码布隆过滤器为 std::vector<uint8_t>
std::vector<uint8_t> BloomFilter::encode() {
  // return{};

  size_t meta_bytes = sizeof(size_t) * 3 + sizeof(double);
  size_t bit_bytes = (bits_.size() + 7) / 8; // 按位压缩存储
  size_t total_bytes = meta_bytes + bit_bytes;

  std::vector<uint8_t> encoded(total_bytes);

  // 存储 meta data
  size_t offset = 0;
  // expected_elements
  memcpy(encoded.data() + offset, &expected_elements_, sizeof(size_t));
  offset += sizeof(size_t);
  // false_positive_rate
  memcpy(encoded.data() + offset, &false_positive_rate_, sizeof(double));
  offset += sizeof(double);
  // num_bits_
  memcpy(encoded.data() + offset, &num_bits_, sizeof(size_t));
  offset += sizeof(size_t);
  // num_hashes_
  memcpy(encoded.data() + offset, &num_hashes_, sizeof(size_t));
  offset += sizeof(size_t);

  // 将 bits_ 压缩到字节数组
  std::fill(encoded.begin() + static_cast<long>(offset), encoded.end(), 0);
  for (size_t i = 0; i < bits_.size(); ++i) {
    if (bits_[i]) {
      size_t byte_index = i / 8;
      uint8_t bit_mask = static_cast<uint8_t>(1u << (i % 8));
      encoded[offset + byte_index] |= bit_mask;
    }
  }
  return encoded;
}

// 从 std::vector<uint8_t> 解码布隆过滤器
BloomFilter BloomFilter::decode(const std::vector<uint8_t> &data) {
  // return BloomFilter{};
  BloomFilter bf;

  // meta data: expected_num, false_positive_rate, num_bits, num_hashes
  size_t offset = 0;
  memcpy(&bf.expected_elements_, data.data() + offset, sizeof(size_t));
  offset += sizeof(size_t);
  memcpy(&bf.false_positive_rate_, data.data() + offset, sizeof(double));
  offset += sizeof(double);
  memcpy(&bf.num_bits_, data.data() + offset, sizeof(size_t));
  offset += sizeof(size_t);
  memcpy(&bf.num_hashes_, data.data() + offset, sizeof(size_t));
  offset += sizeof(size_t);

  // bits (按位压缩 -> 解压)
  bf.bits_.assign(bf.num_bits_, false);
  for (size_t i = 0; i < bf.num_bits_; ++i) {
    size_t byte_index = i / 8;
    uint8_t bit_mask = static_cast<uint8_t>(1u << (i % 8));
    uint8_t byte_val = data[offset + byte_index];
    bf.bits_[i] = (byte_val & bit_mask) != 0;
  }

  return bf;
}
} // namespace tiny_lsm
