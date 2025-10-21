#include "../../include/block/blockmeta.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <string_view>
#include <sys/types.h>
#include <vector>

namespace tiny_lsm {
BlockMeta::BlockMeta() : offset(0), first_key(""), last_key("") {}

BlockMeta::BlockMeta(uint32_t offset, const std::string &first_key,
                     const std::string &last_key)
    : offset(offset), first_key(first_key), last_key(last_key) {}

void BlockMeta::encode_meta_to_slice(std::vector<BlockMeta> &meta_entries,
                                     std::vector<uint8_t> &metadata) {
  // 将内存中所有`Block`的元数据编码为二进制字节数组

  size_t metadata_bytes = 0;
  // meta data 先存储 meta entry num
  metadata.resize(sizeof(uint32_t));
  uint32_t num_entries = meta_entries.size();
  memcpy(metadata.data(), &num_entries, sizeof(uint32_t));
  metadata_bytes += sizeof(uint32_t);

  // 再存储所有 entries
  for (const BlockMeta& meta: meta_entries) {
    size_t first_key_len = meta.first_key.size();
    size_t last_key_len = meta.last_key.size();
    size_t entry_len = sizeof(uint32_t) + sizeof(uint16_t) + first_key_len + sizeof(uint16_t) + last_key_len;

    metadata.resize(metadata_bytes + entry_len);

    // 写入 offset
    memcpy(metadata.data() + metadata_bytes, &meta.offset, sizeof(uint32_t));
    // 写入 first key len 和 first key
    metadata_bytes += sizeof(uint32_t);
    memcpy(metadata.data() + metadata_bytes, &first_key_len, sizeof(uint16_t));
    metadata_bytes += sizeof(uint16_t);
    memcpy(metadata.data() + metadata_bytes, meta.first_key.data(), first_key_len);
    // 写入 last key len 和 last key
    metadata_bytes += first_key_len;
    memcpy(metadata.data() + metadata_bytes, &last_key_len, sizeof(uint16_t));
    metadata_bytes += sizeof(uint16_t);
    memcpy(metadata.data() + metadata_bytes, meta.last_key.data(), last_key_len);
    metadata_bytes += last_key_len;
  }

  // 最后存储 hash 值
  uint32_t hash_value = std::hash<std::string_view>{}(
    std::string_view(reinterpret_cast<const char*>(metadata.data() + sizeof(uint32_t)), metadata_bytes - sizeof(uint32_t)));

  // 把 hash 值编码进去
  metadata.resize(metadata_bytes + sizeof(uint32_t));
  memcpy(metadata.data() + metadata_bytes, &hash_value, sizeof(uint32_t));
}

std::vector<BlockMeta>
BlockMeta::decode_meta_from_slice(const std::vector<uint8_t> &metadata) {
  // 将二进制字节数组解码为内存中的`Block`元数据

  // 对于 metadata ，先验证 hash 再 解码 num 再解码存储所有 entries

  auto check_is_small = [&](const size_t& _size) {
    if (metadata.size() < _size) {
      throw std::runtime_error("decode meta from slice failed: encoded data is small");
    }
  };

  // 至少存在 num_entries 和 hash value
  check_is_small(sizeof(uint32_t) + sizeof(uint32_t));

  // 先解码 hash
  size_t hash_pos = metadata.size() - sizeof(uint32_t);
  uint32_t hash_value;
  memcpy(&hash_value, metadata.data() + hash_pos, sizeof(uint32_t));
  // 验证 hash
  uint32_t compute_hash = std::hash<std::string_view>{}(
    std::string_view(reinterpret_cast<const char*>(metadata.data() + sizeof(uint32_t)), metadata.size() - sizeof(uint32_t) - sizeof(uint32_t)));
  if (hash_value != compute_hash) {
    throw std::runtime_error("decode meta from slice failed: hash verification failed.");
  }

  // 解码 num
  uint32_t num_entries;
  memcpy(&num_entries, metadata.data(), sizeof(uint32_t));

  // 依次解码 block meta
  std::vector<BlockMeta> meta_entries;
  meta_entries.resize(num_entries);
  size_t read_offset = sizeof(uint32_t); // num 解码占用一个 uint32_t
  for (auto &meta: meta_entries) {
    // meta 结构体数据：offset(4B)、first key、last key

    // 解码 offset
    memcpy(&meta.offset, metadata.data() + read_offset, sizeof(uint32_t));
    // 解码 first key len 和 first key
    read_offset += sizeof(uint32_t);
    uint16_t first_key_len;
    memcpy(&first_key_len, metadata.data() + read_offset, sizeof(uint16_t));
    read_offset += sizeof(uint16_t);
    meta.first_key.resize(first_key_len);
    memcpy(meta.first_key.data(), metadata.data() + read_offset, first_key_len);
    // 解码 last key len 和 last key
    read_offset += first_key_len;
    uint16_t last_key_len;
    memcpy(&last_key_len, metadata.data() + read_offset, sizeof(uint16_t));
    read_offset += sizeof(uint16_t);
    meta.last_key.resize(last_key_len);
    memcpy(meta.last_key.data(), metadata.data() + read_offset, last_key_len);
    read_offset += last_key_len;
  }

  return meta_entries;
}
} // namespace tiny_lsm
