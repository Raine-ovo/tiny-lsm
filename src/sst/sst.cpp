#include "../../include/sst/sst.h"
#include "../../include/config/config.h"
#include "../../include/consts.h"
#include "../../include/sst/sst_iterator.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>
#include "../../include/utils/debug.h"
#include "../../include/lsm/engine.h"

namespace tiny_lsm {

// **************************************************
// SST
// **************************************************

std::shared_ptr<SST> SST::open(size_t sst_id, FileObj file,
                               std::shared_ptr<BlockCache> block_cache) {

  // debug("open start");
  std::shared_ptr<SST> sst = std::make_shared<SST>();
  sst->file = std::move(file);

  // debug("sst_file_size, extra info size: ", sst->file.size(), 2*sizeof(uint32_t)+2*sizeof(uint64_t));
  std::vector<uint8_t> file_content = sst->file.read_to_slice(0, sst->file.size());
  // debug("read_to_slice end");

  size_t meta_data_offset = 2 * sizeof(uint32_t) + 2 * sizeof(uint64_t);
  size_t file_offset = sst->file.size() - meta_data_offset;
  // 读取 extra information: meta section offset, bloom section offset, min tranc id, max tranc id
  memcpy(&(sst->meta_block_offset), file_content.data() + file_offset, sizeof(uint32_t));
  file_offset += sizeof(uint32_t);
  memcpy(&(sst->bloom_offset), file_content.data() + file_offset, sizeof(uint32_t));
  file_offset += sizeof(uint32_t);
  memcpy(&(sst->min_tranc_id_), file_content.data() + file_offset, sizeof(uint64_t));
  file_offset += sizeof(uint64_t);
  memcpy(&(sst->max_tranc_id_), file_content.data() + file_offset, sizeof(uint64_t));

  // 读取 meta entries
  std::vector<uint8_t> metadata;
  uint32_t metadata_len = sst->bloom_offset - sst->meta_block_offset;
  metadata.resize(metadata_len);
  memcpy(metadata.data(), file_content.data() + sst->meta_block_offset, metadata_len);
  sst->meta_entries = BlockMeta::decode_meta_from_slice(metadata);

  // 读取 bloom filter
  std::vector<uint8_t> bloom_filter_data;
  uint32_t bloom_filter_len = sst->file.size() - meta_data_offset - sst->bloom_offset;
  bloom_filter_data.resize(bloom_filter_len);
  memcpy(bloom_filter_data.data(), file_content.data() + sst->bloom_offset, bloom_filter_len);
  sst->bloom_filter = std::make_shared<BloomFilter>(BloomFilter::decode(bloom_filter_data));

  // sst id
  sst->sst_id = sst_id;

  // 找出 first key 和 last key
  sst->first_key = sst->meta_entries[0].first_key;
  sst->last_key = sst->meta_entries.back().last_key;

  // block cache
  sst->block_cache = block_cache;
  // debug("open end");

  return sst;
}

void SST::del_sst() { file.del_file(); }

std::shared_ptr<SST> SST::create_sst_with_meta_only(
    size_t sst_id, size_t file_size, const std::string &first_key,
    const std::string &last_key, std::shared_ptr<BlockCache> block_cache) {
  auto sst = std::make_shared<SST>();
  sst->file.set_size(file_size);
  sst->sst_id = sst_id;
  sst->first_key = first_key;
  sst->last_key = last_key;
  sst->meta_block_offset = 0;
  sst->block_cache = block_cache;

  return sst;
}

std::shared_ptr<Block> SST::read_block(size_t block_idx) {
  // 判断缓存是否命中
  std::shared_ptr<Block> block_ptr = nullptr;
  if (block_cache) block_ptr = block_cache->get(sst_id, block_idx);
  // else throw std::runtime_error("not found block_cache!");
  if (block_ptr) return block_ptr;
  size_t block_offset_begin = meta_entries[block_idx].offset;
  size_t block_offset_end = (block_idx == meta_entries.size() - 1 ?
                              meta_block_offset : meta_entries[block_idx+1].offset);
  // debug(block_offset_begin, block_offset_end);
  size_t block_len = block_offset_end - block_offset_begin;
  std::vector<uint8_t> block_data = file.read_to_slice(block_offset_begin, block_len);
  block_ptr = Block::decode(block_data);
  if (block_cache) block_cache->put(sst_id, block_idx, block_ptr);
  return block_ptr;
}

std::optional<uint32_t> SST::find_block_idx(const std::string &key) {
  // ? 给定一个 `key`, 返回其所属的 `block` 的索引
  // ? 如果没有找到包含该 `key` 的 Block，返回-1

  // 布隆过滤器的作用就是进一步减少无效 block 的读取
  // 如果查询时本身这个 key 就不存在，我就不需要进行 block 查找与读取
  // 先在布隆过滤器判断key是否存在
  if (bloom_filter && !bloom_filter->possibly_contains(key)) {
    return std::nullopt;
  }

  // SST 文件由跳表转化而来，跳表内部全局有序，保证了切割后的多个 block 满足严格有序，即有序且不重叠
  // 因此可以使用 二分 来查找对应的 key

  if (meta_entries.empty()) return std::nullopt;

  uint32_t l = 0, r = meta_entries.size() - 1;
  while (l <= r) {
    int mid = l + (r - l) / 2;
    std::string first_key = meta_entries[mid].first_key;
    std::string last_key = meta_entries[mid].last_key;
    if (key < first_key) {
      r = mid - 1;
    } else if (key > last_key) {
      l = mid + 1;
    } else {
      if (read_block(mid)->get_idx_binary(key, max_tranc_id_) != std::nullopt) {
        // debug("find key", key, "in", mid);
        return mid;
      } else {
        // debug("not find key", key, "in", mid);
        return std::nullopt;
      }
    }
  }
  return std::nullopt;
}

SstIterator SST::get(const std::string &key, uint64_t tranc_id) {
  // ? 如果`key`不存在, 返回一个无效的迭代器即可
  // throw std::runtime_error("Not implemented");

  return SstIterator(shared_from_this(), key, tranc_id);
}

size_t SST::num_blocks() const { return meta_entries.size(); }

std::string SST::get_first_key() const { return first_key; }

std::string SST::get_last_key() const { return last_key; }

size_t SST::sst_size() const { return file.size(); }

size_t SST::get_sst_id() const { return sst_id; }

SstIterator SST::begin(uint64_t tranc_id) {
  // throw std::runtime_error("Not implemented");
  return SstIterator(shared_from_this(), tranc_id);
}

SstIterator SST::end() {
  // throw std::runtime_error("Not implemented");
  SstIterator it{};
  it.m_sst = shared_from_this();
  it.m_block_idx = num_blocks();
  return it;
}

std::pair<uint64_t, uint64_t> SST::get_tranc_id_range() const {
  return std::make_pair(min_tranc_id_, max_tranc_id_);
}

// **************************************************
// SSTBuilder
// **************************************************

SSTBuilder::SSTBuilder(size_t block_size, bool has_bloom, size_t target_level) : block(block_size), block_size(block_size) {
  // 初始化第一个block
  if (has_bloom) {
    // 估计每条记录平均大小（可根据实际情况调整或改为配置项）
    const size_t avg_entry_bytes = 64; // 粗略估计：key+value+元数据
    // 目标 sst 大小：沿用 level 估算
    size_t target_sst_bytes = (target_level == 0)
        ? static_cast<size_t>(TomlConfig::getInstance().getLsmPerMemSizeLimit())
        : static_cast<size_t>(TomlConfig::getInstance().getLsmPerMemSizeLimit() *
                              std::pow(TomlConfig::getInstance().getLsmSstLevelRatio(), target_level));
    size_t expected_elements = std::max<size_t>(1, target_sst_bytes / avg_entry_bytes);
    bloom_filter = std::make_shared<BloomFilter>(
        expected_elements,
        // TomlConfig::getInstance().getBloomFilterExpectedSize(),
        TomlConfig::getInstance().getBloomFilterExpectedErrorRate());
  }
  meta_entries.clear();
  data.clear();
  first_key.clear();
  last_key.clear();
}

// 上层 compact 或 skiplist flush 时，往 sstbuilder 中加数据用到迭代器，而我们的迭代器的
// key 都是从小到大的
void SSTBuilder::add(const std::string &key, const std::string &value,
                     uint64_t tranc_id) {

  if (first_key.empty()) first_key = key;
  if (bloom_filter) bloom_filter->add(key);

  // 维护事务范围
  min_tranc_id_ = std::min(min_tranc_id_, tranc_id);
  max_tranc_id_ = std::max(max_tranc_id_, tranc_id);

  bool force_write = (last_key == key);

  // debug("SSTBuilder add: key=", key, "value=", value);

  // 如果 block 中含有这个 key ，则一定要加入到这个 block 中，防止 key 重叠
  // 注意：在没有开始事务时，key 的升序且不重复的，但是加了事务会重复
  // 为了让各个 block 之间没有重叠，我们需要让同 key 在同 block

  if (block.add_entry(key, value, tranc_id, force_write)) {
    last_key = key; // 更新 last key
  } else {
    finish_block();
    block.add_entry(key, value, tranc_id, false);
    // 新的 block 后一定要维护 first_key 和 last_key
    first_key = key;
    last_key = key;
  }
}

size_t SSTBuilder::estimated_size() const { return data.size(); }

void SSTBuilder::finish_block() {
  // ? 当 add 函数发现当前的`block`容量超出阈值时，需要将其编码到`data`，并清空`block`

  size_t data_bytes = estimated_size();
  // 更新 meta entries
  meta_entries.push_back(BlockMeta{static_cast<uint32_t>(data_bytes), first_key, last_key});

  // 把现有的 block 编码到 data 中
  std::vector<uint8_t> block_data = block.encode();
  data.resize(data_bytes + block_data.size());
  memcpy(data.data() + data_bytes, block_data.data(), block_data.size());

  // 重新创建新的 block
  // debug("!!! SSTBuilder 的 blocksize=", block_size);
  block = Block{block_size};
  first_key.clear();
  last_key.clear();
}

std::shared_ptr<SST>
SSTBuilder::build(size_t sst_id, const std::string &path,
                  std::shared_ptr<BlockCache> block_cache) {

  // 在 build 之前，可能当前的 block 还有数据，此时需要 finish
  if (block.size()) finish_block();

  // Builder 必须有数据才能 build
  if (meta_entries.size() == 0) {
    throw std::runtime_error("SSTBuilder build failed: no block data");
  }

  // 文件内容
  // 一个 SST 包含一个 Block Section, Meta Section 和 Bloom Section, Extra Information
  // Block Section 包含若干 Block 以及对应的 Hash value
  // Meta Section 包含 Num, 若干 Block Meta 以及最后的 Hash value
  // Bloom Section 现在不管
  // Extra 包含 MetaSection Offset, Bllom Section Offset, Min Tranc_ID, MAX Tranc_ID
  std::vector<uint8_t> file_content;
  size_t file_offset = 0;

  // Block Section 即为 SSTBuilder 的 data 数组
  uint32_t data_bytes = estimated_size();
  file_content.resize(data_bytes);
  memcpy(file_content.data() + file_offset, data.data(), data_bytes);
  file_offset += data_bytes;

  // Meta Section 可以调用 Block Meta 的 encode_meta_to_slice 函数
  std::vector<uint8_t> metadata;
  BlockMeta::encode_meta_to_slice(meta_entries, metadata);
  file_content.resize(file_content.size() + metadata.size());
  memcpy(file_content.data() + file_offset, metadata.data(), metadata.size());
  file_offset += metadata.size();

  // Bloom Section 使用 encode 编码
  // 在构建 SST 时需要构建布隆过滤器
  // bloom_filter = BloomFilter()
  std::vector<uint8_t> bloom_data = bloom_filter->encode();
  file_content.resize(file_content.size() + bloom_data.size());
  memcpy(file_content.data() + file_offset, bloom_data.data(), bloom_data.size());
  file_offset += bloom_data.size();

  // Extra Information
  // 1. Meta Section Offset
  size_t extra_bytes = sizeof(uint32_t) * 2 + sizeof(uint64_t) * 2;
  file_content.resize(file_content.size() + extra_bytes);
  memcpy(file_content.data() + file_offset, &data_bytes, sizeof(uint32_t));
  file_offset += sizeof(uint32_t);
  // 2. Bloom Section Offset
  uint32_t bloom_section_offset = data_bytes + metadata.size();
  memcpy(file_content.data() + file_offset, &bloom_section_offset, sizeof(uint32_t));
  file_offset += sizeof(uint32_t);
  // 3. MIN Tranc_ID
  memcpy(file_content.data() + file_offset, &min_tranc_id_, sizeof(uint64_t));
  file_offset += sizeof(uint64_t);
  // 4. MAX Tranc_ID
  memcpy(file_content.data() + file_offset, &max_tranc_id_, sizeof(uint64_t));
  file_offset += sizeof(uint64_t);

  // 构建 SST 控制体
  std::shared_ptr<SST> sst = std::make_shared<SST>();
  sst->file = FileObj::create_and_write(path, file_content);
  sst->meta_entries = meta_entries;
  sst->bloom_offset = bloom_section_offset;
  sst->meta_block_offset = data_bytes;
  sst->sst_id = sst_id;
  sst->first_key = meta_entries[0].first_key;
  sst->last_key = meta_entries.back().last_key;
  sst->bloom_filter = bloom_filter;
  sst->block_cache = block_cache;
  sst->min_tranc_id_ = min_tranc_id_;
  sst->max_tranc_id_ = max_tranc_id_;

  return sst;
}
} // namespace tiny_lsm
