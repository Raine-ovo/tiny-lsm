#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

/**
 * SST文件的结构, 参考自 https://skyzh.github.io/mini-lsm/week1-04-sst.html
 * -------------------------------------------------------------------------------------------
 * |         Block Section         |          Meta Section         | Extra |
 * -------------------------------------------------------------------------------------------
 * | data block | ... | data block |            metadata           | meta block
 offset (32) |
 * -------------------------------------------------------------------------------------------

 * 其中, metadata 是一个数组加上一些描述信息, 数组每个元素由一个 BlockMeta
 编码形成 MetaEntry, MetaEntry 结构如下:
 * --------------------------------------------------------------------------------------------------------------
 * | offset (32) | first_key_len (16) | first_key (first_key_len) |
 last_key_len(16) | last_key (last_key_len) |
 * --------------------------------------------------------------------------------------------------------------

 * Meta Section 的结构如下:
 * --------------------------------------------------------------------------------------------------------------
 * | num_entries (32) | MetaEntry | ... | MetaEntry | Hash (32) |
 * --------------------------------------------------------------------------------------------------------------
 * 其中, num_entries 表示 metadata 数组的长度, Hash 是 metadata
 数组的哈希值(只包括数组部分, 不包括 num_entries ), 用于校验 metadata 的完整性
 */

namespace tiny_lsm {
class BlockMeta {
  friend class BlockMetaTest;

public:
  uint32_t offset;         // offset 表示对应的 block 在 SST 中的偏置，相当于数组中的索引作用，等同于 block 中的 offsets
  /*
    这里设置 first key 和 last key 是 LSM 树等存储引擎常见的优化设计
    主要作用：加速查询和范围扫描
    1. 加速查询
      在 LSM 树的 SST 中，数据按块组织，每个块内部的 key 是有序排列的
      BlockMeta 左右块的元数据，会被集中存储（SST末尾）。
      需要查询某个键或范围键时：
        先遍历所有 BlockMeta ，通过比较查询键和 first key ，last key 来快速排除不可能包含目标键的块。
        能显著减少需要访问的块的数量，尤其是在高层 SST 上的查询
    2. 支持范围查询
      对于范围查询（key > A 且 key < B）
      通过 BlockMeta 的 first key 和 last key，可以快速确定：
      1. 哪些块完全落在查询范围内，无需检查块内细节。
      2. 哪些块部分重叠，进一步检查块内细节。
      3. 那些块完全无关，可以直接跳过。
      先过滤元数据，再处理具体块，可以极大提升范围查询的效率。
    3. 辅助 SST 之间的 merge 操作
      LSM 树的核心特性是 “写入优化”，通过定期合并多个 SST 减少数据冗余，再合并过程中：
      1. 需要按键的顺序重新组织数据，而 BlockMeta 的 first key 和 last key 可以帮助快速判断两个 SST 的块
        是否有重叠，可以优化合并顺序。
    4. 验证块数据的完整性（辅助作用）
      由于块内部键有顺序，first key 和 last key 也可以作为一种轻量校验：
      解析块数据时，可以检查实际的第一个键是否与 first key 一致，最后一个键是否与 last key 一致，快速判断块数据是否被损坏或篡改。
  */
  std::string first_key; // 块的第一个key
  std::string last_key;  // 块的最后一个key
  static void encode_meta_to_slice(std::vector<BlockMeta> &meta_entries,
                                   std::vector<uint8_t> &metadata);
  static std::vector<BlockMeta>
  decode_meta_from_slice(const std::vector<uint8_t> &metadata);
  BlockMeta();
  BlockMeta(uint32_t offset, const std::string &first_key,
            const std::string &last_key);
};
} // namespace tiny_lsm
