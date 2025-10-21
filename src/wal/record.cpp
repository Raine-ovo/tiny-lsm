// src/wal/record.cpp

#include "../../include/wal/record.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <sys/types.h>
#include <vector>

namespace tiny_lsm {

Record Record::createRecord(uint64_t tranc_id) {
  // 实现创建事务的Record
  Record record;
  record.operation_type_ = OperationType::CREATE;
  record.tranc_id_ = tranc_id;
  // create 只需要记录 operation type 和 tranc id
  record.record_len_ = sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint64_t);
  return record;
}

Record Record::commitRecord(uint64_t tranc_id) {
  // 实现提交事务的Record
  Record record;
  record.operation_type_ = OperationType::COMMIT;
  record.tranc_id_ = tranc_id;
  // commit 只需要记录 operation type 和 tranc id
  record.record_len_ = sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint64_t);
  return record;
}

Record Record::rollbackRecord(uint64_t tranc_id) {
  // 实现回滚事务的Record
  Record record;
  record.operation_type_ = OperationType::ROLLBACK;
  record.tranc_id_ = tranc_id;
  // rollback 只需要记录 operation type 和 tranc id
  record.record_len_ = sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint64_t);
  return record;
}

Record Record::putRecord(uint64_t tranc_id, const std::string &key,
                         const std::string &value) {
  // 实现插入键值对的Record
  Record record;
  record.operation_type_ = OperationType::PUT;
  record.tranc_id_ = tranc_id;
  record.key_ = key;
  record.value_ = value;
  // put 需要记录 operation type 和 tranc id 和 key value
  record.record_len_ = sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint64_t)
                      + key.size() + value.size() + 2 * sizeof(uint16_t);
  return record;
}

Record Record::deleteRecord(uint64_t tranc_id, const std::string &key) {
  // 实现删除键值对的Record
  Record record;
  record.operation_type_ = OperationType::DELETE;
  record.tranc_id_ = tranc_id;
  record.key_ = key;
  // remove 需要记录 operation type 和 tranc id 和 key
  record.record_len_ = sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint64_t)
         + key.size() + sizeof(uint16_t);
  return record;
}

std::vector<uint8_t> Record::encode() const {
  // 实现Record的编码函数
  std::vector<uint8_t> encoded(record_len_);
  // encode 顺序：record len、tranc id、operator type、key、value
  size_t offset = 0;
  memcpy(encoded.data() + offset, &record_len_, sizeof(uint16_t));
  offset += sizeof(uint16_t);

  memcpy(encoded.data() + offset, &tranc_id_, sizeof(uint64_t));
  offset += sizeof(uint64_t);

  uint8_t type_byte = static_cast<uint8_t>(operation_type_);
  memcpy(encoded.data() + offset, &type_byte, sizeof(uint8_t));
  offset += sizeof(uint8_t);

  if (operation_type_ == OperationType::PUT || operation_type_ == OperationType::DELETE) {
    size_t key_len = key_.size(); // Key len 用 u16 存
    memcpy(encoded.data() + offset, &key_len, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    memcpy(encoded.data() + offset, key_.data(), key_len);
    offset += key_len;
  }

  if (operation_type_ == OperationType::PUT) {
    size_t value_len = value_.size();
    memcpy(encoded.data() + offset, &value_len, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    memcpy(encoded.data() + offset, value_.data(), value_len);
    offset += value_len;
  }

  return encoded;
}

std::vector<Record> Record::decode(const std::vector<uint8_t> &data) {
  // 实现Record的解码函数
  std::vector<Record> decoded;

  size_t offset = 0;
  while (offset != data.size()) {
    Record record;
    // encode 顺序：record len、tranc id、operator type、key、value
    memcpy(&record.record_len_, data.data() + offset, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    memcpy(&record.tranc_id_, data.data() + offset, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    uint8_t type_byte;
    memcpy(&type_byte, data.data() + offset, sizeof(uint8_t));
    offset += sizeof(uint8_t);
    record.operation_type_ = static_cast<OperationType>(type_byte);

    if (record.operation_type_ == OperationType::PUT || record.operation_type_ == OperationType::DELETE) {
      uint16_t key_len;
      memcpy(&key_len, data.data() + offset, sizeof(uint16_t));
      offset += sizeof(uint16_t);

      // 注意必须要给 key resize，不然对 data() 进行直接复制是 UB 行为
      record.key_.resize(key_len);
      memcpy(record.key_.data(), data.data() + offset, key_len);
      offset += key_len;
    }

    if (record.operation_type_ == OperationType::PUT) {
      uint16_t value_len;
      memcpy(&value_len, data.data() + offset, sizeof(uint16_t));
      offset += sizeof(uint16_t);

      record.value_.resize(value_len);
      memcpy(record.value_.data(), data.data() + offset, value_len);
      offset += value_len;
    }

    decoded.push_back(record);
  }
  return decoded;
}

void Record::print() const {
  std::cout << "Record: tranc_id=" << tranc_id_
            << ", operation_type=" << static_cast<int>(operation_type_)
            << ", key=" << key_ << ", value=" << value_ << std::endl;
}

bool Record::operator==(const Record &other) const {
  if (tranc_id_ != other.tranc_id_ ||
      operation_type_ != other.operation_type_) {
    return false;
  }

  // 不需要 key 和 value 比较的情况
  if (operation_type_ == OperationType::CREATE ||
      operation_type_ == OperationType::COMMIT ||
      operation_type_ == OperationType::ROLLBACK) {
    return true;
  }

  // 需要 key 比较的情况
  if (operation_type_ == OperationType::DELETE) {
    return key_ == other.key_;
  }

  // 需要 key 和 value 比较的情况
  return key_ == other.key_ && value_ == other.value_;
}

bool Record::operator!=(const Record &other) const { return !(*this == other); }
} // namespace tiny_lsm
