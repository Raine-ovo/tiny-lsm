#pragma once

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace tiny_lsm {

// 因为要把这个类编码
// Enum class 对象一般是 4/8 字节，因为我们这定义的类少，所以可以直接用 1 字节来表示对应类
// 那么我们在 encode/decode 时，需要显式把这个变量 static_cast 转化为一个 uint8_t 类型，再进行写入
// 不然会异常
enum class OperationType: uint8_t {
  CREATE,
  COMMIT,
  ROLLBACK,
  PUT,
  DELETE,
};

// Record 是 WAL 中单个日志的条目
class Record {
private:
  // 构造函数
  Record() = default;

public:
  // 操作类型枚举

  static Record createRecord(uint64_t tranc_id);
  static Record commitRecord(uint64_t tranc_id);
  static Record rollbackRecord(uint64_t tranc_id);
  static Record putRecord(uint64_t tranc_id, const std::string &key,
                          const std::string &value);
  static Record deleteRecord(uint64_t tranc_id, const std::string &key);

  // 编码记录
  std::vector<uint8_t> encode() const;

  // 解码记录
  static std::vector<Record> decode(const std::vector<uint8_t> &data);

  // 获取记录的各个部分
  uint64_t getTrancId() const { return tranc_id_; }
  OperationType getOperationType() const { return operation_type_; }
  std::string getKey() const { return key_; }
  std::string getValue() const { return value_; }

  // 打印记录（用于调试）
  void print() const;

  bool operator==(const Record &other) const;
  bool operator!=(const Record &other) const;

private:
  uint64_t tranc_id_;
  OperationType operation_type_;
  std::string key_;
  std::string value_;
  uint16_t record_len_;
};
} // namespace tiny_lsm