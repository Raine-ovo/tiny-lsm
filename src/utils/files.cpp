#include "../../include/utils/files.h"
#include <cstring>
#include <stdexcept>
#include "../../include/utils/debug.h"

namespace tiny_lsm {
FileObj::FileObj() : m_file(std::make_unique<StdFile>()) {}

FileObj::~FileObj() = default;

// 实现移动语义
FileObj::FileObj(FileObj &&other) noexcept
    : m_file(std::move(other.m_file)), m_size(other.m_size) {
  other.m_size = 0;
}

FileObj &FileObj::operator=(FileObj &&other) noexcept {
  if (this != &other) {
    m_file = std::move(other.m_file);
    m_size = other.m_size;
    other.m_size = 0;
  }
  return *this;
}

size_t FileObj::size() const { return m_file->size(); }

void FileObj::set_size(size_t size) { m_size = size; }

void FileObj::del_file() { m_file->remove(); }
FileObj FileObj::create_and_write(const std::string &path,
                                  std::vector<uint8_t> buf) {
  FileObj file_obj;
  if (!file_obj.m_file->create(path, buf)) {
    throw std::runtime_error("Failed to create or write file: " + path);
  }

  // 同步到磁盘
  file_obj.m_file->sync();

  return std::move(file_obj);
}

FileObj FileObj::open(const std::string &path, bool create) {
  FileObj file_obj;

  // 打开文件
  if (!file_obj.m_file->open(path, create)) {
    throw std::runtime_error("Failed to open file: " + path);
  }

  return std::move(file_obj);
}

std::vector<uint8_t> FileObj::read_to_slice(size_t offset, size_t length) {
  // 检查边界
  // debug("file read start");
  if (offset + length > m_file->size()) {
    debug("file read failed: read beyond file size");
    throw std::out_of_range("Read beyond file size");
  }

  // 从w文件复制数据
  auto result = m_file->read(offset, length);

  // debug("file read end");
  return result;
}

uint8_t FileObj::read_uint8(size_t offset) {
  // 检查边界
  if (offset + sizeof(uint8_t) > m_file->size()) {
    throw std::out_of_range("Read beyond file size");
  }

  // 从w文件复制数据
  auto result = m_file->read(offset, sizeof(uint8_t));

  // 将数据转换为uint8_t
  return result[0];
}

uint16_t FileObj::read_uint16(size_t offset) {
  // 检查边界
  if (offset + sizeof(uint16_t) > m_file->size()) {
    throw std::out_of_range("Read beyond file size");
  }
  auto result = m_file->read(offset, sizeof(uint16_t));
  return *(uint16_t *)result.data();
}

uint32_t FileObj::read_uint32(size_t offset) {
  // 检查边界
  if (offset + sizeof(uint32_t) > m_file->size()) {
    throw std::out_of_range("Read beyond file size");
  }

  // 从w文件复制数据
  auto result = m_file->read(offset, sizeof(uint32_t));

  // 将数据转换为uint32_t
  return *(uint32_t *)result.data();
}

uint64_t FileObj::read_uint64(size_t offset) {
  // 检查边界
  if (offset + sizeof(uint64_t) > m_file->size()) {
    throw std::out_of_range("Read beyond file size");
  }

  // 从w文件复制数据
  auto result = m_file->read(offset, sizeof(uint64_t));

  // 将数据转换为uint64_t
  return *(uint64_t *)result.data();
}

// 写入到文件
bool FileObj::write(size_t offset, std::vector<uint8_t> &buf) {
  return m_file->write(offset, buf.data(), buf.size());
}

// 追加到文件
bool FileObj::append(std::vector<uint8_t> &buf) {
  // 获取文件大小
  size_t file_size = m_file->size();

  // 写入数据
  if (!m_file->write(file_size, buf.data(), buf.size())) {
    return false;
  }

  // 更新文件大小
  m_size += buf.size();

  return true;
}

bool FileObj::sync() { return m_file->sync(); }
} // namespace tiny_lsm