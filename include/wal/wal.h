// include/wal/wal.h

#pragma once

#include "../utils/files.h"
#include "record.h"
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace tiny_lsm {

class WAL {
public:
  WAL(const std::string &log_dir, size_t buffer_size,
      uint64_t check_point_tranc_id, uint64_t clean_interval,
      uint64_t file_size_limit);

  // 保证析构时所有的 WAL 内容都被持久化
  ~WAL();

  // 恢复 WAL 文件，返回所有未完成的 WAL 记录
  static std::map<uint64_t, std::vector<Record>>
  recover(const std::string &log_dir, uint64_t check_point_tranc_id);

  // 将记录添加到缓冲区
  void log(const std::vector<Record> &records, bool force_flush = false);

  // 写入 WAL 文件
  void flush();

  void update_max_flushed_tranc_id(uint64_t tranc_id);

  std::string get_active_log_filename();

private:
  void cleaner();

protected:
  // WAL 目录
  std::string log_dir_;
  // 当前写入的 WAL 文件路径（通过序号拼接）
  uint64_t active_file_seq;
  // 当前写入的 WAL 文件对象
  FileObj log_file_;
  // WAL 文件大小限制
  size_t file_size_limit_;
  std::mutex mutex_;
  // WAL 记录的缓冲区
  std::vector<Record> log_buffer_;
  size_t buffer_size_;
  // 清理线程
  std::thread cleaner_thread_;
  // checkpoint 的最大已刷盘事务号（决定清理边界）
  uint64_t check_point_tranc_id_;
  uint64_t clean_interval_;
  // cleaner 线程的退出标志
  std::atomic<bool> stop_cleaner_;
};
} // namespace tiny_lsm