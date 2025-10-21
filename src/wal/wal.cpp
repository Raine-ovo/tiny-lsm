// src/wal/wal.cpp

#include "../../include/wal/wal.h"
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>
#include <chrono>
#include "spdlog/spdlog.h"

namespace tiny_lsm {

// 从零开始的初始化流程
WAL::WAL(const std::string &log_dir, size_t buffer_size,
         uint64_t check_point_tranc_id, uint64_t clean_interval,
         uint64_t file_size_limit)
: buffer_size_(buffer_size), check_point_tranc_id_(check_point_tranc_id),
clean_interval_(clean_interval), file_size_limit_(file_size_limit),
stop_cleaner_(false), log_dir_(log_dir)
{

  if (!std::filesystem::exists(log_dir_)) {
    std::filesystem::create_directories(log_dir_);
    active_file_seq = 0;
    // 创建一个新的 WAL 文件作为活跃 WAL
    log_file_ = FileObj::create_and_write(get_active_log_filename(), {});
  } else {
    active_file_seq = 0;
    bool found = false;
    try {
      for (const auto &entries: std::filesystem::directory_iterator(log_dir_)) {
        if (!entries.is_regular_file()) continue;
        std::string filename = entries.path().filename().string();
        if (filename.rfind("wal.", 0) != 0) continue;
        active_file_seq = std::max(active_file_seq,
            static_cast<uint64_t>(std::stoull(filename.substr(filename.find_first_of('.') + 1)))
          );
        found = true;
      }
    } catch (const std::filesystem::filesystem_error&) {
      // 如果遍历目录失败，退化为创建新 WAL 文件
    }
    if (!found) {
      // 目录存在但没有 wal 文件，创建 wal.0
      log_file_ = FileObj::open(get_active_log_filename(), true);
    } else {
      log_file_ = FileObj::open(get_active_log_filename(), false);
      if (log_file_.size() > file_size_limit_) {
        ++active_file_seq;
        log_file_ = FileObj::open(get_active_log_filename(), true);
      }
    }
  }


  // 启动清理线程
  cleaner_thread_ = std::thread(&WAL::cleaner, this);
}

WAL::~WAL() {
  log({}, true); // 把缓冲区内容刷入磁盘
  stop_cleaner_.store(true);
  if (cleaner_thread_.joinable()) cleaner_thread_.join();
  log_file_.sync(); // 把数据同步到磁盘
}

std::string WAL::get_active_log_filename() {
  return log_dir_ + "/wal." + std::to_string(active_file_seq);
}

std::map<uint64_t, std::vector<Record>>
WAL::recover(const std::string &log_dir, uint64_t max_flushed_tranc_id) {
  // 检查需要重放的WAL日志
  // recover 是调用 WAL 的方法，从 log dir 中检测出所有未进行持久化的操作并返回重新执行
  std::map<uint64_t, std::vector<Record>> tranc_recoveies{};

  if (!std::filesystem::exists(log_dir) || !std::filesystem::is_directory(log_dir)) {
    spdlog::info("WAL::recover: log_dir '{}' not found or not a dir, skip.", log_dir);
    return tranc_recoveies;
  }

  size_t total_records = 0;
  // 读取这个目录下所有 wal.<seq> 文件
  std::vector<std::pair<uint64_t, std::string>> wal_files;
  try {
    for (const auto &entry: std::filesystem::directory_iterator(log_dir)) {
      if (!entry.is_regular_file()) continue;
      const std::string filename = entry.path().filename().string();
      if (filename.rfind("wal.", 0) != 0) continue;
      const auto dot = filename.find_last_of('.');
      if (dot == std::string::npos) continue;
      uint64_t seq = std::stoull(filename.substr(dot + 1));
      wal_files.emplace_back(seq, entry.path().string());
    }
  } catch (const std::filesystem::filesystem_error&) {
    return tranc_recoveies;
  }

  // 按 seq 升序排序
  std::sort(wal_files.begin(), wal_files.end(), [](const auto &a, const auto &b) {
    return a.first < b.first;
  });

  for (const auto &p : wal_files) {
    const std::string &path = p.second;
    FileObj wal_file = FileObj::open(path, false);
    const size_t file_size = wal_file.size();
    std::vector<Record> records = Record::decode(wal_file.read_to_slice(0, file_size));
    spdlog::info("WAL::recover: read '{}' size={} bytes, decoded {} records", path, file_size, records.size());
    for (const auto& record: records) {
      if (record.getTrancId() > max_flushed_tranc_id) {
        tranc_recoveies[record.getTrancId()].push_back(record);
        ++ total_records;
      }
    }
  }

  spdlog::info("WAL::recover: max_flushed_tranc_id={}, recovered {} transactions, {} records",
               max_flushed_tranc_id, tranc_recoveies.size(), total_records);

  return tranc_recoveies;
}

void WAL::log(const std::vector<Record> &records, bool force_flush) {
  // 实现WAL的写入流程
  std::unique_lock<std::mutex> wlock(mutex_);

  // 把 records 写入缓冲
  log_buffer_.insert(log_buffer_.end(), records.begin(), records.end());

  if (log_buffer_.size() < buffer_size_ && !force_flush) {
    // 缓存足够
    return;
  }

  auto pre_buffer = std::move(log_buffer_);
  log_buffer_.clear();

  // 否则写入 WAL
  for (const Record& record: pre_buffer) {
    auto encoded_record = record.encode();
    log_file_.append(encoded_record);
    if (log_file_.size() > file_size_limit_) {
      // 文件超出限制，刷入磁盘并创建新的文件
      flush();
    }
  }
  if (force_flush) {
    log_file_.sync(); // 从系统缓存刷到磁盘
  }
}

// commit 时 强制写入
// 把活跃 WAL 文件刷入磁盘
void WAL::flush() {
  // 强制刷盘

  // 把 活跃WAL 刷盘，并创建新的 活跃WAL
  log_file_.sync(); // 刷入磁盘
  ++ active_file_seq;
  log_file_ = FileObj::open(get_active_log_filename(), true);
}

void WAL::update_max_flushed_tranc_id(uint64_t tranc_id) {
  check_point_tranc_id_ = tranc_id;
}

void WAL::cleaner() {

  // 实现清理线程：每个 interval 时间就检查 WAL 目录，清除所有旧 WAL 文件
  while (true) {
    // 每次间隔 interval 秒执行
    std::this_thread::sleep_for(std::chrono::seconds(clean_interval_));
    if (stop_cleaner_.load()) break; // 如果退出标志为 true 则退出

    // 1) 锁内获取快照, 并尽快释放锁
    uint64_t checkpoint_snapshot;
    uint64_t active_seq_snapshot;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      checkpoint_snapshot = check_point_tranc_id_;
      active_seq_snapshot  = active_file_seq;
    }
    const std::string active_path_snapshot = log_dir_ + "/wal." + std::to_string(active_seq_snapshot);

    // 2) 锁外遍历目录
    std::vector<std::pair<uint64_t, std::string>> wal_paths;
    try {
      for (const auto &entry: std::filesystem::directory_iterator(log_dir_)) {
        if (!entry.is_regular_file()) continue;
        const auto filename = entry.path().filename().string();
        if (filename.rfind("wal.", 0) != 0) continue;
        const auto dot = filename.find_last_of('.');
        uint64_t seq = std::stoull(filename.substr(dot + 1));
        wal_paths.emplace_back(seq, entry.path().string());
      }
    } catch (const std::filesystem::filesystem_error&) {
      continue;
    }

    std::sort(wal_paths.begin(), wal_paths.end(), [](const auto &a, const auto &b) {
      return a.first < b.first;
    });

    // 3) 锁外读取/解码并判断可删除
    for (size_t i = 0; i + 1 < wal_paths.size(); ++ i) {
      const std::string &path = wal_paths[i].second;
      if (path == active_path_snapshot) continue; // 不删除当前活跃文件

      auto file = FileObj::open(path, false);
      auto bytes = file.read_to_slice(0, file.size());
      auto records = Record::decode(bytes);

      bool can_delete = true;
      for (const auto& rec: records) {
        if (rec.getTrancId() > checkpoint_snapshot) {
          can_delete = false;
          break;
        }
      }

      if (!can_delete) continue;

      // 4) 删除前二次确认仍然不是活跃文件
      bool still_not_active = true;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        still_not_active = (path != (log_dir_ + "/wal." + std::to_string(active_file_seq)));
      }
      if (still_not_active) file.del_file();
    }
  }
}

} // namespace tiny_lsm
