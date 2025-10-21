#pragma once

#include "../utils/files.h"
#include "../wal/wal.h"
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <set>

namespace tiny_lsm {

// 隔离级别: 读未提交、读已提交、重复读、序列化
enum class IsolationLevel {
  READ_UNCOMMITTED,
  READ_COMMITTED,
  REPEATABLE_READ,
  SERIALIZABLE
};

enum class TransactionState {
  COMMITTED,
  ABORTED,
};

inline std::string isolation_level_to_string(const IsolationLevel &level);

class LSMEngine;
class TranManager;

class TranContext {
  friend class TranManager;

public:
  TranContext(uint64_t tranc_id, std::shared_ptr<LSMEngine> engine,
              std::shared_ptr<TranManager> tranManager,
              const enum IsolationLevel &isolation_level);
  void put(const std::string &key, const std::string &value);
  void remove(const std::string &key);
  std::optional<std::string> get(const std::string &key);

  // ! test_fail = true 是测试中手动触发的崩溃
  bool commit(bool test_fail = false);
  bool abort();
  enum IsolationLevel get_isolation_level();

public:
  // 事务句柄所属 kv 引擎和事务管理器
  std::shared_ptr<LSMEngine> engine_;
  std::shared_ptr<TranManager> tranManager_;
  uint64_t tranc_id_; // 这次事务的 id
  // 记录这次事务的写操作，后续通过转化为 WAL 日志实现崩溃恢复功能
  std::vector<Record> operations; 
  // 写集，记录本事务对数据的修改，实现事务内可见，不记录普通 get
  std::unordered_map<std::string, std::string> temp_map_;
  bool isCommited = false;
  bool isAborted = false;
  enum IsolationLevel isolation_level_;

private:
  /*
    这里有一个重点：temp_map 和 read_map 都是记录此次事务的 get 操作，那为什么要分两个数据结构来存储?
    或者说可不可以替代？
    这是不可以的，区分两个 map 是需要进行冲突检测
    事务在提交前，必须进行冲突检测，检测需要修改的 key ，在事务开启和提交时是不是没有被其他事务先修改过，如果 key 已经
    被其他事务修改了，那么这个事务就必须回滚，避免覆盖其他事务的修改
  */
  // 读集，保存本事务对外部存储读取过的值（未被本事务覆盖的），用于提交时作冲突检测
  // read_map: 读-写冲突检测：我曾读过的键有没有被别人改
  std::unordered_map<std::string,
                     std::optional<std::pair<std::string, uint64_t>>>
      read_map_;
  // 写-写冲突检测：我 commit 时准备写入的键，在我第一次写它时是什么版本
  std::unordered_map<std::string,
                     std::optional<std::pair<std::string, uint64_t>>>
      rollback_map_;
};

class TranManager : public std::enable_shared_from_this<TranManager> {
public:
  TranManager(std::string data_dir);
  ~TranManager();
  void init_new_wal();
  void set_engine(std::shared_ptr<LSMEngine> engine);
  std::shared_ptr<TranContext> new_tranc(const IsolationLevel &isolation_level);

  uint64_t getNextTransactionId();
  uint64_t get_max_finished_tranc_id();
  void update_max_finished_tranc_id(uint64_t tranc_id);

  uint64_t get_max_flushed_tranc_id();
  uint64_t get_checkpoint_tranc_id();

  void add_flushed_tranc_id(uint64_t tranc_id);
  void add_ready_to_flush_tranc_id(uint64_t tranc_id, TransactionState state);

  std::set<uint64_t>& get_flushed_tranc_ids();

  bool write_to_wal(const std::vector<Record> &records);

  std::map<uint64_t, std::vector<Record>> check_recover();

  std::string get_tranc_id_file_path();
  std::string get_wal_dir();
  void write_tranc_id_file();
  void read_tranc_id_file();
  // void flusher();

private:
  mutable std::mutex mutex_;
  std::shared_ptr<LSMEngine> engine_;
  std::shared_ptr<WAL> wal;
  std::string data_dir_;
  // std::atomic<bool> flush_thread_running_ = true;
  // 下一个分配事务的 id ，因为不启用事务是用 tranc_id = 0 表示，因此这里从 1 开始
  std::atomic<uint64_t> nextTransactionId_ = 1;
  // 某个事务对应的句柄
  std::map<uint64_t, std::shared_ptr<TranContext>> activeTrans_;
  FileObj tranc_id_file_; // 对于事务管理器，需要持久化其所分配的 tranc id

  std::atomic<uint64_t> max_finished_tranc_id;
  std::map<uint64_t, TransactionState> readyToFlushTrancIds_;
  std::set<uint64_t> flushedTrancIds_;
};

} // namespace tiny_lsm