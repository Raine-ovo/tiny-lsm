#include "../include/logger/logger.h"
#include "../include/wal/record.h"
#include "../include/wal/wal.h"
#include <filesystem>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace testing;
using namespace ::tiny_lsm;

class MockWAL : public WAL {
public:
  MockWAL(const std::string &log_path, size_t buffer_size,
          uint64_t max_finished_tranc_id)
      : WAL(log_path, buffer_size, max_finished_tranc_id, 1, 100) {}

  MOCK_METHOD(void, cleaner, ());
  MOCK_METHOD(void, cleanWALFile, ());
  MOCK_METHOD(void, reset_file, ());

  FileObj *get_log_file() { return &log_file_; }

  std::vector<Record> &get_log_buffer() { return log_buffer_; }

  size_t get_file_size_limit() { return file_size_limit_; }

  void set_max_finished_tranc_id(uint64_t max_finished_tranc_id) {
    check_point_tranc_id_ = max_finished_tranc_id;
  }

  uint64_t get_max_finished_tranc_id() { return check_point_tranc_id_; }
};

class WALTest : public ::testing::Test {
protected:
  void SetUp() override {
    // 创建测试目录
    if (!std::filesystem::exists(test_dir)) {
      std::filesystem::create_directory(test_dir);
    } else {
      // 清空测试目录
      for (const auto &entry : std::filesystem::directory_iterator(test_dir)) {
        std::filesystem::remove_all(entry.path());
      }
    }
  }

  void TearDown() override {
    // 清理测试文件
    std::filesystem::remove_all(test_dir);
  }

  std::string test_dir = "test_wal_dir";
};

TEST_F(WALTest, LogAndFlush) {
  MockWAL wal(test_dir, 10, 100);

  auto mock_file = wal.get_log_file();

  std::vector<Record> records1;
  std::vector<Record> records2;
  int record1_len = 8;
  int record2_len = 12;

  uint64_t tranc_id = 0;
  for (int i = 0; i < record1_len; i++) {
    auto tranc_str = std::to_string(tranc_id);
    auto key = tranc_str + "-key-" + std::to_string(i);
    if (i == 0) {
      auto rec = Record::createRecord(tranc_id);
      records1.push_back(rec);
    } else if (i == record1_len - 1) {
      auto rec = Record::commitRecord(tranc_id++);
      records1.push_back(rec);
    } else {
      auto value = tranc_str + "-value-" + std::to_string(i);
      auto rec = Record::putRecord(tranc_id, key, value);
      records1.push_back(rec);
    }
  }

  for (int i = 0; i < record2_len; i++) {
    auto tranc_str = std::to_string(tranc_id);
    auto key = tranc_str + "-key-" + std::to_string(i);
    if (i == 0) {
      auto rec = Record::createRecord(tranc_id);
      records2.push_back(rec);
    } else if (i == record2_len - 1) {
      auto rec = Record::commitRecord(tranc_id++);
      records2.push_back(rec);
    } else {
      auto value = tranc_str + "-value-" + std::to_string(i);
      auto rec = Record::putRecord(tranc_id, key, value);
      records2.push_back(rec);
    }
  }

  // ? 下面的测试代码因个人实现不同, 你可自行修改

  // wal.log(records1, false);
  // EXPECT_EQ(wal.get_log_buffer().size(), 8);

  // wal.log(records2, false);
  // EXPECT_EQ(wal.get_log_buffer().size(), 0);

  // wal.flush();
  // EXPECT_EQ(wal.get_log_buffer().size(), 0);
}

TEST_F(WALTest, RecoverTest) {
  std::map<uint64_t, std::vector<Record>> expected;

  {
    MockWAL wal(test_dir, 10, 100);

    std::vector<Record> records;
    int record1_len = 50;

    uint64_t tranc_id = 0;
    for (int i = 0; i < record1_len; i++) {
      auto tranc_str = std::to_string(tranc_id);
      auto key = tranc_str + "-key-" + std::to_string(i);
      if (i == 0) {
        auto rec = Record::createRecord(tranc_id);
        records.push_back(rec);
        expected[tranc_id].push_back(rec);
      } else if (i % 10 == 0 || i == record1_len - 1) {
        // 每10条记录或者最后一条记录提交一次
        auto rec = Record::commitRecord(tranc_id);
        records.push_back(rec);
        expected[tranc_id].push_back(rec);
        tranc_id++;
      } else {
        auto value = tranc_str + "-value-" + std::to_string(i);
        auto rec = Record::putRecord(tranc_id, key, value);
        records.push_back(rec);
        expected[tranc_id].push_back(rec);
      }
      if (i % 20 == 0) {
        // 每20条记录写入一次
        wal.log(records);
        records.clear();
      }
    }
    if (!records.empty()) {
      wal.log(records);
    }
  }

  // 恢复
  auto tranc_records = WAL::recover(test_dir, 100);
  // 校验 Record
  uint64_t tranc_id = 0;
  for (auto &[tranc_id, records] : tranc_records) {
    EXPECT_EQ(records, expected[tranc_id]);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  init_spdlog_file();
  return RUN_ALL_TESTS();
}