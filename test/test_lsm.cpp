#include "../include/logger/logger.h"
#include "../include/lsm/engine.h"
#include "../include/lsm/level_iterator.h"
#include "../include/config/config.h"
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <unordered_map>
#include "../include/utils/debug.h"
#include <chrono>
#include <algorithm>
#include <random>
#include <vector>

using namespace ::tiny_lsm;

class LSMTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create a temporary test directory
    test_dir = "test_lsm_data";
    if (std::filesystem::exists(test_dir)) {
      std::filesystem::remove_all(test_dir);
    }
    std::filesystem::create_directory(test_dir);
  }

  void TearDown() override {
    // Clean up test directory
    if (std::filesystem::exists(test_dir)) {
      std::filesystem::remove_all(test_dir);
    }
  }

  std::string test_dir;
};

// Test basic operations: put, get, remove
TEST_F(LSMTest, BasicOperations) {
  // 打印配置信息
  debug(TomlConfig::getInstance().getLsmTolMemSizeLimit());
  debug(TomlConfig::getInstance().getLsmPerMemSizeLimit());
  debug(TomlConfig::getInstance().getLsmBlockSize());
  LSM lsm(test_dir);

  // Test put and get
  debug("Key key1 start put");
  lsm.put("key1", "value1");
  debug("Key key1 end put");
  EXPECT_EQ(lsm.get("key1").value(), "value1");

  // Test update
  lsm.put("key1", "new_value");
  EXPECT_EQ(lsm.get("key1").value(), "new_value");

  // Test remove
  lsm.remove("key1");
  EXPECT_FALSE(lsm.get("key1").has_value());

  // Test non-existent key
  debug("find key noneistent");
  EXPECT_FALSE(lsm.get("nonexistent").has_value());
  debug("end the test");
}

// Test persistence across restarts
TEST_F(LSMTest, Persistence) {
  // 打印配置信息
  debug(TomlConfig::getInstance().getLsmTolMemSizeLimit());
  debug(TomlConfig::getInstance().getLsmPerMemSizeLimit());
  debug(TomlConfig::getInstance().getLsmBlockSize());
  std::unordered_map<std::string, std::string> kvs;
  int num = 100000;
  {
    LSM lsm(test_dir);
    for (int i = 0; i < num; ++i) {
      std::string key = "key" + std::to_string(i);
      std::string value = "value" + std::to_string(i);
      lsm.put(key, value);
      kvs[key] = value;

      // 删除之前被10整除的key
      if (i % 10 == 0 && i != 0) {
        std::string del_key = "key" + std::to_string(i - 10);
        lsm.remove(del_key);
        kvs.erase(del_key);
      }
    }
  } // LSM destructor called here

  // Create new LSM instance
  LSM lsm(test_dir);
  for (int i = 0; i < num; ++i) {
    std::string key = "key" + std::to_string(i);
    if (kvs.find(key) != kvs.end()) {
      EXPECT_EQ(lsm.get(key, true).value(), kvs[key]);
    } else {
      if (key == "key4410") {
        // debug
        auto res = lsm.get("key4410");
      }
      if (lsm.get(key, true).has_value()) {
        std::cout << "key" << i << " not exist but found" << std::endl;
        exit(-1);
      }
      // EXPECT_FALSE(lsm.get(key).has_value());
    }
  }

  // Query a not exist key
  EXPECT_FALSE(lsm.get("nonexistent").has_value());
}

// Test large scale operations
TEST_F(LSMTest, LargeScaleOperations) {
  // 打印配置信息
  debug(TomlConfig::getInstance().getLsmTolMemSizeLimit());
  debug(TomlConfig::getInstance().getLsmPerMemSizeLimit());
  debug(TomlConfig::getInstance().getLsmBlockSize());

  LSM lsm(test_dir);
  std::vector<std::pair<std::string, std::string>> data;

  // Insert enough data to trigger multiple flushes
  for (int i = 0; i < 1000; i++) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    lsm.put(key, value);
    data.emplace_back(key, value);
  }

  // Verify all data
  for (const auto &[key, value] : data) {
    EXPECT_EQ(lsm.get(key).value(), value);
  }
}

// Test mixed operations
TEST_F(LSMTest, MixedOperations) {
  // 打印配置信息
  debug(TomlConfig::getInstance().getLsmTolMemSizeLimit());
  debug(TomlConfig::getInstance().getLsmPerMemSizeLimit());
  debug(TomlConfig::getInstance().getLsmBlockSize());

  LSM lsm(test_dir);
  std::map<std::string, std::string> reference;

  // Perform mixed operations
  lsm.put("key1", "value1");
  reference["key1"] = "value1";

  lsm.put("key2", "value2");
  reference["key2"] = "value2";

  lsm.remove("key1");
  reference.erase("key1");

  lsm.put("key3", "value3");
  reference["key3"] = "value3";

  // Verify final state
  for (const auto &[key, value] : reference) {
    EXPECT_EQ(lsm.get(key).value(), value);
  }
  EXPECT_FALSE(lsm.get("key1").has_value());
}

// Test iterator functionality
TEST_F(LSMTest, IteratorOperations) {
  // 打印配置信息
  debug(TomlConfig::getInstance().getLsmTolMemSizeLimit());
  debug(TomlConfig::getInstance().getLsmPerMemSizeLimit());
  debug(TomlConfig::getInstance().getLsmBlockSize());

  LSM lsm(test_dir);
  std::map<std::string, std::string> reference;

  // Insert data
  for (int i = 0; i < 100; i++) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    lsm.put(key, value);
    reference[key] = value;
  }

  // Test iterator
  auto it = lsm.begin(0);
  auto ref_it = reference.begin();

  while (it != lsm.end() && ref_it != reference.end()) {
    EXPECT_EQ(it->first, ref_it->first);
    EXPECT_EQ(it->second, ref_it->second);
    ++it;
    ++ref_it;
  }

  EXPECT_EQ(it == lsm.end(), ref_it == reference.end());
}

TEST_F(LSMTest, MonotonyPredicate) {
  // 打印配置信息
  debug(TomlConfig::getInstance().getLsmTolMemSizeLimit());
  debug(TomlConfig::getInstance().getLsmPerMemSizeLimit());
  debug(TomlConfig::getInstance().getLsmBlockSize());

  LSM lsm(test_dir);

  // Insert data
  for (int i = 0; i < 100; i++) {
    std::ostringstream oss_key;
    std::ostringstream oss_value;
    oss_key << "key" << std::setw(2) << std::setfill('0') << i;
    oss_value << "value" << std::setw(2) << std::setfill('0') << i;
    std::string key = oss_key.str();
    std::string value = oss_value.str();
    lsm.put(key, value);
    if (i == 50) {
      // 主动刷一次盘
      lsm.flush();
    }
  }

  // Define a predicate function
  auto predicate = [](const std::string &key) -> int {
    // Extract the number from the key
    int key_num = std::stoi(key.substr(3));
    if (key_num < 20) {
      return 1;
    }
    if (key_num > 60) {
      return -1;
    }
    return 0;
  };

  // Call the method under test
  auto result = lsm.lsm_iters_monotony_predicate(0, predicate);

  // Check if the result is not empty
  ASSERT_TRUE(result.has_value());

  // Extract the iterators from the result
  auto [start, end] = result.value();

  // Verify the range of keys returned by the iterators
  std::set<std::string> expected_keys;
  for (int i = 20; i <= 60; i++) {
    std::ostringstream oss;
    oss << "key" << std::setw(2) << std::setfill('0') << i;
    expected_keys.insert(oss.str());
    debug("expect key:", oss.str());
  }

  std::set<std::string> actual_keys;
  for (auto it = start; it != end; ++it) {
    actual_keys.insert(it->first);
    debug("actual key:", it->first);
  }

  debug("answer: ", expected_keys == actual_keys);

  EXPECT_EQ(actual_keys, expected_keys);
}

TEST_F(LSMTest, TrancIdTest) {
  // 注意是 LSMEngine 而不是 LSM
  // 因为 LSMEngine 才能手动控制事务id
  LSMEngine lsm(test_dir);

  // key00-key20 先插入, 此时事务id为1
  for (int i = 0; i < 20; i++) {
    std::ostringstream oss_key;
    oss_key << "key" << std::setw(2) << std::setfill('0') << i;
    std::string key = oss_key.str();
    lsm.put(key, "tranc1", 1);
  }
  lsm.flush();

  // key10-key10 再插入, 此时事务id为2
  for (int i = 0; i < 10; i++) {
    std::ostringstream oss_key;
    oss_key << "key" << std::setw(2) << std::setfill('0') << i;
    std::string key = oss_key.str();
    lsm.put(key, "tranc2", 2);
  }

  // 在事务id为1时进行遍历, 事务id为2的记录是不可见的
  for (int i = 0; i < 20; i++) {
    std::ostringstream oss_key;
    oss_key << "key" << std::setw(2) << std::setfill('0') << i;
    std::string key = oss_key.str();

    auto res = lsm.get(key, 1);

    EXPECT_EQ(res.value().first, "tranc1");
  }

  // 在事务id为2时进行遍历, 事务id为2的记录现在是可见的了
  for (int i = 0; i < 20; i++) {
    std::ostringstream oss_key;
    oss_key << "key" << std::setw(2) << std::setfill('0') << i;
    std::string key = oss_key.str();

    auto res = lsm.get(key, 2);
    if (i < 10) {
      EXPECT_EQ(res.value().first, "tranc2");
    } else {
      EXPECT_EQ(res.value().first, "tranc1");
    }
  }
}

TEST_F(LSMTest, TranContextTest) {
  LSM lsm(test_dir);
  auto tran_ctx = lsm.begin_tran(IsolationLevel::REPEATABLE_READ);

  tran_ctx->put("key1", "value1");
  tran_ctx->put("key2", "value2");

  auto query = lsm.get("key1");
  // 事务还没有提交, 应该查不到数据
  EXPECT_FALSE(query.has_value());

  auto commit_res = tran_ctx->commit();
  EXPECT_TRUE(commit_res);

  // 事务已经提交, 应该可以查到数据
  query = lsm.get("key1");
  EXPECT_EQ(query.value(), "value1");
  query = lsm.get("key2");
  EXPECT_EQ(query.value(), "value2");

  auto tran_ctx2 = lsm.begin_tran(IsolationLevel::REPEATABLE_READ);
  tran_ctx2->put("key1", "value1");
  tran_ctx2->put("key2", "value2");

  lsm.put("key2", "value22");

  commit_res = tran_ctx2->commit();
  EXPECT_FALSE(commit_res);
}

TEST_F(LSMTest, Recover) {
  {
    LSM lsm(test_dir);

    lsm.put("xxx  ", "yyy");
    auto tran_ctx = lsm.begin_tran(IsolationLevel::REPEATABLE_READ);

    for (int i = 0; i < 100; i++) {
      std::ostringstream oss_key;
      std::ostringstream oss_value;
      oss_key << "key" << std::setw(2) << std::setfill('0') << i;
      oss_value << "value" << std::setw(2) << std::setfill('0') << i;
      std::string key = oss_key.str();
      std::string value = oss_value.str();

      tran_ctx->put(key, value);
    }

    // 提交事务时true表示不会真正写入
    tran_ctx->commit(true);
  }
  {
    debug("in front of the new lsm");
    LSM lsm(test_dir);
    debug("new lsm end constract");

    for (int i = 0; i < 100; i++) {
      std::ostringstream oss_key;
      std::ostringstream oss_value;
      oss_key << "key" << std::setw(2) << std::setfill('0') << i;
      oss_value << "value" << std::setw(2) << std::setfill('0') << i;
      std::string key = oss_key.str();
      std::string value = oss_value.str();

      debug("now fine kv:", key, value);

      EXPECT_EQ(lsm.get(key).value(), value);
    }
  }
}

TEST_F(LSMTest, TestBlockCache) {
  // 更偏向缓存收益的基准：大量重复命中少量热点 key
  const size_t num_keys = 100000;      // 全量数据规模
  const size_t hot_keys = 16384;        // 热集规模
  const size_t value_len = 64;         // 解码成本
  const size_t bench_queries = 200000; // 查询数

  // 构建数据
  {
    LSMEngine build_engine(test_dir);
    std::string value(value_len, 'v');
    for (size_t i = 0; i < num_keys; ++i) {
      build_engine.put("key" + std::to_string(i), value, 0);
    }
    // 手动刷盘，这一步就是 LSM 析构函数，把内存中的数据持久化
    while (build_engine.memtable.get_total_size() > 0) build_engine.flush();
  }

  // 生成热集查询序列（仅从 key_0 ~ key_hot_keys 中采样）
  std::mt19937_64 rng(9527);
  std::uniform_int_distribution<size_t> hot_dist(0, hot_keys - 1);
  std::vector<std::string> queries;
  queries.reserve(bench_queries);
  for (size_t i = 0; i < bench_queries; ++i) queries.emplace_back("key" + std::to_string(hot_dist(rng)));

  for (int check = 0; check < 2; check ++ ) {
    LSMEngine engine(test_dir, check==1?false:true);

    // 随机热点查询
    auto t1_begin = std::chrono::steady_clock::now();
    size_t found1 = 0;
    for (const auto &k : queries) {
      auto res = engine.get(k, 0);
      if (res.has_value()) ++found1;
    }
    auto t1_end = std::chrono::steady_clock::now();

    auto dur1_us = std::chrono::duration_cast<std::chrono::microseconds>(t1_end - t1_begin).count();
    double avg1_us = static_cast<double>(dur1_us) / static_cast<double>(queries.size());
    double qps1 = static_cast<double>(queries.size()) * 1e6 / static_cast<double>(dur1_us);

    if (check == 0) debug("now block cache exist!");
    else debug("now block cache not exist!");
    if (engine.block_cache) debug("BlockCache hit rate (hot random):", engine.block_cache->hit_rate());
    debug("HotsetBenchmark(random): hot_keys=", hot_keys, ", queries=", bench_queries, ", found=", found1);
    debug("random_total_us=", dur1_us, ", random_avg_us=", avg1_us, ", random_qps=", qps1);

  }
  SUCCEED();
}

TEST_F(LSMTest, TestBloomFilter) {
  // 构造最能体现 Bloom 的场景：仅插入偶数键，查询大量“区间内不存在”的奇数键
  const size_t num_pairs = 100000;       // 数据规模的一半（偶数键总数）
  const size_t value_len = 256;           // 加重单次解码开销
  const size_t bench_queries = 100000;   // 计时查询量

  // 1) 构建数据：插入 [0, 2*num_pairs) 的所有偶数 key
  {
    LSMEngine put_engine(test_dir);
    std::string value(value_len, 'v');
    for (size_t i = 0; i < 2 * num_pairs; i += 2) {
      put_engine.put("key_" + std::to_string(i), value, 0);
    }
    while (put_engine.memtable.get_total_size() > 0) put_engine.flush();
  }

  // 2) 生成查询：全部是“区间内不存在”的奇数 key（Bloom 可直接判否）
  std::vector<std::string> queries;
  queries.reserve(bench_queries);
  for (size_t i = 0; i < bench_queries; ++i) {
    size_t odd = ((i * 1315423911ull) % (2 * num_pairs));
    odd |= 1ull; // 强制为奇数
    if (odd >= 2 * num_pairs) odd -= 2; // 保证落在范围内且为奇数
    queries.emplace_back("key_" + std::to_string(odd));
  }
  for (int check = 0; check < 2; check ++ ) {
    // 这里把 block cache 关闭更能体现布隆过滤器的作用
    // 因为对于一个 block ，我们会查询其中的一半不存在元素，如果使用了缓存，只有第一次才需要读取 block
    // 注意：布隆过滤器的作用本身就是 **进一步** 减少无效 block 的读取！！

    LSMEngine engine(test_dir, false);
    if (check == 1) {
      for (auto &[level, ids]: engine.level_sst_ids) {
        for (auto &sst_id: ids) {
          auto sst = engine.ssts[sst_id];
          // 测试时在 sst.h 中把 bloom filter 移动到 public
          sst->bloom_filter = nullptr;
        }
      }
    }

    // 计时（全部是区间内不存在的 key）
    auto t_begin = std::chrono::steady_clock::now();
    size_t found = 0;
    for (const auto &k : queries) {
      auto r = engine.get(k, 0);
      if (r.has_value()) ++found; // 理论应为 0
    }
    auto t_end = std::chrono::steady_clock::now();

    auto dur_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_begin).count();
    double avg_us = static_cast<double>(dur_ms) * 1000.0 / static_cast<double>(queries.size());

    if (check == 1) debug("now bloom filter not exist!");
    else debug("now bloom filter exist!");
    debug("Bloom bench (in-range negatives): queries=", queries.size(), ", found=", found);
    debug("total_ms=", dur_ms, ", avg_us_per_get=", avg_us);
  }

  SUCCEED();
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  init_spdlog_file();
  return RUN_ALL_TESTS();
}