#include "../include/lsm/engine.h"
#include "../include/config/config.h"
#include "../include/logger/logger.h"
#include "spdlog/spdlog.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <vector>

using namespace tiny_lsm;

enum class BenchMode { Default, Persistence };

struct BenchOptions {
  std::string data_dir = "bench_data";
  size_t num_keys = 500000;           // 总写入键数
  size_t value_size = 128;            // 值大小（Default模式）
  size_t rounds = 1;                  // 写入轮次（Default模式）
  int delete_percent = 0;             // 删除比例 [0,100]（Default模式）
  bool clean = true;                  // 是否清空目录
  bool random_keys = false;           // 是否打乱写入顺序（Default模式）
  int report_every = 50000;           // 进度与层级分布上报间隔
  BenchMode mode = BenchMode::Default; // 数据模式
  std::string config_path = "config.toml"; // 配置文件路径
};

static void print_levels(const LSMEngine &engine) {
  // 直接访问公共元数据打印层级分布
  std::ostringstream oss;
  oss << "Levels: ";
  bool first = true;
  for (const auto &kv : engine.level_sst_ids) {
    if (!first) oss << ", ";
    first = false;
    oss << "L" << kv.first << "=" << kv.second.size();
  }
  spdlog::info("{}", oss.str());
}

static std::string make_value(size_t sz) {
  std::string v;
  v.resize(sz, 'v');
  return v;
}

static std::string make_key(size_t idx, size_t width, size_t round) {
  // 前缀包含轮次，避免更新覆盖导致 sst 内容过早消除（Default模式）
  std::ostringstream oss;
  oss << "r" << round << "_key";
  for (size_t i = 0; i + 1 < width - std::to_string(idx).size(); ++i) oss << '0';
  oss << idx;
  return oss.str();
}

static BenchOptions parse_args(int argc, char **argv) {
  BenchOptions opt;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto next = [&](size_t &out) {
      if (i + 1 < argc) { out = static_cast<size_t>(std::stoull(argv[++i])); }
    };
    auto next_s = [&](std::string &out) {
      if (i + 1 < argc) { out = argv[++i]; }
    };
    if (a == "--dir") next_s(opt.data_dir);
    else if (a == "--num_keys") next(opt.num_keys);
    else if (a == "--value_size") next(opt.value_size);
    else if (a == "--rounds") next(opt.rounds);
    else if (a == "--delete_percent") opt.delete_percent = (i + 1 < argc ? std::stoi(argv[++i]) : 0);
    else if (a == "--no-clean") opt.clean = false;
    else if (a == "--random_keys") opt.random_keys = true;
    else if (a == "--report_every") opt.report_every = (i + 1 < argc ? std::stoi(argv[++i]) : opt.report_every);
    else if (a == "--mode") {
      std::string m = (i + 1 < argc ? std::string(argv[++i]) : std::string());
      if (m == "persistence") opt.mode = BenchMode::Persistence;
      else opt.mode = BenchMode::Default;
    }
    else if (a == "--config") {
      next_s(opt.config_path);
    }
    else if (a == "--help") {
      std::cout << "Usage: bench_compact [--dir DIR] [--num_keys N] [--value_size SZ]"
                   " [--rounds R] [--delete_percent P] [--no-clean] [--random_keys] [--report_every K]"
                   " [--mode default|persistence] [--config PATH]\n";
      std::exit(0);
    }
  }
  opt.delete_percent = std::clamp(opt.delete_percent, 0, 100);
  return opt;
}

int main(int argc, char **argv) {
  init_spdlog_file();
  BenchOptions opt = parse_args(argc, argv);

  // persistence 模式默认 10 万条（若未显式指定 num_keys）
  if (opt.mode == BenchMode::Persistence && opt.num_keys == 500000) {
    opt.num_keys = 100000;
  }

  // 强制加载配置（避免工作目录不同导致找不到 config.toml）
  const auto &cfg = TomlConfig::getInstance(opt.config_path);
  spdlog::info("Loaded config from '{}': TOL_MEM={}, PER_MEM={}, BLOCK_SIZE={}, LEVEL_RATIO={}",
               opt.config_path,
               cfg.getLsmTolMemSizeLimit(),
               cfg.getLsmPerMemSizeLimit(),
               cfg.getLsmBlockSize(),
               cfg.getLsmSstLevelRatio());

  spdlog::info("bench_compact: dir={}, num_keys={}, value_size={}, rounds={}, delete%={}, clean={}, random_keys={}, report_every={}, mode={}",
               opt.data_dir, opt.num_keys, opt.value_size, opt.rounds, opt.delete_percent, opt.clean, opt.random_keys, opt.report_every,
               (opt.mode == BenchMode::Default ? "default" : "persistence"));

  try {
    if (opt.clean && std::filesystem::exists(opt.data_dir)) {
      spdlog::info("Cleaning directory: {}", opt.data_dir);
      std::filesystem::remove_all(opt.data_dir);
    }
    std::filesystem::create_directories(opt.data_dir);
  } catch (const std::exception &e) {
    spdlog::error("prepare dir failed: {}", e.what());
    return 1;
  }

  // 创建引擎
  LSMEngine engine(opt.data_dir);

  auto t0 = std::chrono::steady_clock::now();

  if (opt.mode == BenchMode::Persistence) {
    // 复刻 test_lsm.cpp Persistence 模式：
    // - key: "key" + i
    // - value: "value" + i
    // - 每当 i % 10 == 0 且 i != 0 时，删除 key(i-10)
    size_t inserted = 0;
    for (size_t i = 0; i < opt.num_keys; ++i) {
      std::string key = "key" + std::to_string(i);
      std::string value = "value" + std::to_string(i);
      engine.put(key, value, 0);
      if (i % 10 == 0 && i != 0) {
        std::string del_key = "key" + std::to_string(i - 10);
        engine.remove(del_key, 0);
      }

      ++inserted;
      if (opt.report_every > 0 && (inserted % static_cast<size_t>(opt.report_every) == 0)) {
        spdlog::info("Progress: inserted={} / {}", inserted, opt.num_keys);
        print_levels(engine);
      }
    }
  } else {
    // Default 模式
    const std::string value = make_value(opt.value_size);
    const size_t key_width = 12; // 足够宽，避免排序错乱

    std::vector<size_t> indices(opt.num_keys);
    std::iota(indices.begin(), indices.end(), 0);
    std::mt19937_64 rng(std::random_device{}());

    for (size_t r = 0; r < opt.rounds; ++r) {
      if (opt.random_keys) {
        std::shuffle(indices.begin(), indices.end(), rng);
      }

      size_t inserted = 0;
      for (size_t k : indices) {
        const std::string key = make_key(k, key_width, r);

        // 删除比率控制：按比例写 tombstone（空值）
        bool as_delete = (opt.delete_percent > 0) && ((k % 100) < static_cast<size_t>(opt.delete_percent));
        if (as_delete) {
          engine.put(key, "", 0);
        } else {
          engine.put(key, value, 0);
        }

        ++inserted;
        if (opt.report_every > 0 && (inserted % opt.report_every == 0)) {
          spdlog::info("Progress: round={}, inserted={} / {}", r, inserted, opt.num_keys);
          print_levels(engine);
        }
      }

      spdlog::info("Round {} finished. Current level layout:", r);
      print_levels(engine);
    }
  }

  // 强制将所有 MemTable 刷盘
  spdlog::info("Flushing remaining memtables...");
  while (engine.memtable.get_total_size() > 0) {
    engine.flush();
  }
  print_levels(engine);

  auto t1 = std::chrono::steady_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

  // 汇总统计
  size_t total_sst = 0;
  for (const auto &kv : engine.level_sst_ids) total_sst += kv.second.size();
  spdlog::info("Summary: elapsed={} ms, total_sst={}, max_level={}", ms, total_sst, engine.cur_max_level);
  for (const auto &kv : engine.level_sst_ids) {
    spdlog::info("  L{}: {} sst(s)", kv.first, kv.second.size());
  }

  spdlog::info("bench_compact done.");
  return 0;
} 