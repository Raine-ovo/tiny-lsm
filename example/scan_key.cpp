#include "../include/lsm/engine.h"
#include <filesystem>
#include <iostream>

using namespace tiny_lsm;

int main() {
  std::string dir = "persist_debug_data";
  if (!std::filesystem::exists(dir)) {
    std::cerr << "No dir: " << dir << std::endl;
    return 1;
  }

  LSMEngine engine(dir);
  std::cout << "Levels: max=" << engine.cur_max_level << std::endl;
  for (auto &[lvl, ids] : engine.level_sst_ids) {
    std::cout << "Level " << lvl << ": count=" << ids.size() << std::endl;
    for (auto id : ids) {
      if (!engine.ssts.count(id) || !engine.ssts[id]) {
        std::cout << "  sst=" << id << " missing in map" << std::endl;
        continue;
      }
      auto sst = engine.ssts[id];
      std::string first = sst->get_first_key();
      std::string last = sst->get_last_key();
      std::string range = first + " .. " + last;
      if (!("key1" >= first && "key1" <= last)) {
        std::cout << "  sst=" << id << " range=" << range << " skip (out of range)" << std::endl;
        continue;
      }
      auto it = sst->get("key1", 0);
      if (it.is_valid()) {
        auto kv = *it;
        std::cout << "  sst=" << id << " range=" << range << " FOUND value.len=" << kv.second.size() << std::endl;
      } else {
        std::cout << "  sst=" << id << " range=" << range << " not found, dump first 10: ";
        int cnt = 0;
        auto bit = sst->begin(0);
        for (; bit.is_valid() && bit != sst->end() && cnt < 10; ++bit, ++cnt) {
          auto kv2 = *bit;
          std::cout << kv2.first << (cnt < 9 ? "," : "");
        }
        std::cout << std::endl;
      }
    }
  }
  return 0;
} 