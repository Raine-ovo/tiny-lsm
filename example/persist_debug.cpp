#include "../include/lsm/engine.h"
#include <filesystem>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <iomanip>

using namespace tiny_lsm;

int main() {
  std::string dir = "persist_debug_data";
  if (std::filesystem::exists(dir)) std::filesystem::remove_all(dir);
  std::filesystem::create_directory(dir);

  std::unordered_map<std::string, std::string> kvs;
  int num = 100000; // 压测规模
  {
    LSM lsm(dir);
    for (int i = 0; i < num; ++i) {
      std::string key = "key" + std::to_string(i);
      std::string value = "value" + std::to_string(i);
      lsm.put(key, value);
      kvs[key] = value;
      if (i % 10 == 0 && i != 0) {
        std::string del_key = "key" + std::to_string(i - 10);
        lsm.remove(del_key);
        kvs.erase(del_key);
      }
    }
  }

  LSM lsm(dir);
  for (int i = 0; i < num; ++i) {
    std::string key = "key" + std::to_string(i);
    auto exist = kvs.find(key) != kvs.end();
    auto res = lsm.get(key, true);
    if (exist && !res.has_value()) {
      std::cerr << "Missing key: " << key << std::endl;
      return 1;
    }
    if (!exist && res.has_value()) {
      std::cerr << "Unexpected key present: " << key << std::endl;
      return 2;
    }
  }
  std::cout << "All OK" << std::endl;
  return 0;
} 