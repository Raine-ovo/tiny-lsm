#include "../../include/skiplist/skiplist.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

namespace tiny_lsm {

// ************************ SkipListIterator ************************
BaseIterator& SkipListIterator::operator++() {
  std::shared_ptr<SkipListNode> next_node = this->current->forward_[0];
  this->current = next_node;
  return *this;
}

bool SkipListIterator::operator==(const BaseIterator& other) const {
  if (other.get_type() != IteratorType::SkipListIterator) {
    return false; // 类别不同
  }
  // 类别相同，需要把基类转化为子类，才能使用子类的成员和方法
  // 使用 static_cast 转化
  /*
     因为在前面已经判断过了，因此到了这里，这个 BaseIterator 一定是 SkipListIterator 子类，不需要使用运行时检查的
     dynamic_cast，这个转换开销比较大。
  */
  const SkipListIterator& other_skiplist = static_cast<const SkipListIterator&>(other);
  return this->current == other_skiplist.current;
}

bool SkipListIterator::operator!=(const BaseIterator &other) const {
  if (other.get_type() != IteratorType::SkipListIterator) {
    return true;
  }
  const SkipListIterator& other_skiplist = static_cast<const SkipListIterator&>(other);
  return this->current != other_skiplist.current;
}

SkipListIterator::value_type SkipListIterator::operator*() const {
  return {this->get_key(), this->get_value()};
}

IteratorType SkipListIterator::get_type() const {
  return IteratorType::SkipListIterator;
}

bool SkipListIterator::is_valid() const {
  return current && !current->key_.empty();
}
bool SkipListIterator::is_end() const { return current == nullptr; }

std::string SkipListIterator::get_key() const { return current->key_; }
std::string SkipListIterator::get_value() const { return current->value_; }
uint64_t SkipListIterator::get_tranc_id() const { return current->tranc_id_; }

// ************************ SkipList ************************
// 构造函数
SkipList::SkipList(int max_lvl) : max_level(max_lvl), current_level(1) {
  head = std::make_shared<SkipListNode>("", "", max_level, 0);
  dis_01 = std::uniform_int_distribution<>(0, 1);
  dis_level = std::uniform_int_distribution<>(0, (1 << max_lvl) - 1);
  gen = std::mt19937(std::random_device()());
}

int SkipList::random_level() {
  // 插入时随机为这一次操作确定其最高连接的链表层数
  // ? 通过"抛硬币"的方式随机生成层数：
  // ? - 每次有50%的概率增加一层
  // ? - 确保层数分布为：第1层100%，第2层50%，第3层25%，以此类推
  // ? - 层数范围限制在[1, max_level]之间，避免浪费内存

  int level = 1; // 在底层是一定要创建的，因此层数最低为 1
	while (dis_01(gen) && level < max_level) {
		++ level;
	}
  return level;
}

// 插入或更新键值对
/*
  * 跳表 Put 流程：
  1. 首先要查找插入的位置
  2. 创建一个新节点（在最底层）
  3. 判断上层是否需要创建新节点，一般是采用随机方法，比如倒数第二层有 50% 概率生成这个节点；如果倒数第二层生成了，那么
     倒数第三层就有 50% * 50% = 25% 概率生成这个节点
     注意在每一层生成节点的时候，要保证节点被正确链接
  4. 一开始的跳表是一层，通过 (3) 可以判断需不需要生成新的层
*/
void SkipList::put(const std::string &key, const std::string &value,
                   uint64_t tranc_id) {
  spdlog::trace("SkipList--put({}, {}, {})", key, value, tranc_id);

	// 使用一个 update 容器，记录每个层中，新节点可以插入的位置的前驱节点
	std::vector<std::shared_ptr<SkipListNode>> updates(this->max_level, nullptr);

	int new_node_level = this->random_level();
	int current_level = this->current_level;
  // int new_level = std::max(new_node_level, current_level);
	std::shared_ptr<SkipListNode> new_node = std::make_shared<SkipListNode>(key, value, new_node_level, tranc_id);

  // 1. 查找需要插入的位置
  std::shared_ptr<SkipListNode> cur_node = this->head;
	// 按查找方式，从最高层开始查找
	for (int cur_level = current_level - 1; cur_level >= 0; cur_level -- ) {
		// 注意我们这的 node 全是指针形式，如果要用重载的比较函数，应该取其值
		while(cur_node->forward_[cur_level] && *cur_node->forward_[cur_level] < *new_node) {
			cur_node = cur_node->forward_[cur_level];
		}
		// cur_node 是 new_node 的前驱节点
		updates[cur_level] = cur_node;
	}
	// 注意 updates 还没有更新完，如果 new_node_level > current_level ，那还有层需要更新
	for (int cur_level = current_level; cur_level < new_node_level; cur_level ++ ) {
		updates[cur_level] = this->head;
	}

	cur_node = cur_node->forward_[0];

	// // key 存在了，修改值
	// if (cur_node && cur_node->key_ == key) {
	// 	// 注意，无论那一层，其实都是一个节点！！！！
	// 	// 跳表的特性一定牢记！！！
	// 	this->size_bytes += value.size() - cur_node->value_.size();
	// 	cur_node->value_ = value;
	// 	cur_node->tranc_id_ = tranc_id;
	// 	return;


	// } else

  // * update: 加入 MVCC 机制，即事务多版本并发控制，加入了 tran_id ，因此不能直接原地修改
  // 我们可以选择直接加入节点，这样不会有影响，因为 tran id 在 node 比较函数中，因此我们找到的前驱就算是同 key， tranc_id 也是符合我们插入顺序的
  // 简而言之，就是我们一定会选择一个合适的位置插入 node，使得 tranc id 满足从大到小
  // 这样的一个后果就是同一个事务 id 对同一个 key 多次操作时，会导致链表出现多个同 key 同 tranc id 的节点，不过这不影响 kv 存储引擎
  // 因为我们永远是找到第一个，也就是最新的那个操作节点

  // 不过我们也可以进行优化，减小内存量
  if (cur_node && cur_node->key_ == key && cur_node->tranc_id_ == tranc_id)
  {
    // 事务相同，直接修改 value 就行了，不用新增节点
    size_bytes += value.size() - cur_node->value_.size();
    cur_node->value_ = value;
  }
  else
  {
		// 不存在 key，创建节点
		this->size_bytes += key.size() + value.size() + sizeof(uint64_t);
		for (int cur_level = 0; cur_level < new_node_level; cur_level ++ ) {
			auto pre_node = updates[cur_level];
			if (!pre_node) return;
			new_node->forward_[cur_level] = pre_node->forward_[cur_level];
			new_node->backward_[cur_level] = pre_node;
			if (pre_node->forward_[cur_level]) {
				pre_node->forward_[cur_level]->backward_[cur_level] = new_node;
			}
			pre_node->forward_[cur_level] = new_node;
		}
		this->current_level = std::max(new_node_level, current_level);
	}
}

// 查找键值对
SkipListIterator SkipList::get(const std::string &key, uint64_t tranc_id) {
  spdlog::trace("SkipList--get({}, {}) called", key, tranc_id);

	std::shared_ptr<SkipListNode> cur_node = this->head;
	for (int cur_level = this->current_level - 1; cur_level >= 0; cur_level -- ) {
		while (cur_node->forward_[cur_level] != nullptr && cur_node->forward_[cur_level]->key_ < key) {
			cur_node = cur_node->forward_[cur_level];
		}
	}
	// cur_node 是 key 节点前驱节点
	cur_node = cur_node->forward_[0];
  // 根据 tranc_id 来判断当前的 MVCC 机制的隔离级别
  if (tranc_id == 0) {
    // 没有开启事务，直接返回最新的
    if (cur_node && cur_node->key_ == key) return SkipListIterator{cur_node};
  } else {
    // 开启 MVCC ，实现可重复读，读取的数据不能是事务 id 比参数的事务 id 大的，因为这个项目中事务 id 表示了事务开始的时间顺序
    // 可重复读要求读取的是事务开始时候的数据，也就是数据事务 id <= 参数 事务 id 的
    while (cur_node && cur_node->key_ == key && cur_node->tranc_id_ > tranc_id) {
      cur_node = cur_node->forward_[0];
    }
    if (cur_node && cur_node->key_ == key) return SkipListIterator{cur_node};
  }
  return SkipListIterator{};
}

// 删除键值对
// ! 这里的 remove 是跳表本身真实的 remove,  lsm 应该使用 put 空值表示删除,
// ! 这里只是为了实现完整的 SkipList 不会真正被上层调用
/*
 * 删除键值对流程
 1. 按照查找流程找到对应的值
 2. 从最底层开始逐层删除
 3. 如果这一层只剩下头节点和尾节点，则删除这一层
*/
void SkipList::remove(const std::string &key) {

	std::vector<std::shared_ptr<SkipListNode>> updates(this->max_level, nullptr);

  std::shared_ptr<SkipListNode> cur_node = this->head;
  int current_level = this->current_level;
	for (int cur_level = current_level - 1; cur_level >= 0; cur_level -- ) {
		while (cur_node->forward_[cur_level] != nullptr && cur_node->forward_[cur_level]->key_ < key) {
			cur_node = cur_node->forward_[cur_level];
		}
		// 记录前驱
		updates[cur_level] = cur_node;
	}

	// 到底层
	cur_node = cur_node->forward_[0];
	if (cur_node && cur_node->key_ == key) {
		// 对了，删除
		this->size_bytes -= key.size() + cur_node->value_.size() + sizeof(uint64_t);
		for (int cur_level = 0; cur_level < current_level; cur_level ++ ) {
			if (updates[cur_level]->forward_[cur_level] != cur_node) {
				break;
			}
			updates[cur_level]->forward_[cur_level] = cur_node->forward_[cur_level];
			if (cur_node->forward_[cur_level]) {
				cur_node->forward_[cur_level]->backward_[cur_level] = updates[cur_level];
			}
		}

		// 注意把一些空链表删除, 链表最小一个
		while (this->current_level > 1 && head->forward_[this->current_level-1] == nullptr) {
			this->current_level -- ;
		}
	}
}

// 刷盘时可以直接遍历最底层链表
std::vector<std::tuple<std::string, std::string, uint64_t>> SkipList::flush() {
  // std::shared_lock<std::shared_mutex> slock(rw_mutex);
  spdlog::debug("SkipList--flush(): Starting to flush skiplist data");

  std::vector<std::tuple<std::string, std::string, uint64_t>> data;
  auto node = head->forward_[0];
  while (node) {
    data.emplace_back(node->key_, node->value_, node->tranc_id_);
    node = node->forward_[0];
  }

  spdlog::debug("SkipList--flush(): Flushed {} entries", data.size());

  return data;
}

size_t SkipList::get_size() {
  // std::shared_lock<std::shared_mutex> slock(rw_mutex);
  return size_bytes;
}

// 清空跳表，释放内存
void SkipList::clear() {
  // std::unique_lock<std::shared_mutex> lock(rw_mutex);
  head = std::make_shared<SkipListNode>("", "", max_level, 0);
  size_bytes = 0;
}

SkipListIterator SkipList::begin() {
  // return SkipListIterator(head->forward[0], rw_mutex);
  return SkipListIterator(head->forward_[0]);
}

SkipListIterator SkipList::end() {
  return SkipListIterator(); // 使用空构造函数
}

// 找到前缀的起始位置
// 返回第一个前缀匹配或者大于前缀的迭代器
// [begin
SkipListIterator SkipList::begin_preffix(const std::string &preffix) {

  std::shared_ptr<SkipListNode> cur_node = this->head;
	for (int cur_level = this->current_level - 1; cur_level >= 0; cur_level -- ) {
		while (cur_node->forward_[cur_level] != nullptr && cur_node->forward_[cur_level]->key_ < preffix) {
			cur_node = cur_node->forward_[cur_level];
		}
	}
	// cur_node 是 key 节点前驱节点
  cur_node = cur_node->forward_[0];
  return SkipListIterator{cur_node};
}

// 找到前缀的终结位置
// end)
SkipListIterator SkipList::end_preffix(const std::string &prefix) {
  auto is_prefix = [](const std::string& a, const std::string& b) -> bool {
    return a.size() >= b.size() &&
      std::equal(b.begin(), b.end(), a.begin());
  };

  std::shared_ptr<SkipListNode> cur_node = this->head;
	for (int cur_level = this->current_level - 1; cur_level >= 0; cur_level -- ) {
		while (cur_node->forward_[cur_level] != nullptr && cur_node->forward_[cur_level]->key_ < prefix) {
			cur_node = cur_node->forward_[cur_level];
		}
	}
	// cur_node 是 key 节点前驱节点
  cur_node = cur_node->forward_[0];
  while (cur_node && is_prefix(cur_node->key_, prefix)) cur_node = cur_node->forward_[0];
  return SkipListIterator{cur_node};
}

// ? 这里单调谓词的含义是, 整个数据库只会有一段连续区间满足此谓词
// ? 例如之前特化的前缀查询，以及后续可能的范围查询，都可以转化为谓词查询
// ? 返回第一个满足谓词的位置和最后一个满足谓词的迭代器
// ? 如果不存在, 范围nullptr
// ? 谓词作用于key, 且保证满足谓词的结果只在一段连续的区间内, 例如前缀匹配的谓词
// ? predicate返回值:
// ?   0: 满足谓词
// ?   >0: 不满足谓词, 需要向右移动
// ?   <0: 不满足谓词, 需要向左移动
// ! Skiplist 中的谓词查询不会进行事务id的判断, 需要上层自己进行判断
std::optional<std::pair<SkipListIterator, SkipListIterator>>
SkipList::iters_monotony_predicate(
    std::function<int(const std::string &)> predicate) {

  // * 利用跳表快速进行谓词查询
  // 1. 从高层开始，找到第一个满足的节点 node_mid
  // 2. 往下层沉到0层，往前找到真正第一个满足的节点 node_begin，往后找到最后一个真正满足的节点 node_end

  std::shared_ptr<SkipListNode> node_mid = this->head;
  std::shared_ptr<SkipListNode> node_begin, node_end;
  int mid_level = 0;

	for (int cur_level = this->current_level - 1; cur_level >= 0; cur_level -- ) {
		while (node_mid->forward_[cur_level] && predicate(node_mid->forward_[cur_level]->key_) > 0) {
			node_mid = node_mid->forward_[cur_level];
		}
    // node_mid 是这一层中谓词的前缀节点
    if (node_mid->forward_[cur_level] && predicate(node_mid->forward_[cur_level]->key_) == 0) {
      node_mid = node_mid->forward_[cur_level];
      mid_level = cur_level;
      break;
    }
	}

  if (predicate(node_mid->key_) != 0) return std::nullopt;

  node_begin = node_mid;
  node_end = node_mid;

  // 找到 begin node 和 end node
  for (int cur_level = mid_level; cur_level >= 0; cur_level -- ) {
    while (node_begin->backward_[cur_level].lock() != head && predicate(node_begin->backward_[cur_level].lock()->key_) == 0) {
      node_begin = node_begin->backward_[cur_level].lock();
    }
    while (node_end->forward_[cur_level] && predicate(node_end->forward_[cur_level]->key_) == 0) {
      node_end = node_end->forward_[cur_level];
    }
  }

  // 注意迭代器是 [begin, end)
  return std::pair(SkipListIterator{node_begin}, SkipListIterator{node_end->forward_[0]});
}

// ? 打印跳表, 你可以在出错时调用此函数进行调试
void SkipList::print_skiplist() {
  for (int level = 0; level < current_level; level++) {
    std::cout << "Level " << level << ": ";
    auto current = head->forward_[level];
    while (current) {
      std::cout << current->key_;
      current = current->forward_[level];
      if (current) {
        std::cout << " -> ";
      }
    }
    std::cout << std::endl;
  }
  std::cout << std::endl;
}
} // namespace tiny_lsm
