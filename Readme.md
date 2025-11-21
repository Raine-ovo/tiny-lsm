# Tiny-LSM
## 简介
C++17 实现的基于 LSM-Tree 的 KV 存储引擎。

## 核心模块

1. LSM-Tree 实现

    - 实现 SkipList -> MemTable -> SST 的核心数据结构模块，并分别设计基于 SkipList 和 MemTable 的 **内存迭代器** 及基于 SST 的 **惰性迭代器**，并基于迭代器在查询过程中自动过滤无效数据。
    - 设计内存数据结构的编码格式，支持批量化操作减少 I/O 次数以减少性能损耗。
    - SSTable 层级合并策略：全量合并。
    - 设计缓存池和布隆过滤器来优化 LSM-Tree ，提高系统性能，防止缓存穿透。

2. 事务功能

    - 通过自定义事务管理器实现事务的基本功能，包括事务提交和回滚，并实现事务不同隔离级别下的 ACID 特性（MVCC）。
    - 本项目实现的事务隔离级别：Read Uncommitted、Read Committed、Repeatable Read、Serializable。

3. WAL 和崩溃恢复

    实现 Write-Ahead Logging 机制，执行数据操作之前将修改操作记录日志，系统崩溃重启后通过 WAL 日志来恢复数据。

4. Redis 兼容层

    实现对 redis-cli 的兼容，可直接通过 redis-cli 进行使用（通过 `xmake run server` 来监听 6379 端口）


## 使用方法

在本目录下使用 `xmake` 进行编译，使用 `xmake run server` 即可，默认监听 6379 端口。

若不使用 RESP 协议，则参照 /example 下的 `main.cpp` 文件进行编写以使用该存储引擎。
