# 基于 LSM-Tree 实现的 KV 存储引擎
实现了对 Redis-RESP 协议的兼容，可直接通过 redis-cli 进行使用。

使用方法：在本目录下使用 `xmake` 进行编译，使用 `xmake run server` 即可，默认监听 6379 端口。

若不使用 RESP 协议，则参照 /example 下的 `main.cpp` 文件进行编写以使用该存储引擎。
