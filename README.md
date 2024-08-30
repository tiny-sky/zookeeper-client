# zookeeper-client

zookeeper-client 是 [Apache ZooKeeper](https://zookeeper.apache.org/) 的 c++ 客户端库,包括一个高级 API 框架和应用程序，使 ZooKeeper 的使用更加容易。

## 介绍

Zookeeper 作为一个分布式服务框架，主要用来解决分布式数据一致性问题，它提供了简单的分布式原语，zookeeper-client 对其提供的 c 系 API 进行封装，并在此基础上提供以下功能：
1. 支持连接重连，底层通过 EventLoop 设置定时器任务，在 session 超时后进行重连。
2. 支持 Watch 反复注册，取消 Watch 的重注册。
3. 提供同步、异步两种接口。支持递归增删操作，优化回调函数。
4. 对节点操作进行简化，对常见错误码进行归类，并自动处理常见的 ZooKeeper 异常。

## 构建

本项目的构建依赖于 [zookeeper_mt](https://zookeeper.apache.org/releases.html)、[boost](https://github.com/boostorg/boost)、[muduo](https://github.com/chenshuo/muduo) 等第三方库。

在 RPC 模块中依赖于 [protobuf](https://github.com/protocolbuffers/protobuf)\
在 Master-Slave 模块中依赖于 [nlohmann](https://github.com/nlohmann/json)

安装好上述依赖后
```
mkdir build && cd build
cmake ..
make
```
在 example 目录下实现了众多使用场景，你可以使用下面命令对不同模块进行测试，例如：

基础模块测试
```
make basetest
./test/basetest
```
分布式锁测试
```
make distribute_lock
./test/distribute_lock
```

## 常用场景

基于 zookeeper-client 框架实现了 zookeeper [常见使用场景](./example/README.md)


