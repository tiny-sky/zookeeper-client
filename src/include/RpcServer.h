#pragma once

#include "ZkClient.h"
#include "ZkClientManager.h"
#include "ZkUtil.h"

#include "muduo/net/TcpServer.h"
#include "muduo/net/EventLoop.h"
#include "muduo/net/InetAddress.h"

using namespace muduo;
using namespace muduo::net;

using muduo::net::EventLoop;
using muduo::net::InetAddress;

namespace google {
namespace protobuf {

class Service;
}  // namespace protobuf
}  // namespace google

namespace zkclient {

class RpcServer {
  public:
  RpcServer(EventLoop* loop, const InetAddress& listenAddr,const std::string& zkConnStr);

  ~RpcServer();

  void setThreadNum(int numThreads) { server_.setThreadNum(numThreads); }

  bool registerService(::google::protobuf::Service*,const std::string ip);
  void start();

  private:
  void onConnection(const TcpConnectionPtr& conn);

  bool zkinit();

  TcpServer server_;
  std::map<std::string, ::google::protobuf::Service*> services_;

  std::string zkConnStr_;
  ZkClientPtr zkClient_;
  const std::string parentPath_ = "/rpc";
};
}  // namespace zkclient
