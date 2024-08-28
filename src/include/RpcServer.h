#pragma once

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
  RpcServer(EventLoop* loop, const InetAddress& listenAddr);

  void setThreadNum(int numThreads) { server_.setThreadNum(numThreads); }

  void registerService(::google::protobuf::Service*);
  void start();

  private:
  void onConnection(const TcpConnectionPtr& conn);

  TcpServer server_;
  std::map<std::string, ::google::protobuf::Service*> services_;
};
}  // namespace zkclient
