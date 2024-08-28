#include "RpcServer.h"

#include "ZkClient.h"
#include "ZkClientManager.h"
#include "ZkUtil.h"

#include "muduo/base/Logging.h"
#include "muduo/net/EventLoop.h"
#include "muduo/net/InetAddress.h"
#include "muduo/net/protorpc/RpcChannel.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/service.h>

using namespace muduo;
using namespace muduo::net;

namespace zkclient {

using std::placeholders::_1;
using std::placeholders::_2;
using std::placeholders::_3;

RpcServer::RpcServer(EventLoop* loop, const InetAddress& listenAddr,
                     const std::string& zkConnStr)
    : server_(loop, listenAddr, "RpcServer"), zkConnStr_(zkConnStr) {
  server_.setConnectionCallback(std::bind(&RpcServer::onConnection, this, _1));
  zkinit();
}

RpcServer::~RpcServer() {

  for (auto& server : services_) {
    std::string path = parentPath_ + "/" + server.first;
    zkClient_->deleteNode(path);
  }
  ZkClientManager::instance().destroyClient(zkClient_->getHandle());
  zkClient_.reset();
}

bool RpcServer::registerService(google::protobuf::Service* service,
                                const std::string ip) {
  const google::protobuf::ServiceDescriptor* desc = service->GetDescriptor();
  services_[desc->full_name()] = service;

  std::string path = parentPath_ + desc->full_name();
  std::string retPath;
  zkutil::ZkErrorCode ec = zkClient_->create(path, ip, false, false, retPath);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
    std::cout << "create parent path:" << parentPath_ << " failed!"
              << std::endl;
    return false;
  }
  return true;
}

void RpcServer::start() {
  server_.start();
}

void RpcServer::onConnection(const TcpConnectionPtr& conn) {
  LOG_INFO << "RpcServer - " << conn->peerAddress().toIpPort() << " -> "
           << conn->localAddress().toIpPort() << " is "
           << (conn->connected() ? "UP" : "DOWN");
  if (conn->connected()) {
    RpcChannelPtr channel = std::make_shared<RpcChannel>(conn);
    channel->setServices(&services_);
    conn->setMessageCallback(
        std::bind(&RpcChannel::onMessage, channel.get(), _1, _2, _3));
    conn->setContext(channel);
  } else {
    conn->setContext(RpcChannelPtr());
  }
}

bool RpcServer::zkinit() {
  //设置zookeeper日志路径
  if (ZkClientManager::setLogConf(true, "./zk_log") == false) {
    std::cout << "setLogConf failed!" << std::endl;
    return false;
  }

  //创建一个session
  uint32_t handle = ZkClientManager::instance().createZkClient(
      zkConnStr_, 30000, NULL, NULL, NULL);
  if (handle == 0) {
    std::cout << "create session failed! connStr: " << zkConnStr_ << std::endl;
    return false;
  }

  zkClient_ = ZkClientManager::instance().getZkClient(handle);
  return true;
}
}  // namespace zkclient
