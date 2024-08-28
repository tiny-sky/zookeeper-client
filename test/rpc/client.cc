#include "echo.pb.h"

#include "ZkClient.h"
#include "ZkClientManager.h"
#include "ZkUtil.h"

#include <optional>

#include "muduo/base/Logging.h"
#include "muduo/net/EventLoop.h"
#include "muduo/net/InetAddress.h"
#include "muduo/net/TcpClient.h"
#include "muduo/net/TcpConnection.h"

#include <google/protobuf/message.h>

#include "RpcChannel.h"

using namespace muduo;
using namespace zkclient;
using namespace muduo::net;

#define ZOOKEEPER_SERVER_CONN_STRING \
  "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"

using RpcChannelPtr = std::shared_ptr<zkclient::RpcChannel>;

class EchoClient {
  public:
  EchoClient(EventLoop* loop, const InetAddress& serverAddr)
      : loop_(loop),
        client_(loop, serverAddr, "RpcClient"),
        channel_(new RpcChannel),
        stub_(channel_.get()) {
    client_.setConnectionCallback(
        std::bind(&EchoClient::onConnection, this, _1));
    client_.setMessageCallback(
        std::bind(&RpcChannel::onMessage, channel_.get(), _1, _2, _3));
  }

  void connect() { client_.connect(); }

  private:
  void onConnection(const TcpConnectionPtr& conn) {
    LOG_INFO << "Connection established\n";
    if (conn->connected()) {
      channel_->setConnection(conn);
      echo::EchoRequest request;
      request.set_msg("Hello World");
      echo::EchoResponse* response = new echo::EchoResponse;

      stub_.Echo(nullptr, &request, response,
                 NewCallback(this, &EchoClient::echo, response));
    }
  }

  void echo(echo::EchoResponse* resp) {
    std::cout << "Client receive -> " << resp->DebugString().c_str()
              << std::endl;
    client_.disconnect();
  }
  EventLoop* loop_;
  TcpClient client_;
  RpcChannelPtr channel_;
  echo::EchoService_Stub stub_;
};

class zkAddress {
  public:
  zkAddress() : zkConnStr_("") {};
  ~zkAddress() {
    ZkClientManager::instance().destroyClient(zkClient_->getHandle());
    zkClient_.reset();
  }

  bool init(const std::string zkConnStr) {
    zkConnStr_ = zkConnStr;

    //设置zookeeper日志路径
    if (ZkClientManager::setLogConf(true, "./zk_log") == false) {
      std::cout << "setLogConf failed!" << std::endl;
      return false;
    }

    //创建一个session
    uint32_t handle = ZkClientManager::instance().createZkClient(
        zkConnStr_, 30000, NULL, NULL, NULL);
    if (handle == 0) {
      std::cout << "create session failed! connStr: " << zkConnStr_
                << std::endl;
      return false;
    }

    zkClient_ = ZkClientManager::instance().getZkClient(handle);
  }

  std::optional<InetAddress> getaddrbyservice(const std::string& service) {
    std::string address = "";
    int32_t version;
    std::string path = parentPath_ + "/" + service;
    if (zkClient_->getNode(path, address, version) != zkutil::kZKSucceed) {
      std::cout << "Invalid service" << std::endl;
      return std::nullopt;
    }

    std::size_t pos = address.find(":");
    if (pos == std::string::npos) {
      std::string ip = address.substr(0, pos);
      int port = std::stoi(address.substr(pos + 1));

      return InetAddress(ip, port);
    }
  }

  private:
  std::string zkConnStr_;
  ZkClientPtr zkClient_;

  const std::string parentPath_ = "/rpc";
};

int main(int argc, char* argv[]) {
  if (argc > 1) {

    EventLoop loop;
  
    zkAddress zkaddress;
    if (!zkaddress.init(ZOOKEEPER_SERVER_CONN_STRING)) {
      std::cout << "zkaddress init failed" << std::endl;
      return 0;
    }

    std::optional<InetAddress> serverAddr = zkaddress.getaddrbyservice("echo");
    if (!serverAddr) {
      std::cout << "get address by service failed" << std::endl;
      return 0;
    }

    EchoClient client(&loop, *serverAddr);
    client.connect();  // 建立 sockfd ，监听可写事件
    loop.loop();
  } else {
    std::cout << "Usage: " << argv[0] << "host_ip" << std::endl;
  }

  google::protobuf::ShutdownProtobufLibrary();
}