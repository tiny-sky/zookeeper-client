#include "echo.pb.h"

#include "muduo/base/Logging.h"
#include "muduo/net/EventLoop.h"
#include "muduo/net/protorpc/RpcServer.h"

#include <unistd.h>
#include <iostream>

#include "RpcServer.h"

using namespace muduo;
using namespace muduo::net;

#define ZOOKEEPER_SERVER_CONN_STRING \
  "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"

namespace Echo {

class EchoServerImpl : public echo::EchoService {
  public:
  virtual void Echo(::google::protobuf::RpcController* controller,
                    const ::echo::EchoRequest* request,
                    ::echo::EchoResponse* response,
                    ::google::protobuf::Closure* done) {
    std::cout << " Server Echo -> " << request->msg() << std::endl;
    std::string msg("Server Echo -> " + request->msg());
    response->set_msg(msg);
    done->Run();
  }
};
}  // namespace Echo

int main() {

  EventLoop loop;
  InetAddress listenAddr(6666);
  Echo::EchoServerImpl impl;
  zkclient::RpcServer server(
      &loop, listenAddr,
      ZOOKEEPER_SERVER_CONN_STRING);  //TcpServer + RpcServerSet
  server.registerService(&impl,"127.0.0.1:6666");      // 注册事件
  server.start();                     // Listening + Add Read event
  loop.loop();  // 启动epoll_wait + 处理事件触发回调
  google::protobuf::ShutdownProtobufLibrary();
}
