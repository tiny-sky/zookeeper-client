#pragma once

#include "muduo/net/Callbacks.h"
#include "muduo/net/protorpc/RpcCodec.h"

#include <google/protobuf/service.h>
#include <atomic>
#include <map>
#include <mutex>

namespace google {
namespace protobuf {

// Defined in other files.
class Descriptor;         // descriptor.h
class ServiceDescriptor;  // descriptor.h
class MethodDescriptor;   // descriptor.h
class Message;            // message.h

class Closure;

class RpcController;
class Service;
}  // namespace protobuf

}  // namespace google

using namespace muduo;
using namespace muduo::net;

namespace zkclient {

class RpcChannel : public ::google::protobuf::RpcChannel {
  public:
  RpcChannel();

  explicit RpcChannel(const TcpConnectionPtr& conn);

  ~RpcChannel() override;

  void setConnection(const TcpConnectionPtr& conn) { conn_ = conn; }

  void setServices(
      const std::map<std::string, ::google::protobuf::Service*>* services) {
    services_ = services;
  }

  void CallMethod(const ::google::protobuf::MethodDescriptor* method,
                  ::google::protobuf::RpcController* controller,
                  const ::google::protobuf::Message* request,
                  ::google::protobuf::Message* response,
                  ::google::protobuf::Closure* done) override;

  void onMessage(const TcpConnectionPtr& conn, Buffer* buf,
                 Timestamp receiveTime);

  private:
  void onRpcMessage(const TcpConnectionPtr& conn,
                    const RpcMessagePtr& messagePtr, Timestamp receiveTime);

  void doneCallback(::google::protobuf::Message* response, int64_t id);

  struct OutstandingCall {
    ::google::protobuf::Message* response;
    ::google::protobuf::Closure* done;
  };
  RpcCodec codec_;
  TcpConnectionPtr conn_;
  std::atomic<int> id_;

  std::mutex mutex_;
  std::map<int64_t, OutstandingCall> outstandings_;

  const std::map<std::string, ::google::protobuf::Service*>* services_;
};
using RpcChannelPtr = std::shared_ptr<RpcChannel>;
}  // namespace zkclient
