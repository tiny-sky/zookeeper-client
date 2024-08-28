#include "RpcChannel.h"

#include <google/protobuf/descriptor.h>
#include <functional>
#include "muduo/base/Logging.h"
#include "muduo/net/protorpc/rpc.pb.h"

using namespace muduo;
using namespace muduo::net;

namespace zkclient {

using std::placeholders::_1;
using std::placeholders::_2;
using std::placeholders::_3;

RpcChannel::RpcChannel()
    : codec_(std::bind(&RpcChannel::onRpcMessage, this, _1, _2, _3)),
      services_(nullptr) {}

RpcChannel::RpcChannel(const TcpConnectionPtr& conn)
    : codec_(std::bind(&RpcChannel::onRpcMessage, this, _1, _2, _3)),
      conn_(conn),
      services_(nullptr) {}

RpcChannel::~RpcChannel() {
  for (const auto& outstanding : outstandings_) {
    OutstandingCall out = outstanding.second;
    delete out.response;
    delete out.done;
  }
}

// send request
void RpcChannel::CallMethod(const ::google::protobuf::MethodDescriptor* method,
                            google::protobuf::RpcController* controller,
                            const ::google::protobuf::Message* request,
                            ::google::protobuf::Message* response,
                            ::google::protobuf::Closure* done) {
  (void)controller;  // TODO

  RpcMessage message;
  message.set_type(REQUEST);
  int id = id_.fetch_add(1, std::memory_order_seq_cst) + 1;
  message.set_id(id);
  message.set_service(method->service()->full_name());
  message.set_method(method->name());
  message.set_request(request->SerializeAsString());

  OutstandingCall out = {response, done};
  {
    std::lock_guard<std::mutex> guard(mutex_);
    outstandings_[id] = out;
  }
  codec_.send(conn_, message);
}

void RpcChannel::onMessage(const TcpConnectionPtr& conn, Buffer* buf,
                           Timestamp receiveTime) {
  codec_.onMessage(conn, buf, receiveTime);
}

void RpcChannel::onRpcMessage(const TcpConnectionPtr& conn,
                              const RpcMessagePtr& messagePtr,
                              Timestamp receiveTime) {
  (void)receiveTime;  //TODO

  assert(conn == conn_);
  RpcMessage& message = *messagePtr;
  if (message.type() == RESPONSE) {
    LOG_INFO << "RESPONSE Message\n";
    int64_t id = message.id();
    assert(message.has_response() || message.has_error());

    OutstandingCall out = {nullptr, nullptr};

    {
      std::lock_guard<std::mutex> guard(mutex_);
      std::map<int64_t, OutstandingCall>::iterator it = outstandings_.find(id);
      if (it != outstandings_.end()) {
        out = it->second;
        outstandings_.erase(it);
      }
    }

    if (out.response) {
      std::unique_ptr<google::protobuf::Message> d(out.response);
      if (message.has_response()) {
        out.response->ParseFromString(message.response());
      }
      if (out.done) {
        out.done->Run();
      }
    }
  } else if (message.type() == REQUEST) {
    LOG_INFO << "REQUEST Message\n";
    ErrorCode error = WRONG_PROTO;
    if (services_) {
      std::map<std::string, google::protobuf::Service*>::const_iterator it =
          services_->find(message.service());
      if (it != services_->end()) {
        google::protobuf::Service* service = it->second;
        const google::protobuf::ServiceDescriptor* desc =
            service->GetDescriptor();
        const google::protobuf::MethodDescriptor* method =
            desc->FindMethodByName(message.method());
        if (method) {
          std::unique_ptr<google::protobuf::Message> request(
              service->GetRequestPrototype(method).New());
          if (request->ParseFromString(message.request())) {
            google::protobuf::Message* response =
                service->GetResponsePrototype(method).New();
            int64_t id = message.id();
            service->CallMethod(
                method, nullptr, request.get(), response,
                NewCallback(this, &RpcChannel::doneCallback, response, id));
            error = NO_ERROR;
          } else {
            error = INVALID_REQUEST;
          }
        } else {
          error = NO_METHOD;
        }
      } else {
        error = NO_SERVICE;
      }
    } else {
      error = NO_SERVICE;
    }
    if (error != NO_ERROR) {
      RpcMessage response;
      response.set_type(RESPONSE);
      response.set_id(message.id());
      response.set_error(error);
      codec_.send(conn_, response);
    }
  }
}

void RpcChannel::doneCallback(::google::protobuf::Message* response,
                              int64_t id) {
  std::unique_ptr<google::protobuf::Message> d(response);
  RpcMessage message;
  message.set_type(RESPONSE);
  message.set_id(id);
  LOG_DEBUG << "response->SerializeAsString() -> "
            << response->SerializeAsString();
  message.set_response(response->SerializeAsString());
  codec_.send(conn_, message);
}
}  // namespace zkclient
