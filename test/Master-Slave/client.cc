#include "client.h"

#include "ZkClient.h"
#include "ZkClientManager.h"
#include "ZkUtil.h"

#include <binders.h>
#include <iostream>
#include <string>
#include <vector>

using namespace zkclient;

using std::placeholders::_4;
using std::placeholders::_5;
using std::placeholders::_6;

Client::Client() : zkConnStr_("") {}

Client::~Client() {
  ZkClientManager::instance().destroyClient(zkClient_->getHandle());
  zkClient_.reset();
}

bool Client::init(const std::string& zkConnStr) {
  zkutil::ZkErrorCode ec;

  zkConnStr_ = zkConnStr;
  //设置zookeeper日志路径
  if (ZkClientManager::setLogConf(true, "./zkClient_log") == false) {
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

  //通过session handle，获取ZkClient
  zkClient_ = ZkClientManager::instance().getZkClient(handle);

  return true;
}

bool Client::submit_task(std::string& task) {
  task_ = task;
  std::string path = "/tasks/" + task;
  if (zkClient_->create(
          path, task,
          std::bind(&Client::submitTask_completion, this, _1, _2, _3, _4, _5),
          nullptr, false, false) == false) {
    std::cout << "create path failed! path: /tasks/task" << std::endl;
    return false;
  }

  if (zkClient_->regNodeWatcher(
          path, std::bind(&Client::task_watcher, this, _1, _2, _3, _4, _5, _6),
          nullptr) == false) {
    std::cout << "Watcher failed! path: /tasks/" << task << std::endl;
    return false;
  }
}

void Client::submitTask_completion(zkutil::ZkErrorCode errcode,
                                   const ZkClientPtr& client,
                                   const std::string& path,
                                   const std::string& value, void* context) {
  switch (errcode) {
    case zkutil::kZKSucceed:
      std::cout << "submit task " << value << std::endl;
      break;
    case zkutil::kZKExisted:
      std::cout << path << " already exist" << std::endl;
      break;
    default:
      std::cout << "Something went wrong when running for master" << std::endl;
  }
}

void Client::task_watcher(zkutil::ZkNotifyType type, const ZkClientPtr& client,
                          const std::string& path, const std::string& value,
                          int32_t version, void* context) {
  const std::string path = path + "/status";
  std::string Value = " ";
  int32_t version;
  client->getNode(path, Value, version);
  status_ = Value;
  
  std::unique_lock<std::mutex> lock(mutex_);
  condition_.notify_one();
}

std::string Client::task_wait() {
  std::unique_lock<std::mutex> lock(mutex_);
  condition_.wait(lock);
  return status_;
}