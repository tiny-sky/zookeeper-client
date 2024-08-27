#include "worker.h"

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

std::string WorkCode2string(WorkerCode code) {
  if (code == Down) {
    return "Down";
  }
  if (code == False) {
    return "False";
  } else {
    return "Error";
  }
}

Worker::Worker(std::string name) : zkConnStr_(""), name_(name) {}

Worker::~Worker() {

  zkClient_->deleteNode("/workers/" + name_);
  zkClient_->deleteNode("/assign/" + name_);

  //释放zookeeper handle
  ZkClientManager::instance().destroyClient(zkClient_->getHandle());
  zkClient_.reset();
}

bool Worker::init(const std::string& zkConnStr) {
  zkutil::ZkErrorCode ec;

  zkConnStr_ = zkConnStr;
  //设置zookeeper日志路径
  if (ZkClientManager::setLogConf(true, "./zkWorker_log") == false) {
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

bool Worker::Register() {
  // 向Master注册
  std::string path = "/workers/" + name_;
  std::string value = path + ":2182";
  if (zkClient_->create(
          path, value,
          std::bind(&Worker::register_completion, this, _1, _2, _3, _4, _5),
          nullptr, true, false) == false) {
    std::cout << "create path failed! path: " << path << std::endl;
    return false;
  }

  // 创建任务接受路径，并监视
  path = "/assign/" + name_;
  if (zkClient_->create(
          path, "",
          std::bind(&Worker::register_completion, this, _1, _2, _3, _4, _5),
          nullptr, false, false) == false) {
    std::cout << "create path failed! path: " << path << std::endl;
    return false;
  }

  if (zkClient_->regChildWatcher(
          path, std::bind(&Worker::task_watcher, this, _1, _2, _3, _4, _5),
          nullptr) == false) {
    std::cout << "Watcher failed! path: " << path << std::endl;
    return false;
  }
}

void Worker::task_watcher(zkutil::ZkNotifyType type, const ZkClientPtr& client,
                          const std::string& path,
                          const std::vector<std::string>& childNodes,
                          void* context) {
  for (auto& task : childNodes) {
    // do something...
    std::cout << "Working for " << task << std::endl;

    // 直接标记成功
    std::string Path = path + "/" + task + "/status";
    zkClient_->create(
        Path, WorkCode2string(Down),
        std::bind(Worker::down_completion, this, _1, _2, _3, _4, _5), nullptr,
        false, false);
  }
}

void Worker::down_completion(zkutil::ZkErrorCode errcode,
                             const ZkClientPtr& client, const std::string& path,
                             const std::string& value, void* context) {
  switch (errcode) {
    case zkutil::kZKSucceed:
      std::cout << "Worker down..." << std::endl;
      break;
    case zkutil::kZKExisted:
      std::cout << "Task already down" << std::endl;
      break;
    default:
      std::cout << "Something went wrong when running for master" << std::endl;
  }
}

void Worker::register_completion(zkutil::ZkErrorCode errcode,
                                 const ZkClientPtr& client,
                                 const std::string& path,
                                 const std::string& value, void* context) {
  switch (errcode) {
    case zkutil::kZKSucceed:
      std::cout << "Worker register ok" << std::endl;
      break;
    case zkutil::kZKExisted:
      std::cout << "Worker Node already exists" << std::endl;
      break;
    default:
      std::cout << "Something went wrong when running for master" << std::endl;
  }
}