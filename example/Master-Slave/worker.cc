#include "worker.h"

#include "ZkClient.h"
#include "ZkClientManager.h"
#include "ZkUtil.h"

#include <binders.h>
#include <iostream>
#include <string>
#include <vector>

#include <fstream>
#include <nlohmann/json.hpp>

using namespace zkclient;
using json = nlohmann::json;

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

Worker::Worker(std::string ip) : zkConnStr_(""), ip_(ip) {}

Worker::~Worker() {

  zkClient_->deleteNode("/workers/" + ip_);
  zkClient_->deleteNode("/assign/" + ip_);

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

  if (!getconfig()) {
    std::cout << "Get db_config failed!" << std::endl;
    return false;
  }

  return true;
}

bool Worker::Register() {
  // 向Master注册
  std::string path = "/workers/" + ip_;
  if (zkClient_->create(
          path, "0",
          std::bind(&Worker::register_completion, this, _1, _2, _3, _4, _5),
          nullptr, true, false) == false) {
    std::cout << "create path failed! path: " << path << std::endl;
    return false;
  }

  // 创建任务接受路径，并监视
  path = "/assign/" + ip_;
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
    std::cout << "Child watcher failed! path: " << path << std::endl;
    return false;
  }

  // Watch db_config.json
  if (zkClient_->regNodeWatcher(
          configpath_,
          std::bind(&Worker::config_watcher, this, _1, _2, _3, _4, _5, _6),
          nullptr)) {
    std::cout << "Config watcher failed! path: " << path << std::endl;
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

void Worker::config_watcher(zkutil::ZkNotifyType type,
                            const ZkClientPtr& client, const std::string& path,
                            const std::string& value, int32_t version,
                            void* context) {
  // config 发生改变应该更新重新获取
  json config = json::parse(value);

  std::ofstream file("worker_db_config.json");

  if (!file.is_open()) {
    std::cout << "update db_config failed!" << std::endl;
    return ;
  }

  file << config.dump(4);
  file.close();
}

bool Worker::getconfig() {
  std::string value = "";
  int32_t version;
  if(zkClient_->getNode(configpath_,value,version) != zkutil::kZKSucceed) {
    return false;
  }

  json config = json::parse(value);

  std::ofstream file("worker_db_config.json");

  if (!file.is_open()) {
    std::cout << "Get db_config failed!" << std::endl;
    return false;
  }

  file << config.dump(4);
  file.close();
}