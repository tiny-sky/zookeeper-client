#include "master.h"

#include "ZkClient.h"
#include "ZkClientManager.h"
#include "ZkUtil.h"

#include <binders.h>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

using namespace zkclient;

using std::placeholders::_4;
using std::placeholders::_5;
using std::placeholders::_6;

std::string getNotifyTypeStr(zkutil::ZkNotifyType type) {
  if (zkutil::kNodeDelete == type) {
    return "zkutil::kNodeDelete";
  } else if (zkutil::kNodeCreate == type) {
    return "zkutil::kNodeCreate";
  } else if (zkutil::kNodeChange == type) {
    return "zkutil::kNodeChange";
  } else if (zkutil::kGetNodeValueFailed == type) {
    return "zkutil::kGetNodeValueFailed";
  } else if (zkutil::kChildChange == type) {
    return "zkutil::kChildChange";
  } else if (zkutil::kGetChildListFailed == type) {
    return "zkutil::kGetChildListFailed";
  } else if (zkutil::kTypeError == type) {
    return "zkutil::kTypeError";
  } else if (zkutil::kGetNodeValueFailed_NodeNotExist == type) {
    return "zkutil::kGetNodeValueFailed_NodeNotExist";
  } else if (zkutil::kGetChildListFailed_ParentNotExist == type) {
    return "zkutil::kGetChildListFailed_ParentNotExist";
  }
  return "";
}

void printChild(const std::string& path,
                const std::vector<std::string>& childnodes) {
  std::cout << "\n\n-----------[printChild] ParentPath: " << path
            << ", child size:" << childnodes.size() << "-------------- "
            << std::endl;

  for (const auto& child : childnodes) {
    std::cout << "child name:" << child << std::endl;
  }

  std::cout << "---------------------------------------------------------------"
               "----- "
            << std::endl;
}

Master::Master() : zkConnStr_(""), serverid_(0) {}

Master::~Master() {
  for (auto& path : workers) {
    zkClient_->deleteRecursive(path);
  }

  for (auto& path : tasks) {
    zkClient_->deleteRecursive(path);
  }

  //释放zookeeper handle
  ZkClientManager::instance().destroyClient(zkClient_->getHandle());
  zkClient_.reset();
}

bool Master::init(const std::string& zkConnStr) {
  zkutil::ZkErrorCode ec;

  zkConnStr_ = zkConnStr;
  //设置zookeeper日志路径
  if (ZkClientManager::setLogConf(true, "./zkMaster_log") == false) {
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
  if (zkClient_->create("/workers", "",
                        std::bind(&Master::create_parent_completion, this, _1,
                                  _2, _3, _4, _5),
                        nullptr, false, false) == false) {
    std::cout << "create path failed! path: /workers" << std::endl;
    return false;
  }
  if (zkClient_->create("/assign", "",
                        std::bind(&Master::create_parent_completion, this, _1,
                                  _2, _3, _4, _5),
                        nullptr, false, false) == false) {
    std::cout << "create path failed! path: /assign" << std::endl;
    return false;
  }
  if (zkClient_->create("/status", "",
                        std::bind(&Master::create_parent_completion, this, _1,
                                  _2, _3, _4, _5),
                        nullptr, false, false) == false) {
    std::cout << "create path failed! path: /status" << std::endl;
    return false;
  }

  return true;
}

bool Master::run_for_master() {
  char server_id_string[9];
  snprintf(server_id_string, 9, "%x", serverid_);

  if (zkClient_->create("/master", server_id_string,
                        std::bind(&Master::master_create_completion, this, _1,
                                  _2, _3, _4, _5),
                        nullptr, true, false) == false) {
    std::cout << "create path failed! path: /master" << std::endl;
    return false;
  }
}

void Master::create_parent_completion(zkutil::ZkErrorCode errcode,
                                      const ZkClientPtr& client,
                                      const std::string& path,
                                      const std::string& value, void* context) {
  switch (errcode) {
    case zkutil::kZKSucceed:
      std::cout << "Created parent node" << std::endl;
      break;
    case zkutil::kZKExisted:
      std::cout << "Node already exists" << std::endl;
      break;
    default:
      std::cout << "Something went wrong when running for master" << std::endl;
  }
}

void Master::master_create_completion(zkutil::ZkErrorCode errcode,
                                      const ZkClientPtr& client,
                                      const std::string& path,
                                      const std::string& value, void* context) {

  switch (errcode) {
    case zkutil::kZKLostConnection:
      check_master();
      break;
    case zkutil::kZKSucceed:
      take_leadership();
      break;
    case zkutil::kZKExisted:
      master_exists();
      break;
    default:
      std::cout << "Something went wrong when running for master." << std::endl;
      break;
  }
}

void Master::check_master() {
  zkClient_->getNode(
      "/master",
      std::bind(&Master::master_check_completion, this, _1, _2, _3, _4, _5, _6),
      nullptr);
}

void Master::master_check_completion(zkutil::ZkErrorCode errcode,
                                     const ZkClientPtr& client,
                                     const std::string& path,
                                     const std::string& value, int32_t version,
                                     void* context) {
  int master_id;
  switch (errcode) {
    case zkutil::kZKLostConnection:
      check_master();
      break;
    case zkutil::kZKSucceed:
      master_id = std::stoi(value);
      if (master_id == serverid_) {
        take_leadership();
        std::cout << "Elected primary master" << std::endl;
      } else {
        master_exists();
        std::cout << "The primary is some other process" << std::endl;
      }
      break;

    case zkutil::kZKNotExist:
      run_for_master();
      break;

    default:
      std::cout << "Something went wrong when checking the master lock"
                << std::endl;
      break;
  }
}

void Master::take_leadership() {
  get_workers();
}

void Master::get_workers() {
  zkClient_->getChildren(
      "/workers",
      std::bind(&Master::workers_completion, this, _1, _2, _3, _4, _5),
      nullptr);

  zkClient_->regChildWatcher(
      "/workers", std::bind(&Master::workers_watcher, this, _1, _2, _3, _4, _5),
      nullptr);
}

void Master::workers_completion(zkutil::ZkErrorCode errcode,
                                const ZkClientPtr& client,
                                const std::string& path,
                                const std::vector<std::string>& childNodes,
                                void* context) {
  switch (errcode) {
    case zkutil::kZKLostConnection:
      get_workers();
      break;
    case zkutil::kZKSucceed:
      set_workers(childNodes, workers);
      get_tasks();
      break;
    default:
      std::cout << "Something went wrong when checking workers" << std::endl;
  }
}

void Master::get_tasks() {
  zkClient_->getChildren(
      "/tasks", std::bind(&Master::tasks_completion, this, _1, _2, _3, _4, _5),
      nullptr);
  zkClient_->regChildWatcher(
      "/tasks", std::bind(&Master::tasks_watcher, this, _1, _2, _3, _4, _5),
      nullptr);
}

void Master::get_task_data(std::string& task) {
  std::cout << "Task path :" << task << std::endl;

  std::string path = "/tasks/" + task;

  zkClient_->getNode(path,
                     std::bind(&Master::get_task_data_completion, this, _1, _2,
                               _3, _4, _5, _6),
                     static_cast<void*>(&task[0]));
}

void Master::task_assignment(task_info* task) {
  //Add task to worker list
  std::string path = "/assign/" + task->worker + "/" + task->name;
  zkClient_->create(
      path, task->value,
      std::bind(&Master::task_assignment_completion, this, _1, _2, _3, _4, _5),
      static_cast<void*>(task), false, false);
}

void Master::task_assignment_completion(zkutil::ZkErrorCode errcode,
                                        const ZkClientPtr& client,
                                        const std::string& path,
                                        const std::string& value,
                                        void* context) {
  task_info* task = static_cast<task_info*>(context);
  switch (errcode) {
    case zkutil::kZKLostConnection:
      task_assignment(task);
      break;
    case zkutil::kZKSucceed:
      if (context != nullptr) {
        //Delete task from list of pending
        std::cout << "Deleting pending task :" << task->name << std::endl;
        std::string del_path = "/tasks/" + task->name;
        zkClient_->deleteNode(del_path);

        std::string path = "/workers/" + task->worker;
        sub_worker_load(path);
      }

      break;
    case zkutil::kZKExisted:
      std::cout << "Assignment has already been created: " << value
                << std::endl;
      break;
    default:
      std::cout << "Something went wrong when running for master." << std::endl;
      break;
  }
}

void Master::get_task_data_completion(zkutil::ZkErrorCode errcode,
                                      const ZkClientPtr& client,
                                      const std::string& path,
                                      const std::string& value, int32_t version,
                                      void* context) {
  std::string task = *static_cast<std::string*>(context);
  switch (errcode) {
    case zkutil::kZKLostConnection:
      get_task_data(task);
      break;
    case zkutil::kZKSucceed:
      std::cout << "Choosing worker for task : " << task << std::endl;
      if (!workers.empty()) {
        // Choose worker
        int worker_index = chooseworker();
        std::cout << "chose worker :" << worker_index << std::endl;

        // Assign task to worker
        task_info* new_task = new task_info(task, value, workers[worker_index]);
        task_assignment(new_task);
      }

    default:
      std::cout << "Something went wrong when checking the master lock"
                << std::endl;
      break;
  }
}

int Master::chooseworker() {
  int workerindex = 0;
  int maxnumber = std::numeric_limits<int>::max();

  for (int i = 0; i < workers.size(); i++) {
    std::string path = "/workers/" + workers[i];
    std::string value = "";
    int32_t version;
    if (zkClient_->getNode(path, value, version) != zkutil::kZKSucceed) {
      continue;
    }

    int number = std::stoi(value);
    if (number > maxnumber) {
      workerindex = i;
      maxnumber = number;
    }
  }

  std::string path = "/workers/" + workers[workerindex];
  std::string value = std::to_string(maxnumber + 1);
  zkClient_->set(path, value);
  return workerindex;
}

void Master::tasks_completion(zkutil::ZkErrorCode errcode,
                              const ZkClientPtr& client,
                              const std::string& path,
                              const std::vector<std::string>& childNodes,
                              void* context) {
  switch (errcode) {
    case zkutil::kZKLostConnection:
      get_tasks();
      break;
    case zkutil::kZKSucceed: {
      std::vector<std::string> tmp_tasks = added_and_set(childNodes, tasks);
      assign_tasks(tmp_tasks);
      break;
    }
    default:
      std::cout << "Something went wrong when checking workers" << std::endl;
      break;
  }
}

void Master::master_exists() {

  zkClient_->isExist(
      "/master",
      std::bind(&Master::master_exists_completion, this, _1, _2, _3, _4),
      nullptr);
  zkClient_->regNodeWatcher(
      "/workers",
      std::bind(&Master::master_exists_watcher, this, _1, _2, _3, _4, _5, _6),
      nullptr);
}

void Master::master_exists_completion(zkutil::ZkErrorCode errcode,
                                      const ZkClientPtr& client,
                                      const std::string& path, void* context) {
  switch (errcode) {
    case zkutil::kZKLostConnection:
      master_exists();
      break;
    case zkutil::kZKSucceed:
      break;
    case zkutil::kZKNotExist:
      std::cout << "Previous master is gone, running for master" << std::endl;
      run_for_master();
      break;
    default:
      std::cout << "Something went wrong when executing exists" << std::endl;
      break;
  }
}

void Master::master_exists_watcher(zkutil::ZkNotifyType type,
                                   const ZkClientPtr& client,
                                   const std::string& path,
                                   const std::string& value, int32_t version,
                                   void* context) {
  //TODO
}

void Master::workers_watcher(zkutil::ZkNotifyType type,
                             const ZkClientPtr& client, const std::string& path,
                             const std::vector<std::string>& childNodes,
                             void* context) {
  std::cout << "[workers_watcher] notifyType :" << type << ","
            << getNotifyTypeStr(type) << ", parentPath:" << path << std::endl;
  printChild(path, childNodes);
}

void Master::tasks_watcher(zkutil::ZkNotifyType type, const ZkClientPtr& client,
                           const std::string& path,
                           const std::vector<std::string>& childNodes,
                           void* context) {
  std::cout << "[tasks_watcher] notifyType :" << type << ","
            << getNotifyTypeStr(type) << ", parentPath:" << path << std::endl;
  printChild(path, childNodes);
}

void Master::set_workers(const std::vector<std::string>& current,
                         std::vector<std::string>& previous) {
  previous = std::move(current);
}

std::vector<std::string> Master::added_and_set(
    const std::vector<std::string>& current,
    std::vector<std::string>& previous) {
  std::vector<std::string> diff;

  for (const auto& str : current) {
    if (std::find(previous.begin(), previous.end(), str) == previous.end()) {
      diff.push_back(str);
    }
  }

  previous = current;

  return diff;
}

void Master::assign_tasks(std::vector<std::string>& strings) {
  //  For each task, assign it to a worker.
  for (auto& data : strings) {
    std::cout << "Assigning task :" << data << std::endl;
    get_task_data(data);
  }
}

void Master::sub_worker_load(std::string path) {
  std::string value = "";
  int32_t version;

  if (zkClient_->getNode(path, value, version) != zkutil::kZKSucceed) {
    return;
  }

  std::string load = std::to_string(std::stoi(value) - 1);
  zkClient_->set(path, load);
}
