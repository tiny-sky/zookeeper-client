#include "ZkClient.h"
#include "ZkClientManager.h"
#include "ZkUtil.h"

#include <binders.h>
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

using namespace zkclient;

#define ZOOKEEPER_SERVER_CONN_STRING \
  "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"

using std::placeholders::_4;
using std::placeholders::_5;

class DistriBarrier : boost::noncopyable {
  public:
  DistriBarrier() : mutex_(), zkConnStr_(""), isInit_(false), barrNum_(0) {}

  ~DistriBarrier() {
    //删除创建的　所有子结点
    std::string childPath;
    while (childPaths_.empty() != true) {
      childPath = childPaths_.front();
      childPaths_.pop();
      zkutil::ZkErrorCode ec = zkClient_->deleteRecursive(childPath);
    }

    //释放zookeeper handle
    ZkClientManager::instance().destroyClient(zkClient_->getHandle());
    zkClient_.reset();
    isInit_ = false;
    barrNum_ = 0;
  }

  bool init(const std::string& zkConnStr, int barrier_num) {
    zkutil::ZkErrorCode ec;
    if (isInit_.load() == false) {
      barrNum_ = barrier_num;
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

      //通过session handle，获取ZkClient
      zkClient_ = ZkClientManager::instance().getZkClient(handle);

      // 清除旧数据
      ec = zkClient_->deleteNode(parentPath_);
      if (ec != zkutil::kZKSucceed && ec != zkutil::kZKNotExist) {
        std::cout << "delete parent path:" << parentPath_ << " failed!"
                  << std::endl;
        return false;
      }

      //创建父路径
      bool isTemp = false;
      bool isSeq = false;
      std::string retPath;
      char value[48] = {0};
      snprintf(value, sizeof(value), "%d", barrNum_);
      ec = zkClient_->create(parentPath_, value, isTemp, isSeq, retPath);
      if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
        std::cout << "create parent path:" << parentPath_ << " failed!"
                  << std::endl;
        return false;
      }

      if (ec == zkutil::kZKExisted) {
        zkClient_->set(parentPath_, value);
      }

      //注册 watcher
      if (zkClient_->regChildWatcher(
              parentPath_,
              std::bind(&DistriBarrier::regChildWatcher_cb, this, _1, _2, _3,
                        _4, _5),
              nullptr) == false) {
        printf("\n regChildWatcher failed! path:%s\n", parentPath_.c_str());
        return false;
      }
      isInit_ = true;
    }

    return true;
  }

  bool countdown() {
    std::string retPath;
    zkutil::ZkErrorCode ec = createChild(retPath);
    if (ec == zkutil::kZKError) {
      return false;
    }

    std::lock_guard<std::mutex> lock(childMutex_);
    childPaths_.push(retPath);
    return true;
  }

  bool wait() {
    if (isInit_ == false)
      return false;

    std::vector<std::string> childNodes;
    if (zkClient_->getChildren(parentPath_, childNodes) != zkutil::kZKSucceed) {
      std::cout << " getChildren failed! path:" << parentPath_ << std::endl;
      return false;
    }

    if (childNodes.size() >= barrNum_) {
      return true;
    } else {
      std::unique_lock<std::mutex> lock(mutex_);
      condition_.wait(lock);
    }
    return true;
  }

  private:
  void regChildWatcher_cb(zkutil::ZkNotifyType type, const ZkClientPtr& client,
                          const std::string& path,
                          const std::vector<std::string>& childNodes,
                          void* context) {
    if (childNodes.size() >= barrNum_) {
      std::cout << "[regChildWatcher_cb] notifyType:" << type
                << ", path:" << path
                << ", childNodes_size:" << childNodes.size() << std::endl;
      client->cancelRegChildWatcher(parentPath_);
      std::unique_lock<std::mutex> lock(mutex_);
      condition_.notify_all();
    }
  }

  zkutil::ZkErrorCode createChild(std::string& retPath) {
    std::string childPath = parentPath_ + "/" + childNodeName_;
    return zkClient_->create(childPath, "", true, true, retPath);
  }

  std::string zkConnStr_;
  ZkClientPtr zkClient_;
  std::atomic<bool> isInit_;
  const std::string parentPath_ = "/queue_barrier";
  const std::string childNodeName_ = "barrier";
  std::queue<std::string> childPaths_;
  std::mutex childMutex_;

  int barrNum_;
  std::mutex mutex_;
  std::condition_variable condition_;
};

void threadFun(DistriBarrier& bar, int i) {
  bar.countdown();
  std::cout << "[threadFun] countdown " << i << "ready" << std::endl;
  bar.wait();
  std::cout << "[threadFun] working " << i << "on" << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(3));
  std::cout << "[threadFun] working " << i << "down!" << std::endl;
}

int main() {
  DistriBarrier bar;
  std::vector<std::thread> threads;

  if (bar.init(ZOOKEEPER_SERVER_CONN_STRING, 10) == false) {
    std::cout << "DistriBarrier failed! " << std::endl;
    return 0;
  }

  for (int i = 0; i < 15; i++) {
    std::thread t(threadFun, std::ref(bar), i);
    threads.push_back(std::move(t));
  }

  for (auto& t : threads) {
    t.join();
  }

  std::cout << "distribarrier test ok!" << std::endl;

  return 0;
}