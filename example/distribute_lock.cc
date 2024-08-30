#include "ZkClient.h"
#include "ZkClientManager.h"
#include "ZkUtil.h"

#include <binders.h>
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace zkclient;

#define ZOOKEEPER_SERVER_CONN_STRING \
  "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"

using std::placeholders::_4;
using std::placeholders::_5;
using std::placeholders::_6;

class DistriSharedLock : boost::noncopyable {
  public:
  DistriSharedLock() : mutex_(), zkConnStr_(""), isInit_(false),ready_(false), childNodeName_("") {}

  ~DistriSharedLock() {
    //删除结点
    std::string childPath = parentPath_ + "/" + childNodeName_;
    zkutil::ZkErrorCode ec = zkClient_->deleteRecursive(childPath);
    //释放zookeeper handle
    ZkClientManager::instance().destroyClient(zkClient_->getHandle());
    zkClient_.reset();
    isInit_ = false;
  };

  bool init(const std::string& zkConnStr, const std::string& childNodeName) {
    zkutil::ZkErrorCode ec;

    if (isInit_.load() == false) {
      childNodeName_ = childNodeName;
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

      ec = zkClient_->deleteNode(parentPath_);
      if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
        std::cout << "delete parent path:" << parentPath_ << " failed!"
                  << std::endl;
        return false;
      }

      //创建父路径
      bool isTemp = false;
      bool isSeq = false;
      std::string retPath;
      ec = zkClient_->create(parentPath_, "", isTemp, isSeq, retPath);
      if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
        std::cout << "create parent path:" << parentPath_ << " failed!"
                  << std::endl;
        return false;
      }

      isInit_ = true;
    }
    return true;
  }

  bool lock() {
    if (isInit_.load() == false) {
      return false;
    }

    zkutil::ZkErrorCode ec;

    std::string retPath;
    std::vector<std::string> childNodes;

    std::string Path = parentPath_ + "/" + childNodeName_;
    if (zkClient_->create(Path, "", true, true, retPath) !=
        zkutil::kZKSucceed) {
      std::cout << "create [" << Path << "] failed!" << std::endl;
      return false;
    }

    retpath_ = trimPath(retPath);

    if (zkClient_->getChildren(parentPath_, childNodes) != zkutil::kZKSucceed) {
      std::cout << "get [" << parentPath_ << "] failed!" << std::endl;
      return false;
    }

    // 如果只剩下一个节点则获得锁成功
    if (childNodes.size() == 1 && childNodes.front() == retpath_) {
      return true;
    }

    // 排序判断是否是第一个节点
    std::sort(childNodes.begin(), childNodes.end());
    if (childNodes.front() == retpath_) {
      return true;
    } else {
      // Watch 前面一个节点
      auto it = std::find(childNodes.begin(), childNodes.end(), retpath_);
      int index = std::distance(childNodes.begin(), it) - 1;

      std::string path = parentPath_ + '/' + childNodes.at(index);
      if (zkClient_->regNodeWatcher(path,
                                    std::bind(&DistriSharedLock::regWatcher_cb, this,
                                              _1, _2, _3, _4, _5, _6),
                                    nullptr) == false) {
        std::cout << "regWatcher failed! path:" << path << std::endl;
        return false;
      }
    }

    std::unique_lock<std::mutex> lock(mutex_);
    condition_.wait(lock,[this](){
      return ready_;
    });

    return true;
  }

  bool unlock() {
    //删除子结点
    string path = parentPath_ + '/' + retpath_;
    if (zkClient_->deleteNode(path) != zkutil::kZKSucceed) {
      return false;
    }
    return true;
  }

  private:
  void regWatcher_cb(zkutil::ZkNotifyType type, const ZkClientPtr& client,
                     const std::string& path, const std::string& value,
                     int32_t version, void* context) {

    std::cout << "[regWatcher_cb] notifyType:" << type << ", value:" << value
              << ", version:" << version << std::endl;
    if (type == zkutil::kNodeDelete) {
      std::unique_lock<std::mutex> lock(mutex_);
      ready_ = true;
      condition_.notify_one();
    }
  }

  std::string trimPath(const std::string& path) {
    std::size_t pos = path.rfind('/');
    if (pos != std::string::npos) {
      return path.substr(pos + 1);
    }
    return path;
  }

  std::string zkConnStr_;
  ZkClientPtr zkClient_;
  std::atomic<bool> isInit_;
  const std::string parentPath_ = "/exclusive_lock";
  std::string childNodeName_;

  std::string retpath_;
  std::mutex mutex_;
  bool ready_;
  std::condition_variable condition_;
};

void distriLocktest_1(DistriSharedLock& d1) {
  d1.lock();
  std::cout << "Get distriLock1 lock" << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(5));
  std::cout << "unlock distriLock1" << std::endl;
  d1.unlock();
}

void distriLocktest_2(DistriSharedLock& d2) {
  d2.lock();
  std::cout << "Get distriLock2 lock" << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(5));
  std::cout << "unlock distriLock2" << std::endl;
  d2.unlock();
}

int main() {
  DistriSharedLock d1;
  DistriSharedLock d2;

  if (d1.init(ZOOKEEPER_SERVER_CONN_STRING, "lock") == false) {
    std::cout << "distrilock_1 init lock failed!" << std::endl;
    return 0;
  }

  if (d2.init(ZOOKEEPER_SERVER_CONN_STRING, "lock") == false) {
    std::cout << "distrilock_2 init lock failed!" << std::endl;
    return 0;
  }

  std::thread t1(distriLocktest_1, std::ref(d1));
  std::thread t2(distriLocktest_2, std::ref(d2));

  t1.join();
  t2.join();

  std::cout << "distrilock test ok!" << std::endl;

  return 0;
}