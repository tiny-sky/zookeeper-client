#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <mutex>
#include <vector>
#include "ZkClient.h"
#include "ZkTimerQueue.h"
#include "callback.h"
#include "muduo/base/Singleton.h"
#include "muduo/base/Thread.h"
#include "muduo/base/noncopyable.h"

using namespace muduo;
using namespace muduo::net;

namespace zkclient {

class ZkClientManager : boost::noncopyable {
  public:
  ZkClientManager();

  ~ZkClientManager();

  ZkEventLoop* getFirstZkEventLoop() {
    if (zkThreadsmap_.empty() == false) {
      int pid = getFirstThreadId();
      return zkThreadsmap_[pid];
    }
    return nullptr;
  }

  ZkEventLoop* getSecondZkEventLoop() {
    if (zkThreadsmap_.empty() == false) {
      int pid = getSecondThreadId();
      return zkThreadsmap_[pid];
    }
    return nullptr;
  }

  ZkClientPtr getZkClient(uint32_t handle);
  ZkClientPtr __getZkClient(uint32_t handle);

  static bool setLogConf(bool isDebug, const std::string& zkLogFilePath = "");

  static ZkClientManager& instance() {
    return muduo::Singleton<ZkClientManager>::instance();
  };

  uint32_t createZkClient(const std::string& host, int timeout,
                          SessionClientId* clientId = nullptr,
                          SessionExpiredHandler expired_handler = nullptr,
                          void* context = nullptr);

  private:
  void init();
  void LoopFunc();

  int getFirstThreadId() {
    if (zkThreads_.empty() == false && zkThreads_[0] != nullptr) {
      return (zkThreads_[0])->tid();
    }
    return 0;
  }

  int getSecondThreadId() {
    if (zkThreads_.empty() == false && (zkThreads_.size() >= 2) &&
        zkThreads_[1] != NULL) {
      return (zkThreads_[1])->tid();
    }
    return 0;
  }

  std::vector<muduo::Thread*> zkThreads_;  // 运行定时器 和 runInThread 注册函数
  std::map<pid_t, ZkEventLoop*> zkThreadsmap_;
  std::mutex zkthreadMutex_;

  std::atomic<bool> isExit_;

  std::map<uint32_t, ZkClientPtr> totalZkClients_;
  std::mutex mutex_;

  uint32_t nextHandle_;
};
}  // namespace zkclient
