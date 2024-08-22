#include "ZkClientManager.h"
#include "CbFunManager.h"
#include "ZkClient.h"
#include "ZkUtil.h"

#include <sys/epoll.h>
#include <unistd.h>
#include <iostream>
#include <string>

#include "zookeeper/zookeeper.h"

#include "muduo/base/AsyncLogging.h"
#include "muduo/base/CurrentThread.h"
#include "muduo/base/Logging.h"
#include "muduo/base/Thread.h"

#include "ZkEventLoop.h"
#include "callback.h"

namespace zkclient {

#define NET_THREAD_NUM 2

muduo::AsyncLogging* gpAsyncLog = nullptr;

static bool IsDebugLogLevel = true;
static std::string ZkLogPath = "";
static FILE* ZkLogFd = nullptr;

ZkClientManager::ZkClientManager() {
  nextHandle_ = 1;
  zkThreads_.clear();
  isExit_ = false;
  totalZkClients_.clear();

  init();
}

ZkClientManager::~ZkClientManager() {
  isExit_ = true;

  std::vector<muduo::Thread*>::iterator iter = zkThreads_.begin();
  for (; iter != zkThreads_.end(); iter++) {
    if ((*iter) != NULL) {
      (*iter)->join();
      delete (*iter);
    }
  }
  zkThreads_.clear();
  zkThreadsmap_.clear();
  totalZkClients_.clear();
}

void ZkClientManager::init() {

  //初始化日志线程
  gpAsyncLog = new muduo::AsyncLogging("zk_client", 50 * 1000 * 1000, 3);
  muduo::Logger::setLogLevel(IsDebugLogLevel ? muduo::Logger::DEBUG
                                             : muduo::Logger::WARN);
  gpAsyncLog->start();

  /**
   * 创建 多个线程
   * 线程1：定时(1ms~10ms)检查zookeeper session是否超时。
   * 线程2：
   *        1）如果session失效，则需要定时重连zookeeper server
   *        2）触发watcher后，需重复注册watcher(用了阻塞的方式注册watch,为了避免阻塞原线程，故用另外的线程来注册)
   *        3）与zookeeper server重连成功后，需要 重新注册watcher。
   *        4）创建叶子结点时，如果分支结点不存在，需要递归创建目录结点
   *        5）删除分支结点时，如果含有叶子结点，需要递归删除所有叶子结点
   */
  char threadName[48] = {0};
  for (int i = 1; i <= NET_THREAD_NUM; i++) {
    snprintf(threadName, 48, "zk_thread_%d", i);
    muduo::Thread* pThreadHandle = new muduo::Thread(
        std::bind(&ZkClientManager::LoopFunc, this), threadName);
    if (pThreadHandle != nullptr) {
      zkThreads_.push_back(pThreadHandle);
    }
  }

  for (std::vector<muduo::Thread*>::iterator iter = zkThreads_.begin();
       iter != zkThreads_.end(); iter++) {
    if ((*iter) != nullptr) {
      (*iter)->start();
    }
  }
}

void ZkClientManager::LoopFunc() {
  ZkEventLoop loop;

  std::unique_lock<std::mutex> lock(zkthreadMutex_);
  zkThreadsmap_[muduo::CurrentThread::tid()] = &loop;
  lock.unlock();

  loop.loop();
  LOG_WARN << "thread exit. thread id:" << muduo::CurrentThread::tid();
}

uint32_t ZkClientManager::createZkClient(
    const std::string& host, int timeout,
    SessionClientId* clientId /*= nullptr*/,
    SessionExpiredHandler expired_handler /*= nullptr*/,
    void* context /*= nullptr*/) {
  nextHandle_++;
  if (nextHandle_ == 0) {
    nextHandle_++;
  }

  ZkClientPtr client = std::make_shared<ZkClient>(nextHandle_);
  std::unique_lock<std::mutex> lock(mutex_);
  totalZkClients_[nextHandle_] = client;
  lock.unlock();

  if (client->init(host, timeout, clientId, expired_handler, context) ==
      false) {
    LOG_ERROR << "zkclient init failed! handle:" << nextHandle_
              << ", host:" << host << ", timeout:" << timeout;
    return 0;
  }

  LOG_INFO << "zkclient init...";
  return nextHandle_;
}

ZkClientPtr ZkClientManager::getZkClient(uint32_t handle) {
  ZkClientPtr client;
  std::unique_lock<std::mutex> lock(mutex_);
  if (totalZkClients_.find(handle) != totalZkClients_.end()) {
    if (totalZkClients_[handle]->isInit()) {
      client = totalZkClients_[handle];
    }
  } else {
    LOG_WARN << "Can't find this zkclient! handle:" << handle;
  }
  lock.unlock();

  return client;
}

ZkClientPtr ZkClientManager::__getZkClient(uint32_t handle) {
  ZkClientPtr client;
  std::unique_lock<std::mutex> lock(mutex_);
  if (totalZkClients_.find(handle) != totalZkClients_.end()) {
    client = totalZkClients_[handle];
  } else {
    LOG_WARN << "Can't find this zkclient! handle:" << handle;
  }
  lock.unlock();

  return client;
}

bool ZkClientManager::setLogConf(bool isDebug,
                                 const std::string& zkLogFilePath) {
  IsDebugLogLevel = isDebug;

  ZooLogLevel log_level = isDebug ? ZOO_LOG_LEVEL_DEBUG : ZOO_LOG_LEVEL_INFO;
  zoo_set_debug_level(log_level);
  std::cout << "[ZkClientManager::setLogConf] isDebugLogLevel:" << isDebug
            << std::endl;
  // log目录
  if (zkLogFilePath != "") {
    std::cout << "[ZkClientManager::setLogConf] log_path:" << zkLogFilePath
              << std::endl;
    ZkLogFd = fopen(zkLogFilePath.c_str(), "w");
    if (!ZkLogFd) {
      std::cout
          << "[ZkClientManager::setLogConf] Can't open this log path. path "
          << zkLogFilePath << std::endl;
      return false;
    }
    zoo_set_log_stream(ZkLogFd);
  }
  return true;
}

}  // namespace zkclient
