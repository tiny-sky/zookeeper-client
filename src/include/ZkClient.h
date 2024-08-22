#pragma once

#include <string.h>
#include <string>
#include <zookeeper/zookeeper.h>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <callback.h>

#include "muduo/base/noncopyable.h"

namespace zkclient {

struct SessionClientId {
  int64_t client_id;
  char passwd[16];

  SessionClientId() { memset(this, 0, sizeof(SessionClientId)); }
};

struct ZkZooInitCbData {
  ZkZooInitCbData(uint32_t handle) { handle_ = handle; }
  uint32_t handle_;
};

//管理session的状态，提供各个操作接口的实现
class ZkClient : muduo::noncopyable,
                 public std::enable_shared_from_this<ZkClient> {
  public:

  ZkClient(uint32_t handle);
  bool init(const std::string& host, int timeout,
            SessionClientId* clientId = nullptr,
            SessionExpiredHandler expired_handler = nullptr,
            void* context = nullptr);

  std::mutex& getStateMutex() { return stateMutex_; };
  std::condition_variable& getStateCondition() { return stateCondition_; };

  uint32_t getHandle() { return handle_; };
  void* getContext() { return userContext_; };
  bool isSupportReconnect() { return isSupportReconnect_; };

  bool isInit() { return isInitialized_; };
  void setIsInit(bool isInited) { isInitialized_ = isInited; };

  // session
  SessionExpiredHandler& getExpireHandler() { return expiredHandler_; };

  int getSessStat();
  void setSessStat(int stat);

  int getRetryDelay() { return retryDelay_; };
  void setRetryDelay(int delay) { retryDelay_ = delay; };

  bool isRetrying() { return isRetrying_; };
  void setIsRetrying(bool retrying) { isRetrying_ = retrying; };

  bool hasCallTimeoutFun() { return hasCallTimeoutFun_; };
  void setHasCallTimeoutFun(bool isCall) { hasCallTimeoutFun_ = isCall; };

  int getSessTimeout();
  void setSessTimeout(int time);

  int64_t getSessDisconn();
  void setSessDisconn(int64_t disconn);

  private:
  // current time
  int64_t getCurrentMs();

  // session watch callback
  static void sessionWatcher(zhandle_t* zh, int type, int state,
                             const char* path, void* watcher_ctx);
  static void checkSessionState(uint32_t handle);
  static std::string getSessStatStr(int stat);

  // Connection
  bool reconnect();
  static void retry(uint32_t handle);

  uint32_t handle_;

  std::atomic<bool> isInitialized_;  // 是否初始化
  std::atomic<bool> isRetrying_;     // 重试状态
  std::atomic<bool> hasCallTimeoutFun_;
  std::atomic<bool> retryDelay_;
  std::atomic<bool> isSupportReconnect_;

  std::string host_;
  zhandle_t* zhandle_;

  // Zk会话状态
  SessionClientId* clientId_;

  std::atomic<int> sessionState_; // zk 会话状态
  std::mutex stateMutex_;
  std::condition_variable stateCondition_;
  std::mutex sessStateMutex_;

  std::atomic<int> sessionTimeout_; //  会话超时时间
  std::mutex sessTimeoutMutex_;

  std::atomic<int64_t> sessionDisconnectMs_; // 会话异常时间点
  std::mutex sessDisconnMutex_;

  void* userContext_;

  SessionExpiredHandler expiredHandler_;
};
}  // namespace zkclient
