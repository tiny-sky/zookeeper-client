#pragma once

#include <callback.h>
#include <string.h>
#include <zookeeper/zookeeper.h>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

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

//所有回调（操作、Watcher）都用这一个数据结构作为上下文
//通常是从 Zookeeper收到回调（context* 用这个数据结构）后，从ZKWatchContext中取出要用到的参数，再 回调 用户定义的函数
struct ZkOperateAndWatchContext {
  ZkOperateAndWatchContext(const std::string& path, void* context,
                           ZkClientPtr zkclient);

  void* context_;
  std::string path_;
  ZkClientPtr zkclient_;

  GetNodeHandler getnode_handler_;
  GetChildrenHandler getchildren_handler_;
  ExistHandler exist_handler_;
  CreateHandler create_handler_;
  SetHandler set_handler_;
  DeleteHandler delete_handler_;
  NodeChangeHandler node_notify_handler_;
  ChildChangeHandler child_notify_handler_;
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
  bool getClientId(SessionClientId& cliId);

  bool isConnected() { return getSessStat() == ZOO_CONNECTED_STATE; };
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

  public:
  // 对外服务接口

  /* async operation api */
  // 返回false，操作失败；返回true，有可能成功（要根据回调handler返回的rc参数确定是否成功）.
  bool getNode(const std::string& path, GetNodeHandler handler, void* context);
  bool getChildren(const std::string& path, GetChildrenHandler handler,
                   void* context);
  //存在: kZKSucceed, 不存在: kZKNotExist 其它错误：kZKError
  bool isExist(const std::string& path, ExistHandler handler, void* context);

  //创建结点的类型（默认持久型非顺序型，isTemp 临时型，isSequence 顺序型）
  bool create(const std::string& path, const std::string& value,
              CreateHandler handler, void* context, bool isTemp = false,
              bool isSequence = false);
  bool createIfNeedCreateParents(const std::string& path,
                                 const std::string& value,
                                 CreateHandler handler, void* context,
                                 bool isTemp = false, bool isSequence = false);

  //如果设置version，对指定版本的结点set操作 会是CAS操作；否则，默认是设置结点的最新版本的值(version: -1)
  bool set(const std::string& path, const std::string& value,
           SetHandler handler, void* context, int32_t version = -1);

  bool deleteNode(const std::string& path, DeleteHandler handler, void* context,
                  int32_t version = -1);
  bool deleteRecursive(const std::string& path, DeleteHandler handler,
                       void* context, int32_t version = -1);

  /* sync operation api */
  zkutil::ZkErrorCode getNode(const std::string& path, std::string& value,
                              int32_t& version);
  zkutil::ZkErrorCode getChildren(const std::string& path,
                                  std::vector<std::string>& childNodes);
  //存在: kZKSucceed, 不存在: kZKNotExist 其它错误：kZKError
  zkutil::ZkErrorCode isExist(const std::string& path);
  zkutil::ZkErrorCode create(const std::string& path, const std::string& value,
                             bool isTemp /*= false*/,
                             bool isSequence /*= false*/, std::string& retPath);
  //创建时，如果路径的分支结点不存在，则会先创建分支结点，再创建叶子结点。（注：分支结点必须是 持久型的）
  zkutil::ZkErrorCode createIfNeedCreateParents(const std::string& path,
                                                const std::string& value,
                                                bool isTemp /*= false*/,
                                                bool isSequence /*= false*/,
                                                std::string& retPath);
  zkutil::ZkErrorCode set(const std::string& path, const std::string& value,
                          int32_t version = -1);
  zkutil::ZkErrorCode deleteNode(const std::string& path, int32_t version = -1);
  zkutil::ZkErrorCode deleteRecursive(const std::string& path,
                                      int32_t version = -1);

  private:
  // current time
  int64_t getCurrentMs();

  static void createCompletion(int rc, const char* value, const void* data);

  // session watch callback
  static void sessionWatcher(zhandle_t* zh, int type, int state,
                             const char* path, void* watcher_ctx);
  static void checkSessionState(uint32_t handle);
  static std::string getSessStatStr(int stat);

  bool createPersistentDir(const std::string& path);
  zkutil::ZkErrorCode createPersistentDirNode(const std::string& path);

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

  std::atomic<int> sessionState_;  // zk 会话状态
  std::mutex stateMutex_;
  std::condition_variable stateCondition_;
  std::mutex sessStateMutex_;

  std::atomic<int> sessionTimeout_;  //  会话超时时间
  std::mutex sessTimeoutMutex_;

  std::atomic<int64_t> sessionDisconnectMs_;  // 会话异常时间点
  std::mutex sessDisconnMutex_;

  void* userContext_;

  SessionExpiredHandler expiredHandler_;
};
}  // namespace zkclient
