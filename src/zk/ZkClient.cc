#include "ZkClient.h"
#include <assert.h>
#include <time.h>
#include <iostream>
#include <string>
#include "ZkClientManager.h"
#include "ZkEventLoop.h"
#include "ZkUtil.h"
#include "ZkUtilClass.h"
#include "callback.h"

#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper.jute.h>

#include "muduo/base/Logging.h"
#include "muduo/net/TimerId.h"

#include <boost/bind.hpp>

namespace zkclient {

ZkOperateAndWatchContext::ZkOperateAndWatchContext(const std::string& path,
                                                   void* context,
                                                   ZkClientPtr zkclient) {
  this->path_ = path;
  this->context_ = context;
  this->zkclient_ = zkclient;
}

void ZkClient::sessionWatcher(zhandle_t* zh, int type, int state,
                              const char* path, void* watcher_ctx) {
  if (type == ZOO_SESSION_EVENT) {
    ZkZooInitCbData* data = static_cast<ZkZooInitCbData*>(watcher_ctx);
    assert(data != nullptr);

    ZkClientPtr zkclient =
        ZkClientManager::instance().__getZkClient(data->handle_);
    if (!zkclient) {
      delete data;
      data = nullptr;
      return;
    }

    LOG_WARN << "[SessionWatcher] session state:" << getSessStatStr(state)
             << ", session Handle:" << zkclient->getHandle();
    std::lock_guard<std::mutex> lock(zkclient->getStateMutex());

    zkclient->setSessStat(state);

    if (state == ZOO_CONNECTED_STATE) {
      zkclient->setSessTimeout(zoo_recv_timeout(zh));
      LOG_WARN << "[SessionWatcher] set sessionTimeout:"
               << zkclient->getSessTimeout()
               << ", session Handle:" << zkclient->getHandle();
      zkclient->getStateCondition().notify_one();
    } else if (state == ZOO_EXPIRED_SESSION_STATE) {
      // 会话过期，唤醒init函数
      zkclient->getStateCondition().notify_one();
    } else {
      // 连接异常，记录下异常开始时间
      zkclient->setSessDisconn(zkclient->getCurrentMs());
      LOG_WARN << "[SessionWatcher] set sessionDisconnectMs:"
               << zkclient->getSessDisconn()
               << ", session Handle:" << zkclient->getHandle();
    }
  }
}

bool ZkClient::reconnect() {
  ZkZooInitCbData* cbfunData = new ZkZooInitCbData(handle_);
  zhandle_ = zookeeper_init(host_.c_str(), sessionWatcher, sessionTimeout_,
                            (const clientid_t*)clientId_, cbfunData, 0);
  if (!zhandle_) {
    delete cbfunData;
    cbfunData = NULL;
    LOG_ERROR << "[ZkClient::reconnect] reconnnect failed, zookeeper_init "
                 "failed. session Handle:"
              << handle_;
    return false;
  }

  std::unique_lock<std::mutex> lock(stateMutex_);
  stateCondition_.wait(lock, [this]() {
    int status = sessionState_;
    return status == ZOO_CONNECTED_STATE || status == ZOO_EXPIRED_SESSION_STATE;
  });

  // 会话过期
  if (getSessStat() == ZOO_EXPIRED_SESSION_STATE) {
    LOG_ERROR << "session Handle:" << handle_
              << ", session stat is session_expired! ";
    return false;
  }

  setIsInit(true);
  return true;
}

void ZkClient::retry(uint32_t handle) {
  ZkClientPtr client = ZkClientManager::instance().getZkClient(handle);
  if (!client) {
    return;  //如果找不到，说明此session已经销毁了，不用再定时retry了.
  }

  if (client->reconnect() == false) {
    LOG_WARN << "[ZkClient::retry] reconnect failed. session Handle:" << handle
             << ", retryDelay: " << client->getRetryDelay();
    if (client->isSupportReconnect() == true) {
      //再重试
      ZkClientManager::instance().getSecondZkEventLoop()->runAfter(
          client->getRetryDelay(), std::bind(&ZkClient::retry, handle));
      client->setRetryDelay(
          std::min(client->getRetryDelay() * 2, zkutil::kMaxRetryDelay));
      client->setIsRetrying(true);
    }
  } else {
    LOG_WARN << "[ZkClient::retry] reconnnect succeed. session Handle:"
             << handle;
    client->setRetryDelay(zkutil::kInitRetryDelay);
    client->setIsRetrying(false);
    client->setHasCallTimeoutFun(false);
  }
}

void ZkClient::checkSessionState(uint32_t handle) {
  ZkClientPtr client = ZkClientManager::instance().getZkClient(handle);
  if (!client) {
    return;  //如果找不到，说明此session已经销毁了，不用再定时check了.
  }

  bool session_expired = false;
  if (client->getSessStat() == ZOO_EXPIRED_SESSION_STATE) {
    session_expired = true;
  } else if (client->getSessStat() != ZOO_CONNECTED_STATE) {
    if (client->getCurrentMs() - client->getSessDisconn() >
        client->getSessTimeout()) {
      LOG_WARN << "[ZkClient::CheckSessionState] sesssion disconnect expired! "
                  "currMs:"
               << client->getCurrentMs()
               << ", sessDisconn:" << client->getSessDisconn()
               << ", sessTimeout:" << client->getSessTimeout()
               << ", session Handle:" << client->getHandle();
      session_expired = true;
    }
  }

  if (session_expired) {
    client->setSessStat(ZOO_EXPIRED_SESSION_STATE);

    // 会话过期，回调用户终结程序
    SessionExpiredHandler& handler = client->getExpireHandler();
    if (client->hasCallTimeoutFun() == false && handler) {
      LOG_WARN << "[ZkClient::CheckSessionState] session expired, so call user "
                  "handler."
               << ", session Handle:" << client->getHandle();
      handler(client, client->getContext());  // 停止检测
      client->setHasCallTimeoutFun(true);
    }

    if (client->isRetrying() == false && client->isSupportReconnect() == true) {
      LOG_WARN << "[ZkClient::CheckSessionState] session expired, so retry "
                  "create session. retryDelay: "
               << client->getRetryDelay()
               << ", session Handle:" << client->getHandle();
      //重连zookeeper server
      TimerId timerid =
          ZkClientManager::instance().getSecondZkEventLoop()->runAfter(
              client->getRetryDelay(), std::bind(&ZkClient::retry, handle));
      client->setRetryDelay(
          std::min(client->getRetryDelay() * 2, zkutil::kMaxRetryDelay));
      client->setIsRetrying(true);
    }
  }

  //定时 每10ms 检查 是否会话超时
  double timeInterval = 0.01;
  ZkClientManager::instance().getFirstZkEventLoop()->runAfter(
      timeInterval, std::bind(&ZkClient::checkSessionState, handle));
};

int64_t ZkClient::getCurrentMs() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

ZkClient::ZkClient(uint32_t handle)
    : handle_(handle),
      isInitialized_(false),
      isRetrying_(false),
      hasCallTimeoutFun_(false),
      retryDelay_(zkutil::kInitRetryDelay),
      isSupportReconnect_(true),  // 默认支持重连
      host_(""),
      zhandle_(nullptr),
      clientId_(nullptr),
      sessionState_(ZOO_CONNECTING_STATE),
      sessionTimeout_(0),
      userContext_(nullptr),
      expiredHandler_(nullptr) {}

ZkClient::~ZkClient() {
  LOG_WARN << "distroy this zkclient, session Handle:" << handle_;
  std::cout << "[~ZkClient] distroy this zkclient, session Handle:" << handle_
            << std::endl;
  if (clientId_) {
    delete clientId_;
    clientId_ = NULL;
  }

  isSupportReconnect_ = false;
  if (zhandle_) {
    zookeeper_close(zhandle_);
  }

  nodeWatchDatas_.clear();
  childWatchDatas_.clear();
};

bool ZkClient::init(const std::string& host, int timeout,
                    SessionClientId* clientId /*= nullptr*/,
                    SessionExpiredHandler expired_handler /*= nullptr*/,
                    void* context /*= nullptr*/) {
  sessionTimeout_ = timeout;
  if (expired_handler) {
    expiredHandler_ = expired_handler;
  }
  userContext_ = context;

  host_ = host;
  LOG_DEBUG << "session Handle:" << handle_ << ", host: " << host_;
  if (clientId == nullptr) {
    clientId_ = nullptr;
  } else {
    clientId_ = new SessionClientId();
    clientId_->client_id = clientId->client_id;
    strncpy(clientId_->passwd, clientId->passwd, sizeof(clientId->passwd));
    LOG_DEBUG << "session Handle:" << handle_
              << ", clientId.id: " << clientId_->client_id
              << ", clientId.passwd:" << clientId_->passwd;
  }

  // zk 初始化
  ZkZooInitCbData* cbfunData = new ZkZooInitCbData(handle_);
  zhandle_ = zookeeper_init(host.c_str(), sessionWatcher, sessionTimeout_,
                            reinterpret_cast<const clientid_t*>(clientId_),
                            cbfunData, 0);
  if (!zhandle_) {
    delete cbfunData;
    cbfunData = nullptr;
    LOG_ERROR << "session Handle:" << handle_
              << ", zookeeper_init failed! host: " << host_;
    return false;
  }

  std::unique_lock<std::mutex> lock(stateMutex_);
  stateCondition_.wait(lock, [this]() {
    int status = sessionState_;
    return status == ZOO_CONNECTED_STATE || status == ZOO_EXPIRED_SESSION_STATE;
  });

  // 会话过期
  if (getSessStat() == ZOO_EXPIRED_SESSION_STATE) {
    LOG_ERROR << "session Handle:" << handle_
              << ", session stat is session_expired! ";
    return false;
  }

  setIsInit(true);

  /**
   * 启动zk状态检测线程：
   * 1. 检测到session_expire状态，回调SessionExpiredHandler，由用户 Kill 掉
   * 2. 检测非connected状态，若超过 session timeout时间
	 *	  回调SessionExpiredHandler，由用户 Kill 掉
   */
  isRetrying_ = false;
  hasCallTimeoutFun_ = false;
  double timeInterval = 0.01;  //  每10ms 检查会话是否超时

  ZkClientManager::instance().getFirstZkEventLoop()->runAfter(
      timeInterval, std::bind(&ZkClient::checkSessionState, handle_));

  return true;
}

int ZkClient::getSessStat() {
  int retStat;
  std::lock_guard<std::mutex> lock(sessStateMutex_);
  retStat = sessionState_;
  return retStat;
}

void ZkClient::setSessStat(int stat) {
  std::lock_guard<std::mutex> lock(sessStateMutex_);
  sessionState_ = stat;
}

int ZkClient::getSessTimeout() {
  int retTime;
  std::lock_guard<std::mutex> lock(sessTimeoutMutex_);
  retTime = sessionTimeout_;
  return retTime;
}

void ZkClient::setSessTimeout(int time) {
  std::lock_guard<std::mutex> lock(sessTimeoutMutex_);
  sessionTimeout_ = time;
}

int64_t ZkClient::getSessDisconn() {
  int64_t disconn;
  std::lock_guard<std::mutex> lock(sessDisconnMutex_);
  disconn = sessionDisconnectMs_;
  return disconn;
}

void ZkClient::setSessDisconn(int64_t disconn) {
  std::lock_guard<std::mutex> lock(sessDisconnMutex_);
  sessionDisconnectMs_ = disconn;
}

std::string ZkClient::getSessStatStr(int stat) {
  if (stat == ZOO_EXPIRED_SESSION_STATE) {
    return "ZOO_EXPIRED_SESSION_STATE";
  } else if (stat == ZOO_AUTH_FAILED_STATE) {
    return "ZOO_AUTH_FAILED_STATE";
  } else if (stat == ZOO_CONNECTING_STATE) {
    return "ZOO_CONNECTING_STATE";
  } else if (stat == ZOO_ASSOCIATING_STATE) {
    return "ZOO_ASSOCIATING_STATE";
  } else if (stat == ZOO_CONNECTED_STATE) {
    return "ZOO_CONNECTED_STATE";
  } else {
    return "";
  }
}

void ZkClient::setNodeWatchData(const std::string& path,
                                const NodeWatchData& data) {
  std::lock_guard<std::mutex> lock(nodeWatchMutex_);
  nodeWatchDatas_[path] = data;
}

bool ZkClient::getNodeWatchData(const std::string& path,
                                NodeWatchData& retNodeWatchData) {
  bool result = false;
  std::lock_guard<std::mutex> lock(nodeWatchMutex_);
  if (nodeWatchDatas_.find(path) != nodeWatchDatas_.end()) {
    retNodeWatchData = nodeWatchDatas_[path];
    result = true;
  }
  return result;
}

void ZkClient::getNodeWatchPaths(std::vector<std::string>& data) {
  std::lock_guard<std::mutex> lock(nodeWatchMutex_);
  std::map<std::string, NodeWatchData>::iterator iter = nodeWatchDatas_.begin();
  for (; iter != nodeWatchDatas_.end(); iter++) {
    data.push_back(iter->first);
  }
}

bool ZkClient::isShouldNotifyNodeWatch(const std::string& path) {
  bool result = false;
  std::lock_guard<std::mutex> lock(nodeWatchMutex_);
  if (nodeWatchDatas_.find(path) != nodeWatchDatas_.end()) {
    result = true;
  }
  return result;
}

void ZkClient::getChildWatchPaths(std::vector<std::string>& data) {
  std::lock_guard<std::mutex> lock(childWatchMutex_);
  std::map<std::string, ChildWatchData>::iterator iter =
      childWatchDatas_.begin();
  for (; iter != childWatchDatas_.end(); iter++) {
    data.push_back(iter->first);
  }
}

void ZkClient::setChildWatchData(const std::string& path,
                                 const ChildWatchData& data) {
  std::lock_guard<std::mutex> lock(childWatchMutex_);
  childWatchDatas_[path] = data;
}

bool ZkClient::getChildWatchData(const std::string& path,
                                 ChildWatchData& retChildWatchData) {
  bool result = false;
  std::lock_guard<std::mutex> lock(childWatchMutex_);
  if (childWatchDatas_.find(path) != childWatchDatas_.end()) {
    retChildWatchData = childWatchDatas_[path];
    result = true;
  }
  return result;
}

bool ZkClient::isShouldNotifyChildWatch(const std::string& path) {
  bool result = false;
  std::lock_guard<std::mutex> lock(childWatchMutex_);
  if (childWatchDatas_.find(path) != childWatchDatas_.end()) {
    result = true;
  }
  return result;
}

bool ZkClient::getClientId(SessionClientId& cliId) {
  if (isConnected() == true) {
    const SessionClientId* pClientId =
        reinterpret_cast<const SessionClientId*>(zoo_client_id(zhandle_));
    if (pClientId != nullptr) {
      cliId.client_id = pClientId->client_id;
      strncpy(cliId.passwd, pClientId->passwd, sizeof(pClientId->passwd));
      return true;
    }
  }
  return false;
}

bool ZkClient::create(const std::string& path, const std::string& value,
                      CreateHandler handler, void* context,
                      bool isTemp /*= false*/, bool isSequence /*= false*/) {
  if (handler == nullptr) {
    return false;
  }
  if (isConnected() == false) {
    return false;
  }

  ZkOperateAndWatchContext* watch_ctx =
      new ZkOperateAndWatchContext(path, context, shared_from_this());
  watch_ctx->create_handler_ = handler;

  int flags = 0;
  if (isTemp == true) {
    flags |= ZOO_EPHEMERAL;
  }
  if (isSequence == true) {
    flags |= ZOO_SEQUENCE;
  }
  int rc =
      zoo_acreate(zhandle_, path.c_str(), value.c_str(), value.size(),
                  &ZOO_OPEN_ACL_UNSAFE, flags, createCompletion, watch_ctx);
  LOG_DEBUG << "[ZkClient::Create] zoo_acreate path:" << path
            << ", value:" << value << ", isTemp:" << isTemp
            << ", isSeq:" << isSequence << ", session Handle:" << handle_;
  return rc == ZOK ? true : false;
}

void ZkClient::createCompletion(int rc, const char* value, const void* data) {
  assert(rc == ZOK || rc == ZNODEEXISTS || rc == ZCONNECTIONLOSS ||
         rc == ZOPERATIONTIMEOUT || rc == ZNOAUTH || rc == ZNONODE ||
         rc == ZNOCHILDRENFOREPHEMERALS || rc == ZCLOSING);
  assert(data != NULL);

  const ZkOperateAndWatchContext* watch_ctx =
      (const ZkOperateAndWatchContext*)data;
  assert(watch_ctx->zkclient_);

  LOG_DEBUG << "[ZkClient::CreateCompletion] rc:" << rc
            << ", create path:" << watch_ctx->path_
            << ", session Handle:" << watch_ctx->zkclient_->getHandle();

  if (rc == ZOK) {
    if (watch_ctx->create_handler_) {
      watch_ctx->create_handler_(zkutil::kZKSucceed, watch_ctx->zkclient_,
                                 watch_ctx->path_, value, watch_ctx->context_);
    }
  } else if (rc == ZNONODE) {
    if (watch_ctx->create_handler_) {
      //子路径不存在
      watch_ctx->create_handler_(zkutil::kZKNotExist, watch_ctx->zkclient_,
                                 watch_ctx->path_, "", watch_ctx->context_);
    }
  } else if (rc == ZNODEEXISTS) {
    if (watch_ctx->create_handler_) {
      watch_ctx->create_handler_(zkutil::kZKExisted, watch_ctx->zkclient_,
                                 watch_ctx->path_, "", watch_ctx->context_);
    }
  } else {
    if (watch_ctx->create_handler_) {
      watch_ctx->create_handler_(zkutil::kZKError, watch_ctx->zkclient_,
                                 watch_ctx->path_, "", watch_ctx->context_);
    }
  }
  delete watch_ctx;
  watch_ctx = NULL;
}

zkutil::ZkErrorCode ZkClient::getNode(const std::string& path,
                                      std::string& value, int32_t& version) {
  if (isConnected() == false) {
    return zkutil::kZKLostConnection;
  }

  int isWatch = 0;
  struct Stat stat;
  char buffer[zkutil::kMaxNodeValueLength] = {0};
  int buffer_len = sizeof(buffer);

  int rc = zoo_get(zhandle_, path.c_str(), isWatch, buffer, &buffer_len, &stat);
  LOG_DEBUG << "[ZkClient::GetNode] zoo_get path:" << path
            << ", version:" << version << ", rc:" << rc
            << ", session Handle:" << handle_;
  if (rc == ZOK) {
    if (buffer_len != -1) {
      value.assign(buffer, buffer_len);
    } else {
      value = "";
    }
    version = stat.version;
    return zkutil::kZKSucceed;
  } else if (rc == ZNONODE) {
    return zkutil::kZKNotExist;
  } else {
    return zkutil::kZKError;
  }
}

zkutil::ZkErrorCode ZkClient::getChildren(
    const std::string& path, std::vector<std::string>& childNodes) {
  if (isConnected() == false) {
    return zkutil::kZKLostConnection;
  }

  int isWatch = 0;
  struct String_vector strings = {0, NULL};
  int rc = zoo_get_children(zhandle_, path.c_str(), isWatch, &strings);
  LOG_DEBUG << "[ZkClient::GetChildren] zoo_get_children path:" << path
            << ", rc:" << rc << ", session Handle:" << handle_;
  if (rc == ZOK) {
    for (int i = 0; i < strings.count; ++i) {
      childNodes.push_back(strings.data[i]);
    }
    deallocate_String_vector(&strings);
    return zkutil::kZKSucceed;
  } else if (rc == ZNONODE) {
    return zkutil::kZKNotExist;
  }
  return zkutil::kZKError;
}

zkutil::ZkErrorCode ZkClient::isExist(const std::string& path) {
  if (isConnected() == false) {
    return zkutil::kZKLostConnection;
  }

  int isWatch = 0;
  int rc = zoo_exists(zhandle_, path.c_str(), isWatch, nullptr);
  LOG_DEBUG << "[ZkClient::IsExist] zoo_exists path:" << path << ", rc:" << rc
            << ", session Handle:" << handle_;
  if (rc == ZOK) {
    return zkutil::kZKSucceed;
  } else if (rc == ZNONODE) {
    return zkutil::kZKNotExist;
  }
  return zkutil::kZKError;
}

//阻塞式 创建目录结点
bool ZkClient::createPersistentDir(const std::string& path) {
  LOG_DEBUG << "[ZkClient::CreatePersistentDir] path:" << path
            << ", session Handle:" << handle_;
  //先尝试创建 外层的 目录结点
  zkutil::ZkErrorCode ec = createPersistentDirNode(path);
  if (ec == zkutil::kZKSucceed || ec == zkutil::kZKExisted) {
    return true;
  } else if (ec == zkutil::kZKNotExist) {
    //如果失败，则先尝试 创建里层的 目录结点，然后创建 外层的目录结点

    string::size_type pos = path.rfind('/');
    if (pos == string::npos) {
      LOG_ERROR << "[ZkClient::CreatePersistentDir] Can't find / character, "
                   "create dir failed! path:"
                << path << ", session Handle:" << handle_;
      return false;
    } else {
      std::string parentDir = path.substr(0, pos);
      if (createPersistentDir(parentDir) == true) {
        //创建父目录成功
        return createPersistentDir(path);
      } else {
        LOG_ERROR
            << "[ZkClient::CreatePersistentDir] create parent dir failed! dir:"
            << parentDir << ", session Handle:" << handle_;
        return false;
      }
    }
  } else  //zkutil::kZKError
  {
    LOG_ERROR << "[ZkClient::CreatePersistentDir] CreatePersistentDirNode "
                 "failed! path:"
              << path << ", session Handle:" << handle_;
    return false;
  }
}

void ZkClient::postCreateParentAndNode(
    const ContextInCreateParentAndNodes* watch_ctx) {
  assert(watch_ctx != nullptr);

  bool createDirSucc = true;
  string::size_type pos = watch_ctx->path_.rfind('/');
  if (pos == string::npos) {
    LOG_ERROR << "[ZkClient::postCreateParentAndNode] Can't find / character, "
                 "create node failed! path:"
              << watch_ctx->path_ << ", session Handle:" << handle_;

    createDirSucc = false;
    goto TAG_CREATE_DIR;
  } else {
    std::string parentDir = watch_ctx->path_.substr(0, pos);
    //同步 创建目录结点
    if (createPersistentDir(parentDir) == true) {
      //异步 创建叶子结点
      bool ret = create(watch_ctx->path_, watch_ctx->value_,
                        watch_ctx->create_handler_, watch_ctx->context_,
                        watch_ctx->isTemp_, watch_ctx->isSequence_);
      if (ret == false) {
        LOG_ERROR
            << "[ZkClient::postCreateParentAndNode] create node failed! path:"
            << watch_ctx->path_ << ", isTemp_:" << watch_ctx->isTemp_
            << ", isSeq:" << watch_ctx->isSequence_
            << ", session Handle:" << handle_;

        createDirSucc = false;
        goto TAG_CREATE_DIR;
      }
    } else {
      LOG_ERROR << "[ZkClient::postCreateParentAndNode] create dir failed! dir:"
                << parentDir << ", path:" << watch_ctx->path_
                << ", session Handle:" << handle_;

      createDirSucc = false;
      goto TAG_CREATE_DIR;
    }
  }

TAG_CREATE_DIR:
  if (createDirSucc == false) {
    if (watch_ctx->create_handler_) {
      watch_ctx->create_handler_(zkutil::kZKError, watch_ctx->zkclient_,
                                 watch_ctx->path_, "", watch_ctx->context_);
    }
  }

  delete watch_ctx;
  watch_ctx = NULL;
}

zkutil::ZkErrorCode ZkClient::createPersistentDirNode(const std::string& path) {
  if (isConnected() == false) {
    return zkutil::kZKLostConnection;
  }

  int flags = 0;  //分支路径的结点 默认是 持久型、非顺序型
  int rc = zoo_create(zhandle_, path.c_str(), NULL, -1, &ZOO_OPEN_ACL_UNSAFE,
                      flags, NULL, 0);
  LOG_DEBUG << "[ZkClient::CreatePersistentDirNode] handle: " << handle_
            << "path:" << path << "rc:" << rc << ", session Handle:" << handle_;
  if (rc == ZOK) {
    return zkutil::kZKSucceed;
  } else if (rc == ZNONODE) {
    return zkutil::kZKNotExist;
  } else if (rc == ZNODEEXISTS) {
    return zkutil::kZKExisted;
  }
  return zkutil::kZKError;
}

zkutil::ZkErrorCode ZkClient::createIfNeedCreateParents(
    const std::string& path, const std::string& value, bool isTemp /*= false*/,
    bool isSequence /*= false*/, std::string& retPath) {
  zkutil::ZkErrorCode ec = create(path, value, isTemp, isSequence, retPath);
  LOG_DEBUG << "ZkClient::CreateIfNeedCreateParents Create path:" << path
            << ", value:" << value << ", isTemp" << isTemp
            << ", isSeq:" << isSequence << ", ec:" << ec
            << ", session Handle:" << handle_;
  if (ec == zkutil::kZKNotExist)  //分支结点不存在
  {
    string::size_type pos = path.rfind('/');
    if (pos == string::npos) {
      LOG_ERROR << "[ZkClient::CreateIfNeedCreateParents] Can't find / "
                   "character, create node failed! path:"
                << path << ", session Handle:" << handle_;
      return zkutil::kZKError;
    } else {
      std::string parentDir = path.substr(0, pos);
      //递归创建 所有 父目录结点
      if (createPersistentDir(parentDir) == true) {
        //创建叶子结点
        return create(path, value, isTemp, isSequence, retPath);
      } else {
        LOG_ERROR
            << "[ZkClient::CreateIfNeedCreateParents] create dir failed! dir:"
            << parentDir << ", path:" << path << ", session Handle:" << handle_;
        return zkutil::kZKError;
      }
    }
  } else {
    return ec;
  }
}

zkutil::ZkErrorCode ZkClient::set(const std::string& path,
                                  const std::string& value,
                                  int32_t version /*= -1*/) {
  if (isConnected() == false) {
    return zkutil::kZKLostConnection;
  }

  int rc =
      zoo_set(zhandle_, path.c_str(), value.c_str(), value.size(), version);
  LOG_DEBUG << "[ZkClient::Set] zoo_set path:" << path << ", value:" << value
            << ", version:" << version << ", rc:" << rc
            << ", session Handle:" << handle_;
  if (rc == ZOK) {
    return zkutil::kZKSucceed;
  } else if (rc == ZNONODE) {
    return zkutil::kZKNotExist;
  }
  return zkutil::kZKError;
}

zkutil::ZkErrorCode ZkClient::deleteNode(const std::string& path,
                                         int32_t version /*= -1*/) {
  if (isConnected() == false) {
    return zkutil::kZKLostConnection;
  }

  int rc = zoo_delete(zhandle_, path.c_str(), version);
  LOG_DEBUG << "[ZkClient::Delete] zoo_delete path:" << path
            << ", version:" << version << ", rc:" << rc
            << ", session Handle:" << handle_;
  if (rc == ZOK) {
    return zkutil::kZKSucceed;
  } else if (rc == ZNONODE) {
    return zkutil::kZKNotExist;
  } else if (rc == ZNOTEMPTY) {
    return zkutil::kZKNotEmpty;
  }
  return zkutil::kZKError;
}

/*
 * return:
 *      kZKSucceed: 删除成功
 *      kZKNotExist: 结点已不存在
 *      kZKError: 操作时出现错误
 */
zkutil::ZkErrorCode ZkClient::deleteRecursive(const std::string& path,
                                              int32_t version /*= -1*/) {
  //获取child 结点
  std::vector<std::string> childNodes;
  childNodes.clear();
  zkutil::ZkErrorCode ec = getChildren(path, childNodes);
  if (ec == zkutil::kZKNotExist) {
    return zkutil::kZKSucceed;
  } else if (ec != zkutil::kZKSucceed) {
    LOG_ERROR << "[ZkClient::DeleteRecursive] GetChildren failed! ec:" << ec
              << ", path:" << path << ", version:" << version
              << ", session Handle:" << handle_;
    return zkutil::kZKError;
  } else {
    //删除 child 结点
    std::vector<std::string>::iterator iter = childNodes.begin();
    for (; iter != childNodes.end(); iter++) {
      std::string childPath = path + "/" + (*iter);
      zkutil::ZkErrorCode ec1 =
          deleteRecursive(childPath, -1);  //删除子结点 用 最近的version

      if (ec1 != zkutil::kZKSucceed && ec1 != zkutil::kZKNotExist) {
        LOG_ERROR << "[ZkClient::DeleteRecursive] GetChildren failed! ec:" << ec
                  << ", path:" << path << ", version:" << version
                  << ", session Handle:" << handle_;
        return zkutil::kZKError;
      }
    }

    //删除分支结点
    return deleteNode(path, version);
  }
}

zkutil::ZkErrorCode ZkClient::create(const std::string& path,
                                     const std::string& value, bool isTemp,
                                     bool isSequence, std::string& retPath) {
  if (isConnected() == false) {
    return zkutil::kZKLostConnection;
  }

  int flags = 0;
  if (isTemp == true) {
    flags |= ZOO_EPHEMERAL;
  }
  if (isSequence == true) {
    flags |= ZOO_SEQUENCE;
  }

  char buffer[zkutil::kMaxPathLength] = {0};
  int buffer_len = sizeof(buffer);
  int rc = zoo_create(zhandle_, path.c_str(), value.c_str(), value.size(),
                      &ZOO_OPEN_ACL_UNSAFE, flags, buffer, buffer_len);
  LOG_DEBUG << "[ZkClient::Create] zoo_create path:" << path
            << ", value:" << value << ", isTemp:" << isTemp
            << ", isSeq:" << isSequence << ", rc:" << rc
            << ", session Handle:" << handle_;
  if (rc == ZOK) {
    retPath.assign(buffer);
    return zkutil::kZKSucceed;
  } else if (rc == ZNONODE) {
    return zkutil::kZKNotExist;
  } else if (rc == ZNODEEXISTS) {
    return zkutil::kZKExisted;
  }
  return zkutil::kZKError;
}

bool ZkClient::getNode(const std::string& path, GetNodeHandler handler,
                       void* context) {
  if (handler == nullptr) {
    return false;
  }
  if (isConnected() == false) {
    return false;
  }

  ZkOperateAndWatchContext* watch_ctx =
      new ZkOperateAndWatchContext(path, context, shared_from_this());
  watch_ctx->getnode_handler_ = handler;

  int isWatch = 0;  //这里默认不触发zookeeper_init中注册的watch函数.
  int rc = zoo_aget(zhandle_, path.c_str(), isWatch, getNodeDataCompletion,
                    watch_ctx);
  LOG_DEBUG << "[GetNode] zoo_aget  path:" << path
            << ", session Handle:" << handle_;
  return rc == ZOK ? true : false;
}

void ZkClient::getNodeDataCompletion(int rc, const char* value, int value_len,
                                     const struct Stat* stat,
                                     const void* data) {
  assert(rc == ZOK || rc == ZCONNECTIONLOSS || rc == ZOPERATIONTIMEOUT ||
         rc == ZNOAUTH || rc == ZNONODE || rc == ZCLOSING);
  assert(data != nullptr);

  const ZkOperateAndWatchContext* watch_ctx =
      (const ZkOperateAndWatchContext*)data;
  assert(watch_ctx->zkclient_);
  std::string strValue = "";

  LOG_DEBUG << "[ZkClient::GetNodeDataCompletion] rc: " << rc
            << ", getnode path:" << watch_ctx->path_
            << ", session Handle:" << watch_ctx->zkclient_->getHandle();
  if (rc == ZOK) {
    if (watch_ctx->getnode_handler_) {
      strValue.assign(value, value_len);
      watch_ctx->getnode_handler_(zkutil::kZKSucceed, watch_ctx->zkclient_,
                                  watch_ctx->path_, strValue, stat->version,
                                  watch_ctx->context_);
    }
  } else if (rc == ZNONODE) {
    if (watch_ctx->getnode_handler_) {
      watch_ctx->getnode_handler_(
          zkutil::kZKNotExist, watch_ctx->zkclient_, watch_ctx->path_, strValue,
          zkutil::kInvalidDataVersion, watch_ctx->context_);
    }
  } else {
    if (watch_ctx->getnode_handler_) {
      watch_ctx->getnode_handler_(
          zkutil::kZKError, watch_ctx->zkclient_, watch_ctx->path_, strValue,
          zkutil::kInvalidDataVersion, watch_ctx->context_);
    }
  }

  delete watch_ctx;
  watch_ctx = nullptr;
}

bool ZkClient::getChildren(const std::string& path, GetChildrenHandler handler,
                           void* context) {
  if (handler == nullptr) {
    return false;
  }

  if (isConnected() == false) {
    return false;
  }

  ZkOperateAndWatchContext* watch_ctx =
      new ZkOperateAndWatchContext(path, context, shared_from_this());
  watch_ctx->getchildren_handler_ = handler;

  int isWatch = 0;
  int rc = zoo_aget_children(zhandle_, path.c_str(), isWatch,
                             getChildrenStringCompletion, watch_ctx);
  LOG_DEBUG << "[GetChildren] zoo_aget_children path:" << path
            << ", session Handle:" << handle_;
  return rc == ZOK ? true : false;
}

void ZkClient::getChildrenStringCompletion(int rc,
                                           const struct String_vector* strings,
                                           const void* data) {
  assert(rc == ZOK || rc == ZCONNECTIONLOSS || rc == ZOPERATIONTIMEOUT ||
         rc == ZNOAUTH || rc == ZNONODE || rc == ZCLOSING);
  assert(data != nullptr);

  const ZkOperateAndWatchContext* watch_ctx =
      (const ZkOperateAndWatchContext*)data;
  assert(watch_ctx->zkclient_);

  LOG_DEBUG << "[ZkClient::GetChildrenStringCompleteion] rc:" << rc
            << ", getChildren path:" << watch_ctx->path_
            << ", session Handle:" << watch_ctx->zkclient_->getHandle();

  if (rc == ZOK) {
    if (watch_ctx->getchildren_handler_) {
      std::vector<std::string> childNodes(strings->data,
                                          strings->data + strings->count);
      watch_ctx->getchildren_handler_(zkutil::kZKSucceed, watch_ctx->zkclient_,
                                      watch_ctx->path_, childNodes,
                                      watch_ctx->context_);
    }
  } else if (rc == ZNONODE) {
    if (watch_ctx->getchildren_handler_) {
      std::vector<std::string> childNodes;
      watch_ctx->getchildren_handler_(zkutil::kZKNotExist, watch_ctx->zkclient_,
                                      watch_ctx->path_, childNodes,
                                      watch_ctx->context_);
    }
  } else {
    if (watch_ctx->getchildren_handler_) {
      std::vector<std::string> childNodes;
      watch_ctx->getchildren_handler_(zkutil::kZKError, watch_ctx->zkclient_,
                                      watch_ctx->path_, childNodes,
                                      watch_ctx->context_);
    }
  }
  delete watch_ctx;
  watch_ctx = nullptr;
}

bool ZkClient::isExist(const std::string& path, ExistHandler handler,
                       void* context) {
  if (handler == nullptr) {
    return false;
  }
  if (isConnected() == false) {
    return false;
  }

  ZkOperateAndWatchContext* watch_ctx =
      new ZkOperateAndWatchContext(path, context, shared_from_this());
  watch_ctx->exist_handler_ = handler;

  int isWatch = 0;
  int rc =
      zoo_aexists(zhandle_, path.c_str(), isWatch, existCompletion, watch_ctx);
  LOG_DEBUG << "[IsExist] zoo_aexists path:" << path
            << ", session Handle:" << handle_;
  return rc == ZOK ? true : false;
}

void ZkClient::existCompletion(int rc, const struct Stat* stat,
                               const void* data) {
  assert(rc == ZOK || rc == ZCONNECTIONLOSS || rc == ZOPERATIONTIMEOUT ||
         rc == ZNOAUTH || rc == ZNONODE || rc == ZCLOSING);
  assert(data != nullptr);

  const ZkOperateAndWatchContext* watch_ctx =
      (const ZkOperateAndWatchContext*)data;
  assert(watch_ctx->zkclient_);
  LOG_DEBUG << "[ZkClient::ExistCompletion] rc:" << rc
            << ", isExist path:" << watch_ctx->path_
            << ", session Handle:" << watch_ctx->zkclient_->getHandle();

  if (rc == ZOK || rc == ZNONODE) {
    if (watch_ctx->exist_handler_) {
      watch_ctx->exist_handler_(
          rc == ZOK ? zkutil::kZKSucceed : zkutil::kZKNotExist,
          watch_ctx->zkclient_, watch_ctx->path_, watch_ctx->context_);
    }
  } else {
    if (watch_ctx->exist_handler_) {
      watch_ctx->exist_handler_(zkutil::kZKError, watch_ctx->zkclient_,
                                watch_ctx->path_, watch_ctx->context_);
    }
  }
  delete watch_ctx;
  watch_ctx = NULL;
}

bool ZkClient::createIfNeedCreateParents(const std::string& path,
                                         const std::string& value,
                                         CreateHandler handler, void* context,
                                         bool isTemp /*= false*/,
                                         bool isSequence /*= false*/) {
  if (handler == NULL) {
    return false;
  }
  if (isConnected() == false) {
    return false;
  }

  ContextInCreateParentAndNodes* watch_ctx = new ContextInCreateParentAndNodes(
      path, value, handler, context, isTemp, isSequence, shared_from_this());

  int flags = 0;
  if (isTemp == true) {
    flags |= ZOO_EPHEMERAL;
  }
  if (isSequence == true) {
    flags |= ZOO_SEQUENCE;
  }
  int rc = zoo_acreate(zhandle_, path.c_str(), value.c_str(), value.size(),
                       &ZOO_OPEN_ACL_UNSAFE, flags,
                       createIfNeedCreateParentsCompletion, watch_ctx);
  LOG_DEBUG << "[CreateIfNeedCreateParents] path:" << path
            << ", value:" << value << ", isTemp:" << isTemp
            << ", isSeq:" << isSequence << ", session Handle:" << handle_;
  return rc == ZOK ? true : false;
}

void ZkClient::createIfNeedCreateParentsCompletion(int rc, const char* value,
                                                   const void* data) {
  assert(rc == ZOK || rc == ZNODEEXISTS || rc == ZCONNECTIONLOSS ||
         rc == ZOPERATIONTIMEOUT || rc == ZNOAUTH || rc == ZNONODE ||
         rc == ZNOCHILDRENFOREPHEMERALS || rc == ZCLOSING);
  assert(data != NULL);

  const ContextInCreateParentAndNodes* watch_ctx =
      (const ContextInCreateParentAndNodes*)data;
  assert(watch_ctx->zkclient_);
  LOG_DEBUG << "[ZkClient::CreateIfNeedCreateParentsCompletion] rc:" << rc
            << ", create path:" << watch_ctx->path_
            << ", session Handle:" << watch_ctx->zkclient_->getHandle();

  if (rc == ZOK) {
    if (watch_ctx->create_handler_) {
      watch_ctx->create_handler_(zkutil::kZKSucceed, watch_ctx->zkclient_,
                                 watch_ctx->path_, value, watch_ctx->context_);
    }
    delete watch_ctx;
    watch_ctx = NULL;
  } else if (rc == ZNONODE)  //分支路径不存在
  {
    //先创建分支路径结点，再创建叶子结点
    //因为创建分支路径结点 需要一些时间，可能会阻塞当前线程，所以 转到其它线程来 执行这个操作
    ZkClientManager::instance().getSecondZkEventLoop()->runInLoop(std::bind(
        &ZkClient::postCreateParentAndNode, watch_ctx->zkclient_, watch_ctx));
  } else if (rc == ZNODEEXISTS) {
    if (watch_ctx->create_handler_) {
      watch_ctx->create_handler_(zkutil::kZKExisted, watch_ctx->zkclient_,
                                 watch_ctx->path_, "", watch_ctx->context_);
    }
    delete watch_ctx;
    watch_ctx = NULL;
  } else {
    if (watch_ctx->create_handler_) {
      watch_ctx->create_handler_(zkutil::kZKError, watch_ctx->zkclient_,
                                 watch_ctx->path_, "", watch_ctx->context_);
    }
    delete watch_ctx;
    watch_ctx = NULL;
  }
}

bool ZkClient::set(const std::string& path, const std::string& value,
                   SetHandler handler, void* context,
                   int32_t version /*= -1*/) {
  if (handler == NULL) {
    return false;
  }
  if (isConnected() == false) {
    return false;
  }

  ZkOperateAndWatchContext* watch_ctx =
      new ZkOperateAndWatchContext(path, context, shared_from_this());
  watch_ctx->set_handler_ = handler;

  int rc = zoo_aset(zhandle_, path.c_str(), value.c_str(), value.size(),
                    version, setCompletion, watch_ctx);
  LOG_DEBUG << "[ZkClient::Set] zoo_aset path:" << path << ", value: " << value
            << ", version:" << version << ", session Handle:" << handle_;
  return rc == ZOK ? true : false;
}

void ZkClient::setCompletion(int rc, const struct Stat* stat,
                             const void* data) {
  assert(rc == ZOK || rc == ZCONNECTIONLOSS || rc == ZOPERATIONTIMEOUT ||
         rc == ZBADVERSION || rc == ZNOAUTH || rc == ZNONODE || rc == ZCLOSING);

  const ZkOperateAndWatchContext* watch_ctx =
      (const ZkOperateAndWatchContext*)data;
  assert(watch_ctx->zkclient_);
  LOG_DEBUG << "[ZkClient::SetCompletion] rc:" << rc
            << ", set path:" << watch_ctx->path_
            << ", session Handle:" << watch_ctx->zkclient_->getHandle();

  if (rc == ZOK) {
    if (watch_ctx->set_handler_) {
      watch_ctx->set_handler_(zkutil::kZKSucceed, watch_ctx->zkclient_,
                              watch_ctx->path_, stat->version,
                              watch_ctx->context_);
    }
  } else if (rc == ZNONODE) {
    if (watch_ctx->set_handler_) {
      watch_ctx->set_handler_(zkutil::kZKNotExist, watch_ctx->zkclient_,
                              watch_ctx->path_, zkutil::kInvalidDataVersion,
                              watch_ctx->context_);
    }
  } else {
    if (watch_ctx->set_handler_) {
      watch_ctx->set_handler_(zkutil::kZKError, watch_ctx->zkclient_,
                              watch_ctx->path_, zkutil::kInvalidDataVersion,
                              watch_ctx->context_);
    }
  }
  delete watch_ctx;
  watch_ctx = NULL;
}

bool ZkClient::deleteNode(const std::string& path, DeleteHandler handler,
                          void* context, int32_t version /*= -1*/) {
  if (handler == NULL) {
    return false;
  }
  if (isConnected() == false) {
    return false;
  }

  ZkOperateAndWatchContext* watch_ctx =
      new ZkOperateAndWatchContext(path, context, shared_from_this());
  watch_ctx->delete_handler_ = handler;

  int rc =
      zoo_adelete(zhandle_, path.c_str(), version, deleteCompletion, watch_ctx);
  LOG_DEBUG << "[ZkClient::Delete] zoo_adelete path:" << path
            << ", version:" << version << ", session Handle:" << handle_;

  return rc == ZOK ? true : false;
}

void ZkClient::deleteCompletion(int rc, const void* data) {
  assert(rc == ZOK || rc == ZCONNECTIONLOSS || rc == ZOPERATIONTIMEOUT ||
         rc == ZBADVERSION || rc == ZNOAUTH || rc == ZNONODE ||
         rc == ZNOTEMPTY || rc == ZCLOSING);

  const ZkOperateAndWatchContext* watch_ctx =
      (const ZkOperateAndWatchContext*)data;
  assert(watch_ctx->zkclient_);
  LOG_DEBUG << "[ZkClient::DeleteCompletion] rc:" << rc
            << ", delete path:" << watch_ctx->path_
            << ", session Handle:" << watch_ctx->zkclient_->getHandle();

  if (rc == ZOK) {
    if (watch_ctx->delete_handler_) {
      watch_ctx->delete_handler_(zkutil::kZKSucceed, watch_ctx->zkclient_,
                                 watch_ctx->path_, watch_ctx->context_);
    }
  } else if (rc == ZNONODE) {
    if (watch_ctx->delete_handler_) {
      watch_ctx->delete_handler_(zkutil::kZKNotExist, watch_ctx->zkclient_,
                                 watch_ctx->path_, watch_ctx->context_);
    }
  } else if (rc == ZNOTEMPTY) {
    if (watch_ctx->delete_handler_) {
      watch_ctx->delete_handler_(zkutil::kZKNotEmpty, watch_ctx->zkclient_,
                                 watch_ctx->path_, watch_ctx->context_);
    }
  } else {
    if (watch_ctx->delete_handler_) {
      watch_ctx->delete_handler_(zkutil::kZKError, watch_ctx->zkclient_,
                                 watch_ctx->path_, watch_ctx->context_);
    }
  }
  delete watch_ctx;
  watch_ctx = NULL;
}

bool ZkClient::deleteRecursive(const std::string& path, DeleteHandler handler,
                               void* context, int32_t version /*= -1*/) {
  if (handler == nullptr) {
    return false;
  }
  if (isConnected() == false) {
    return false;
  }

  ContextInDeleteRecursive* watch_ctx = new ContextInDeleteRecursive(
      path, handler, context, version, shared_from_this());
  int rc = zoo_adelete(zhandle_, path.c_str(), version,
                       deleteRecursiveCompletion, watch_ctx);
  LOG_DEBUG << "[ZkClient::DeleteRecursive] zoo_adelete path:" << path
            << ", verson:" << version << ", session Handle:" << handle_;

  return rc == ZOK ? true : false;
}

void ZkClient::deleteRecursiveCompletion(int rc, const void* data) {
  assert(rc == ZOK || rc == ZCONNECTIONLOSS || rc == ZOPERATIONTIMEOUT ||
         rc == ZBADVERSION || rc == ZNOAUTH || rc == ZNONODE ||
         rc == ZNOTEMPTY || rc == ZCLOSING);

  const ContextInDeleteRecursive* watch_ctx =
      (const ContextInDeleteRecursive*)data;
  assert(watch_ctx->zkclient_);
  LOG_DEBUG << "[ZkClient::DeleteRecursiveCompletion] rc:" << rc
            << ", delete path:" << watch_ctx->path_
            << ", session Handle:" << watch_ctx->zkclient_->getHandle();

  if (rc == ZOK) {
    if (watch_ctx->delete_handler_) {
      watch_ctx->delete_handler_(zkutil::kZKSucceed, watch_ctx->zkclient_,
                                 watch_ctx->path_, watch_ctx->context_);
    }
    delete watch_ctx;
    watch_ctx = NULL;
  } else if (rc == ZNONODE) {
    if (watch_ctx->delete_handler_) {
      watch_ctx->delete_handler_(zkutil::kZKNotExist, watch_ctx->zkclient_,
                                 watch_ctx->path_, watch_ctx->context_);
    }
    delete watch_ctx;
    watch_ctx = NULL;
  } else if (rc == ZNOTEMPTY)  //含有叶子结点
  {
    //先删除子结点，再删除 分支结点
    //因为删除子结点 需要一些时间，可能会阻塞当前线程，所以 转到其它线程来 执行这个操作
    ZkClientManager::instance().getSecondZkEventLoop()->runInLoop(std::bind(
        &ZkClient::postDeleteRecursive, watch_ctx->zkclient_, watch_ctx));
  } else {
    if (watch_ctx->delete_handler_) {
      watch_ctx->delete_handler_(zkutil::kZKError, watch_ctx->zkclient_,
                                 watch_ctx->path_, watch_ctx->context_);
    }
    delete watch_ctx;
    watch_ctx = NULL;
  }
}

void ZkClient::postDeleteRecursive(const ContextInDeleteRecursive* watch_ctx) {
  assert(watch_ctx != NULL);
  assert(watch_ctx->delete_handler_ != NULL);

  bool deleteChildFailed = false;

  //获取child结点
  std::vector<std::string> childNodes;
  zkutil::ZkErrorCode ec = getChildren(watch_ctx->path_, childNodes);
  if (ec == zkutil::kZKNotExist) {
    watch_ctx->delete_handler_(zkutil::kZKSucceed, watch_ctx->zkclient_,
                               watch_ctx->path_, watch_ctx->context_);
  } else if (ec != zkutil::kZKSucceed) {
    LOG_ERROR << "[ZkClient::postDeleteRecursive] GetChildren failed! ec:" << ec
              << ", path:" << watch_ctx->path_
              << ", session Handle:" << watch_ctx->zkclient_->getHandle();

    deleteChildFailed = true;
    goto TAG_DELETE_CHILD;
  } else {
    // zkutil::kZKSucceed
    //同步 删除 child 结点
    std::vector<std::string>::iterator iter = childNodes.begin();
    for (; iter != childNodes.end(); iter++) {
      std::string childPath = watch_ctx->path_ + "/" + (*iter);

      zkutil::ZkErrorCode ec1 =
          deleteRecursive(childPath, -1);  //删除子结点 用 最近的version

      if (ec1 != zkutil::kZKSucceed && ec1 != zkutil::kZKNotExist) {
        LOG_ERROR << "[ZkClient::postDeleteRecursive] GetChildren failed! ec:"
                  << ec << ", path:" << watch_ctx->path_
                  << ", session Handle:" << watch_ctx->zkclient_->getHandle();

        watch_ctx->delete_handler_(zkutil::kZKError, watch_ctx->zkclient_,
                                   watch_ctx->path_, watch_ctx->context_);

        deleteChildFailed = true;
        goto TAG_DELETE_CHILD;
      }
    }

    //异步 删除分支结点
    if (deleteNode(watch_ctx->path_, watch_ctx->delete_handler_,
                   watch_ctx->context_, watch_ctx->version_) == false) {
      LOG_ERROR << "[ZkClient::postDeleteRecursive] async delete failed! path:"
                << watch_ctx->path_ << ", version:" << watch_ctx->version_
                << ", session Handle:" << watch_ctx->zkclient_->getHandle();

      deleteChildFailed = true;
      goto TAG_DELETE_CHILD;
    }
  }

TAG_DELETE_CHILD:
  if (deleteChildFailed == true) {
    if (watch_ctx->delete_handler_) {
      watch_ctx->delete_handler_(zkutil::kZKError, watch_ctx->zkclient_,
                                 watch_ctx->path_, watch_ctx->context_);
    }
  }

  delete watch_ctx;
  watch_ctx = NULL;
}

bool ZkClient::regNodeWatcher(const std::string& path,
                              NodeChangeHandler handler, void* context) {
  if (isConnected() == false) {
    return false;
  }
  if (handler == nullptr) {
    return false;
  };

  ZkOperateAndWatchContext* watch_ctx =
      new ZkOperateAndWatchContext(path, context, shared_from_this());
  watch_ctx->node_notify_handler_ = handler;

  int rc = zoo_wexists(zhandle_, path.c_str(), existWatcher, watch_ctx, NULL);
  LOG_DEBUG << "[ZkClient::regNodeWatcher] zoo_wexists path:" << path
            << ", rc:" << rc << ", session Handle:" << handle_;
  if (rc == ZOK || rc == ZNONODE) {
    //注册成功，则保存 watch数据
    NodeWatchData data;
    data.path_ = path;
    data.handler_ = handler;
    data.context_ = context;
    setNodeWatchData(path, data);

    LOG_DEBUG << "[ZkClient::regNodeWatcher] reg child watcher succeed, rc:"
              << rc << ", session Handle:" << handle_;
    return true;
  } else {
    LOG_ERROR << "[ZkClient::regNodeWatcher] reg child watcher failed, rc:"
              << rc << ", session Handle:" << handle_;
    return false;
  }
}

bool ZkClient::regChildWatcher(const std::string& path,
                               ChildChangeHandler handler, void* context) {
  if (isConnected() == false) {
    return false;
  }
  if (handler == NULL) {
    return false;
  };

  ZkOperateAndWatchContext* watch_ctx =
      new ZkOperateAndWatchContext(path, context, shared_from_this());
  watch_ctx->child_notify_handler_ = handler;

  struct String_vector strings = {0, NULL};
  int rc = zoo_wget_children(zhandle_, path.c_str(), getChildrenWatcher,
                             watch_ctx, &strings);
  LOG_DEBUG << "[ZkClient::regChildWatcher] zoo_wget_children path:" << path
            << ", rc:" << rc << ", session Handle:" << handle_;
  if (rc == ZOK) {
    LOG_DEBUG << "[ZkClient::regChildWatcher] reg child watcher succeed.";
    deallocate_String_vector(&strings);
    //注册成功，则保存 watch数据
    ChildWatchData data;
    data.path_ = path;
    data.handler_ = handler;
    data.context_ = context;
    setChildWatchData(path, data);
    return true;
  } else if (rc == ZNONODE) {
    LOG_ERROR << "[ZkClient::regChildWatcher] reg child watcher failed, znode "
                 "not existed."
              << ", session Handle:" << handle_;
    return false;
  } else {
    LOG_ERROR << "[ZkClient::regChildWatcher] reg child watcher failed, rc: "
              << rc << ", session Handle:" << handle_;
    return false;
  }
}

void ZkClient::existWatcher(zhandle_t* zh, int type, int state,
                            const char* path, void* watcher_ctx) {
  assert(type == ZOO_DELETED_EVENT || type == ZOO_CREATED_EVENT ||
         type == ZOO_CHANGED_EVENT || type == ZOO_NOTWATCHING_EVENT ||
         type == ZOO_SESSION_EVENT);

  ZkOperateAndWatchContext* context = (ZkOperateAndWatchContext*)watcher_ctx;
  assert(context->zkclient_);

  //再注册watch (为了避免阻塞线程，转到另外的线程来注册)
  if (context->zkclient_->isShouldNotifyNodeWatch(context->path_) == true) {
    ZkClientManager::instance().getSecondZkEventLoop()->runInLoop(std::bind(
        &ZkClient::autoRegNodeWatcher, context->zkclient_, context->path_));
  }

  LOG_DEBUG << "[ZkClient::ExistWatcher] type:" << type
            << ", regNodeWatcher path:" << context->path_
            << ", session Handle:" << context->zkclient_->getHandle();

  // 跳过会话事件,由zk handler的watcher进行处理
  if (type == ZOO_SESSION_EVENT) {
    return;
  }

  if (type == ZOO_NOTWATCHING_EVENT) {
    if (context->zkclient_->isShouldNotifyNodeWatch(context->path_) == true) {
      context->node_notify_handler_(
          zkutil::kTypeError, context->zkclient_, context->path_, "",
          zkutil::kInvalidDataVersion, context->context_);
    }
  } else if (type == ZOO_DELETED_EVENT) {
    if (context->zkclient_->isShouldNotifyNodeWatch(context->path_) == true) {
      context->node_notify_handler_(
          zkutil::kNodeDelete, context->zkclient_, context->path_, "",
          zkutil::kInvalidDataVersion, context->context_);
    }
  } else if (type == ZOO_CREATED_EVENT || type == ZOO_CHANGED_EVENT) {
    //节点创建或者元信息变动, 则向zookeeper获取节点最新的数据，再回调用户
    zkutil::ZkNotifyType eventType;
    if (type == ZOO_CREATED_EVENT) {
      eventType = zkutil::kNodeCreate;
    } else if (type == ZOO_CHANGED_EVENT) {
      eventType = zkutil::kNodeChange;
    }
    ContextInNodeWatcher* getDataContext = new ContextInNodeWatcher(
        context->path_, context->zkclient_, context->node_notify_handler_,
        eventType, context->context_);

    bool isWatch = 0;
    int rc = zoo_aget(zh, path, isWatch, getNodeDataOnWatcher, getDataContext);
    if (rc != ZOK) {
      LOG_ERROR << "[ZkClient::ExistWatcher] Get latest data failed! path:"
                << context->path_
                << ", session Handle:" << context->zkclient_->getHandle();
      if (context->zkclient_->isShouldNotifyNodeWatch(context->path_) == true) {
        //如果获取数据失败，则回调用户，再注册watch
        context->node_notify_handler_(
            zkutil::kGetNodeValueFailed, context->zkclient_, context->path_, "",
            zkutil::kInvalidDataVersion, context->context_);
      }
    }
  }
  delete context;
  context = NULL;
}

void ZkClient::autoRegNodeWatcher(std::string path) {
  if (isConnected() == false) {
    return;
  }

  LOG_DEBUG << "[ZkClient::autoRegNodeWatcher] path: " << path
            << ", session Handle:" << handle_;
  NodeWatchData data;
  bool ret = getNodeWatchData(path, data);
  if (ret == false || data.isSupportAutoReg_ == false) {
    LOG_WARN << "[ZkClient::autoRegNodeWatcher] get watch data failed or not "
                "support auto register watcher! path:"
             << path << ", session Handle:" << handle_;
    return;
  }

  bool regRet = regNodeWatcher(data.path_, data.handler_, data.context_);
  if (regRet == false) {
    LOG_ERROR << "[ZkClient::autoRegNodeWatcher] regChildWatcher failed, so "
                 "reg node watch again after 5 minutes. path:"
              << path << ", session Handle:" << handle_;

    //如果注册失败，则过5分钟之后再注册
    double timeAfter = 5 * 60;

    // ZkClientManager::instance().getSecondZkEventLoop()->runAfter(
    //     timeAfter,
    //     boost::bind(&ZkClient::autoRegNodeWatcher, shared_from_this(), path));

    ZkClientManager::instance().getSecondZkEventLoop()->runAfter(
        timeAfter, [this, path]() { this->autoRegNodeWatcher(path); });
  }
}

void ZkClient::autoRegChildWatcher(std::string path) {
  if (isConnected() == false) {
    return;
  }

  LOG_DEBUG << "[ZkClient::autoRegChildWatcher] path: " << path
            << ", session Handle:" << handle_;
  ChildWatchData data;
  bool ret = getChildWatchData(path, data);
  if (ret == false || data.isSupportAutoReg_ == false) {
    LOG_ERROR << "[ZkClient::autoRegChildWatcher] get watch data failed or not "
                 "support auto register watcher! path:"
              << path << ", session Handle:" << handle_;
    return;
  }

  bool regRet = regChildWatcher(data.path_, data.handler_, data.context_);
  if (regRet == false) {
    LOG_ERROR << "[ZkClient::autoRegChildWatcher] regChildWatcher failed, so "
                 "reg child watch again after 5 minutes. path:"
              << path << ", session Handle:" << handle_;

    //如果注册失败，则过5分钟之后再注册
    double timeAfter = 5 * 60;
    ZkClientManager::instance().getSecondZkEventLoop()->runAfter(
        timeAfter, [this, path]() { this->autoRegChildWatcher(path); });
  }
}

void ZkClient::cancelRegNodeWatcher(const std::string& path) {
  std::lock_guard<std::mutex> lock(nodeWatchMutex_);
  nodeWatchDatas_.erase(path);
}

void ZkClient::cancelRegChildWatcher(const std::string& path) {
  std::lock_guard<std::mutex> lock(childWatchMutex_);
  childWatchDatas_.erase(path);
}

void ZkClient::getNodeDataOnWatcher(int rc, const char* value, int value_len,
                                    const struct Stat* stat, const void* data) {
  assert(rc == ZOK || rc == ZCONNECTIONLOSS || rc == ZOPERATIONTIMEOUT ||
         rc == ZNOAUTH || rc == ZNONODE || rc == ZCLOSING);
  assert(data != NULL);

  const ContextInNodeWatcher* watch_ctx = (const ContextInNodeWatcher*)data;
  assert(watch_ctx->zkclient_ != NULL);

  LOG_DEBUG << "[ZkClient::GetNodeDataOnWatcher] rc:" << rc
            << ", getNodeData path:" << watch_ctx->path_
            << ", session Handle:" << watch_ctx->zkclient_->getHandle();

  if (rc == ZOK) {
    NodeWatchData data;
    bool ret = watch_ctx->zkclient_->getNodeWatchData(watch_ctx->path_, data);
    if (ret == true) {
      //更新缓存
      data.value_.assign(value, value_len);
      data.version_ = stat->version;

      //回调用户函数
      watch_ctx->node_notify_handler_(
          watch_ctx->notifyType_, watch_ctx->zkclient_, watch_ctx->path_,
          data.value_, data.version_, watch_ctx->contextInOrignalWatcher_);
    } else {
      LOG_ERROR << "[ZkClient::GetNodeDataOnWatcher] Can't find this watch "
                   "data. path: "
                << watch_ctx->path_
                << ", session Handle:" << watch_ctx->zkclient_->getHandle();
    }
  } else if (rc == ZNONODE) {
    //如果获取数据失败，则回调用户，再注册watch
    LOG_ERROR << "[ZkClient::GetNodeDataOnWatcher] Get latest data failed! "
                 "Don't have this znode. path: "
              << watch_ctx->path_
              << ", session Handle:" << watch_ctx->zkclient_->getHandle();

    if (watch_ctx->zkclient_->isShouldNotifyNodeWatch(watch_ctx->path_) ==
        true) {
      watch_ctx->node_notify_handler_(zkutil::kGetNodeValueFailed_NodeNotExist,
                                      watch_ctx->zkclient_, watch_ctx->path_,
                                      "", zkutil::kInvalidDataVersion,
                                      watch_ctx->contextInOrignalWatcher_);
    }
  } else {
    //如果获取数据失败，则回调用户，再注册watch
    LOG_ERROR
        << "[ZkClient::GetNodeDataOnWatcher] Get latest data failed! path: "
        << watch_ctx->path_ << ", rc:" << rc
        << ", session Handle:" << watch_ctx->zkclient_->getHandle();

    if (watch_ctx->zkclient_->isShouldNotifyNodeWatch(watch_ctx->path_) ==
        true) {
      watch_ctx->node_notify_handler_(
          zkutil::kGetNodeValueFailed, watch_ctx->zkclient_, watch_ctx->path_,
          "", zkutil::kInvalidDataVersion, watch_ctx->contextInOrignalWatcher_);
    }
  }

  delete watch_ctx;
  watch_ctx = NULL;
}

void ZkClient::getChildrenWatcher(zhandle_t* zh, int type, int state,
                                  const char* path, void* watcher_ctx) {
  assert(type == ZOO_DELETED_EVENT || type == ZOO_CHILD_EVENT ||
         type == ZOO_NOTWATCHING_EVENT || type == ZOO_SESSION_EVENT);

  ZkOperateAndWatchContext* context = (ZkOperateAndWatchContext*)watcher_ctx;
  assert(context->zkclient_);

  if (context->zkclient_->isShouldNotifyChildWatch(context->path_) == true) {
    ZkClientManager::instance().getSecondZkEventLoop()->runInLoop(std::bind(
        &ZkClient::autoRegChildWatcher, context->zkclient_, context->path_));
  }

  LOG_DEBUG << "[ZkClient::GetChildrenWatcher] type:" << type
            << ", path:" << context->path_
            << ", session Handle:" << context->zkclient_->getHandle();

  // 跳过会话事件,由zk handler的watcher进行处理
  if (type == ZOO_SESSION_EVENT) {
    return;
  }

  if (type == ZOO_NOTWATCHING_EVENT) {
    if (context->zkclient_->isShouldNotifyChildWatch(context->path_) == true) {
      std::vector<std::string> childNodes;
      context->child_notify_handler_(zkutil::kTypeError, context->zkclient_,
                                     context->path_, childNodes,
                                     context->context_);
    }
  } else if (type == ZOO_DELETED_EVENT) {

    LOG_DEBUG << "[ZkClient::GetChildrenWatcher] ZOO_DELETED_EVENT. path:"
              << context->path_
              << ", session Handle:" << context->zkclient_->getHandle();
  } else if (type == ZOO_CHILD_EVENT) {
    //节点创建或者元信息变动, 则向zookeeper获取 最新的子节点列表，再回调用户
    ContextInChildWatcher* getDataContext = new ContextInChildWatcher(
        context->path_, context->zkclient_, context->child_notify_handler_,
        zkutil::kChildChange, context->context_);

    int isWatch = 0;
    int rc = zoo_aget_children(zh, path, isWatch, getChildDataOnWatcher,
                               getDataContext);
    if (rc != ZOK) {
      LOG_ERROR << "[ZkClient::GetChildrenWatcher] Get latest child data "
                   "failed! path:"
                << context->path_
                << ", session Handle:" << context->zkclient_->getHandle();
      //如果获取数据失败，则回调用户，再注册watch
      if (context->zkclient_->isShouldNotifyChildWatch(context->path_) ==
          true) {
        std::vector<std::string> childNodes;
        context->child_notify_handler_(zkutil::kGetChildListFailed,
                                       context->zkclient_, context->path_,
                                       childNodes, context->context_);
      }
    }
  }
  delete context;
  context = NULL;
}

void ZkClient::getChildDataOnWatcher(int rc,
                                     const struct String_vector* strings,
                                     const void* data) {
  assert(rc == ZOK || rc == ZCONNECTIONLOSS || rc == ZOPERATIONTIMEOUT ||
         rc == ZNOAUTH || rc == ZNONODE || rc == ZCLOSING);
  assert(data != NULL);

  const ContextInChildWatcher* watch_ctx = (const ContextInChildWatcher*)data;
  assert(watch_ctx->zkclient_ != NULL);
  LOG_DEBUG << "[ZkClient::GetChildDataOnWatcher] rc:" << rc
            << ", getChildList path:" << watch_ctx->path_
            << ", session Handle:" << watch_ctx->zkclient_->getHandle();

  if (rc == ZOK) {
    ChildWatchData data;
    bool ret = watch_ctx->zkclient_->getChildWatchData(watch_ctx->path_, data);
    if (ret == true) {
      //更新缓存数据
      data.childList_.clear();
      data.childList_.assign(strings->data, strings->data + strings->count);
      //回调用户函数
      watch_ctx->child_notify_handler(
          watch_ctx->notifyType_, watch_ctx->zkclient_, watch_ctx->path_,
          data.childList_, watch_ctx->contextInOrignalWatcher_);
    } else {
      LOG_ERROR << "[ZkClient::GetChildDataOnWatcher] Can't find this watch "
                   "data. path: "
                << watch_ctx->path_
                << ", session Handle:" << watch_ctx->zkclient_->getHandle();
    }
  } else if (rc == ZNONODE) {
    LOG_ERROR << "[ZkClient::GetChildDataOnWatcher] Get latest child list "
                 "failed! Don't have this znode. path: "
              << watch_ctx->path_
              << ", session Handle:" << watch_ctx->zkclient_->getHandle();

    if (watch_ctx->zkclient_->isShouldNotifyChildWatch(watch_ctx->path_) ==
        true) {
      std::vector<std::string> childNodes;
      watch_ctx->child_notify_handler(
          zkutil::kGetChildListFailed_ParentNotExist, watch_ctx->zkclient_,
          watch_ctx->path_, childNodes, watch_ctx->contextInOrignalWatcher_);
    }
  } else {
    LOG_ERROR << "[ZkClient::GetChildDataOnWatcher] Get latest child list "
                 "failed! path: "
              << watch_ctx->path_ << ", rc:" << rc
              << ", session Handle:" << watch_ctx->zkclient_->getHandle();

    if (watch_ctx->zkclient_->isShouldNotifyChildWatch(watch_ctx->path_) ==
        true) {
      std::vector<std::string> childNodes;
      watch_ctx->child_notify_handler(
          zkutil::kGetChildListFailed, watch_ctx->zkclient_, watch_ctx->path_,
          childNodes, watch_ctx->contextInOrignalWatcher_);
    }
  }

  delete watch_ctx;
  watch_ctx = NULL;
}
}  // namespace zkclient
