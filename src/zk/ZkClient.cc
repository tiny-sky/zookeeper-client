#include "ZkClient.h"
#include <assert.h>
#include <time.h>
#include <string>
#include "ZkClientManager.h"
#include "ZkEventLoop.h"
#include "ZkUtil.h"
#include "callback.h"

#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper.jute.h>

#include "muduo/base/Logging.h"

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

    LOG_INFO << "Get StateMutex " << getSessStatStr(state);
    zkclient->setSessStat(state);

    if (state == ZOO_CONNECTED_STATE) {
      LOG_INFO << "get state_mutex";
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
    return;  //如果找不到，说明此session已经销毁了，不用再定时check了.
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
  int rc = zoo_exists(zhandle_, path.c_str(), isWatch, NULL);
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
  } else if (
      ec ==
      zkutil::
          kZKNotExist)  //如果失败，则先尝试 创建里层的 目录结点，然后创建 外层的目录结点
  {
    string::size_type pos = path.rfind('/');
    if (pos == string::npos) {
      LOG_ERROR << "[ZkClient::CreatePersistentDir] Can't find / character, "
                   "create dir failed! path:"
                << path << ", session Handle:" << handle_;
      return false;
    } else {
      std::string parentDir = path.substr(0, pos);
      if (createPersistentDir(parentDir) == true)  //创建父目录成功
      {
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
  } else  //zkutil::kZKSucceed
  {
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

}  // namespace zkclient
