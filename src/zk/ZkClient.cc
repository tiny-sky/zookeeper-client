#include "ZkClient.h"
#include <assert.h>
#include <time.h>
#include <string>
#include "ZkClientManager.h"
#include "ZkEventLoop.h"
#include "ZkNetClient.h"
#include "ZkUtil.h"
#include "callback.h"

#include "muduo/base/Logging.h"

namespace zkclient {

void ZkConnChannel::update(ZkNetClient* client) {
  zkutil::modEpollFd(epollfd_, client);
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

}  // namespace zkclient
