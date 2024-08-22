#pragma once

#include <poll.h>
#include <sys/epoll.h>
#include <memory>
#include "callback.h"
#include "ZkNetClient.h"

namespace zkclient {
namespace zkutil {

//common
const int kThisEpollTimeMs = 10000;
const int kNoneEvent = 0;
const int kReadEvent = POLLIN | POLLPRI;
const int kWriteEvent = POLLOUT;

const int kMaxRetryDelay = 10 * 60;  //单位: 秒
const int kInitRetryDelay = 5;       //单位: 秒

const char* strerror_tl(int savedErrno);

int setNonBlock(int fd, bool value);

bool isReadEvent(int events);
bool isWriteEvent(int events);

void modifyEpollEvent(int operation, int epollfd, ZkNetClient* pClient,
                      std::string printStr);

void addEpollFd(int epollfd, ZkNetClient* pClient);

void modEpollFd(int epollfd, ZkNetClient* pClient);

void delEpollFd(int epollfd, ZkNetClient* pClient);

void enableReading(ZkNetClient* pClient);
void enableWriting(ZkNetClient* pClient);
void disableWriting(ZkNetClient* pClient);
void disableAll(ZkNetClient* pClient);

int getSocketError(int sockfd);

int createEventfd();

//zookeeper client related
const int32_t kInvalidDataVersion = -1;
const int kMaxNodeValueLength = 32 * 1024;
const int kMaxPathLength = 512;

using SessionExpiredHandler =
    std::function<void(const ZkClientPtr& client, void* context)>;

enum ZkErrorCode {
  kZKSucceed = 0,    // 操作成功,或者 结点存在
  kZKNotExist,       // 节点不存在, 或者 分支结点不存在
  kZKError,          // 请求失败
  kZKDeleted,        // 节点删除
  kZKExisted,        // 节点已存在
  kZKNotEmpty,       // 节点含有子节点
  kZKLostConnection  //与zookeeper server断开连接
};
}  // namespace zkutil

}  // namespace zkclient
