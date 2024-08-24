#pragma once

#include <poll.h>
#include <sys/epoll.h>
#include <memory>

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

bool isReadEvent(int events);
bool isWriteEvent(int events);

int createEventfd();

//zookeeper client related
const int32_t kInvalidDataVersion = -1;
const int kMaxNodeValueLength = 32 * 1024;
const int kMaxPathLength = 512;

enum ZkErrorCode {
  kZKSucceed = 0,    // 操作成功,或者 结点存在
  kZKNotExist,       // 节点不存在, 或者 分支结点不存在
  kZKError,          // 请求失败
  kZKDeleted,        // 节点删除
  kZKExisted,        // 节点已存在
  kZKNotEmpty,       // 节点含有子节点
  kZKLostConnection  //与zookeeper server断开连接
};

// Watcher的回调原型
enum ZkNotifyType {
  kNodeDelete = 0,                   // 节点删除
  kNodeCreate,                       // 节点创建
  kNodeChange,                       // 节点的数据变更
  kGetNodeValueFailed_NodeNotExist,  //节点创建 或 数据变更时，再向zookeeper server获取最新数据时 结点已被删除
  kGetNodeValueFailed,  //节点创建 或 数据变更时，再向zookeeper server获取最新数据时 失败
  kChildChange,         // 子节点的变更（增加、删除子节点）
  kGetChildListFailed_ParentNotExist,  //子节点的变更时，再向zookeeper server获取最新子节点列表时 父结点已被删除
  kGetChildListFailed,  //子节点的变更时，再向zookeeper server获取最新子节点列表时 失败
  kTypeError,           //其它错误
};
}  // namespace zkutil

}  // namespace zkclient
