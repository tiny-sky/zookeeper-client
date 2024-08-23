#include "ZkUtil.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include "muduo/base/Logging.h"

namespace zkclient {
namespace zkutil {

bool isReadEvent(int events) {
  return events & EPOLLIN;
}

bool isWriteEvent(int events) {
  return events & EPOLLOUT;
}

int createEventfd() {
  int evtfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (evtfd < 0) {
    LOG_SYSERR << "Failed in eventfd";
    abort();
  }
  return evtfd;
}
}  // namespace zkutil

}  // namespace zkclient
