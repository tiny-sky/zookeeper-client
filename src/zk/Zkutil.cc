#include "ZkUtil.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include "ZkNetClient.h"
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

void addEpollFd(int epollfd, ZkNetClient* pClient) {
  modifyEpollEvent(EPOLL_CTL_ADD, epollfd, pClient, "ADD ");
}
void modEpollFd(int epollfd, ZkNetClient* pClient) {
  modifyEpollEvent(EPOLL_CTL_MOD, epollfd, pClient, "MOD ");
}

void delEpollFd(int epollfd, ZkNetClient* pClient) {
  modifyEpollEvent(EPOLL_CTL_DEL, epollfd, pClient, "DEL ");
}

void modifyEpollEvent(int operation, int epollfd, ZkNetClient* pClient,
                      std::string printStr) {
  if (pClient == nullptr || pClient->getChannel() == nullptr) {
    return;
  }
  struct epoll_event ev;
  memset(&ev, 0, sizeof(ev));
  ev.events = pClient->getChannel()->events_;
  ev.data.ptr = pClient;

  LOG_DEBUG << printStr << " fd: " << pClient->getChannel()->fd_
            << " events: (read:" << isReadEvent(ev.events)
            << ", write:" << isWriteEvent(ev.events)
            << ") in epollfd: " << epollfd
            << " in zkHandle: " << pClient->getNetName();
  int r = epoll_ctl(epollfd, operation, pClient->getChannel()->fd_, &ev);
  if (r < 0) {
    LOG_DEBUG << "epoll_ctl operator(oper:" << operation << ", " << printStr
              << ") failed! " << ",errorNo:" << errno
              << ", errDesc:" << strerror(errno)
              << ", fd: " << pClient->getChannel()->fd_
              << " events: (read:" << isReadEvent(ev.events)
              << ", write:" << isWriteEvent(ev.events)
              << "in epollfd: " << epollfd
              << " in zkHandle: " << pClient->getNetName();
  }
}

void enableReading(ZkNetClient* pClient) {
  if (pClient == nullptr || pClient->getChannel() == nullptr) {
    return;
  }
  pClient->getChannel()->events_ |= kReadEvent;
}

void enableWriting(ZkNetClient* pClient) {
  if (pClient == nullptr || pClient->getChannel() == nullptr) {
    return;
  }
  pClient->getChannel()->events_ |= kWriteEvent;
}

void disableWriting(ZkNetClient* pClient) {
  if (pClient == nullptr || pClient->getChannel() == nullptr) {
    return;
  }
  pClient->getChannel()->events_ &= ~kWriteEvent;
}

void disableAll(ZkNetClient* pClient) {
  if (pClient == nullptr || pClient->getChannel() == nullptr) {
    return;
  }
  pClient->getChannel()->events_ = kNoneEvent;
}
}  // namespace zkutil

}  // namespace zkclient
