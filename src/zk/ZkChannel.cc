#include "ZkChannel.h"

#include <assert.h>
#include <sys/epoll.h>
#include <sstream>

#include "ZkEventLoop.h"

#include "muduo/base/Logging.h"

namespace zkclient {

const int ZkChannel::kNoneEvent = 0;
const int ZkChannel::kReadEvent = POLLIN | POLLPRI;
const int ZkChannel::kWriteEvent = POLLOUT;

ZkChannel::ZkChannel(ZkEventLoop* loop, int fd)
    : loop_(loop), fd_(fd), events_(0), revents_(0), index_(-1), tied_(false) {}

ZkChannel::~ZkChannel() {
  if (loop_->isInLoopThread()) {
    assert(!loop_->hasChannel(this));
  }
}

void ZkChannel::tie(const std::shared_ptr<void>& obj) {
  tie_ = obj;
  tied_ = true;
}

void ZkChannel::update() {
  loop_->updateChannel(this);
}

void ZkChannel::remove() {
  loop_->removeChannel(this);
}

std::string ZkChannel::reventsToString() const {
  return eventsToString(fd_, revents_);
}

std::string ZkChannel::eventsToString(int fd, int ev) {
  std::ostringstream oss;
  oss << fd << ": ";
  if (ev & EPOLLIN)
    oss << "IN ";
  if (ev & EPOLLPRI)
    oss << "PRI ";
  if (ev & EPOLLOUT)
    oss << "OUT ";
  if (ev & EPOLLHUP)
    oss << "HUP ";
  if (ev & EPOLLRDHUP)
    oss << "RDHUP ";
  if (ev & EPOLLERR)
    oss << "ERR ";

  return oss.str();
}

void ZkChannel::handleEvent(Timestamp receiveTime) {
  std::shared_ptr<void> guard;
  if (tied_) {
    guard = tie_.lock();
    if (guard) {
      handleEventWithGuard(receiveTime);
    }
  } else {
    handleEventWithGuard(receiveTime);
  }
}

void ZkChannel::handleEventWithGuard(Timestamp receiveTime) {
  LOG_INFO << fd_ << "channel handleEvent revents -> " << reventsToString();

  // shutdown 触发 EPOLLHUP 且无写数据
  if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN)) {
    if (closeCallback_) {
      closeCallback_();
    }
  }

  // 触发错误
  if (revents_ & EPOLLERR) {
    if (errorCallback_) {
      errorCallback_();
    }
  }

  // 读事件
  if (revents_ & (EPOLLIN | EPOLLPRI)) {
    if (readCallback_) {
      readCallback_(receiveTime);
    }
  }
  // 写事件
  if (revents_ & EPOLLOUT) {
    if (writeCallback_) {
      writeCallback_();
    }
  }
}
}  // namespace zkclient
