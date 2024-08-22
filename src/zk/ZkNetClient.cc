#pragma once

#include "ZkNetClient.h"
#include "ZkUtil.h"

namespace zkclient {

#define ZKCLIENT_LOOP_INDEX_INIT 0xFFFFFFFFFFFFFFFE
#define FUNCTION_LOOP_INDEX_INIT 0xFFFFFFFFFFFFFFFF

__thread int t_eventfd;
__thread int t_timerfd;

ZkNetClient::ZkNetClient(int epollfd, int threadId, int eventfd,
                         std::string netName) {
  pConnChannel_ = new ZkConnChannel(epollfd, eventfd, zkutil::kNoneEvent);
  epollfd_ = epollfd;
  threadId_ = threadId;
  loopIndex_ = ZKCLIENT_LOOP_INDEX_INIT;
  loopIndexFunResetChannel_ = FUNCTION_LOOP_INDEX_INIT;
  loopIndexFunRetry_ = FUNCTION_LOOP_INDEX_INIT;
  netName_ = netName;
}

ZkNetClient::~ZkNetClient() {
  if (pConnChannel_) {
    delete pConnChannel_;
    pConnChannel_ = nullptr;
  }
}

void ZkNetClient::handleRead() {
  if (pConnChannel_ == nullptr)
    return;

  //如果是 eventfd的事件，则调用 handleEventFdRead
  if (pConnChannel_->fd_ == t_eventfd) {
    LOG_DEBUG << "pick the event. eventfd:" << t_eventfd
              << ", clientName:" << netName_;
    handleEventFdRead(t_eventfd);
    return;
  }

  //如果是 timerfd的事件，则调用 handleTimerFdRead
  if (pConnChannel_->fd_ == t_timerfd) {
    LOG_DEBUG << "pick the event. timerfd:" << t_timerfd
              << ", clientName:" << netName_;
    handleTimerFdRead();
    return;
  }
}

void ZkNetClient::handleEventFdRead(int eventfd) {
  uint64_t one = 1;
  ssize_t n = ::read(eventfd, &one, sizeof one);
  if (n != sizeof one) {
    LOG_ERROR << "ZkNetClient::handleEventFdRead() reads " << n
              << " bytes instead of 8";
  }
}

void ZkNetClient::handleTimerFdRead() {
  if (timerReadCb_) {
    timerReadCb_();
  }
}

void ZkNetClient::handleWrite() {
  if (pConnChannel_ == nullptr)
    return;

  LOG_DEBUG << "[pConnChannel_::handleWrite] fd:" << pConnChannel_->fd_
            << ", netName:" << netName_;
  //去掉 监听写事件
  if (pConnChannel_) {
    pConnChannel_->events_ &= ~EPOLLOUT;
    pConnChannel_->update(this);
  }
}

void ZkNetClient::setReadTimerCb(ReadTimerCallback cb) {
  timerReadCb_ = cb;
}

}  // namespace zkclient
