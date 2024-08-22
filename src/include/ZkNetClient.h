#pragma once

#include <muduo/base/Logging.h>
#include <string.h>
#include <atomic>
#include <functional>
#include <string>

#include "boost/noncopyable.hpp"

using namespace muduo;

namespace zkclient {

class ZkNetClient;

#define ZKCLIENT_LOOP_INDEX_INIT 0xFFFFFFFFFFFFFFFE
#define FUNCTION_LOOP_INDEX_INIT 0xFFFFFFFFFFFFFFFF

struct ZkConnChannel : boost::noncopyable {
  public:
  ZkConnChannel(int epollfd, int fd, int events) {
    memset(this, 0, sizeof(*this));
    fd_ = fd;
    events_ = events;
    epollfd_ = epollfd;
  }

  ~ZkConnChannel() {
    LOG_DEBUG << "[~ZkConnChannel] deleting fd:" << fd_;
    close(fd_);
  }

  void update(ZkNetClient* client);

  int fd_;
  int epollfd_;
  int events_;
};

class ZkNetClient : boost::noncopyable {
  public:
  friend class ZkClientManager;
  friend class ZkTimerQueue;

  using ReadTimerCallback = std::function<void()>;

  ZkNetClient(int epollfd, int threadId, int eventfd, std::string netName);

  ~ZkNetClient();

  std::string getNetName() { return netName_; };
  ZkConnChannel* getChannel() { return pConnChannel_; };

  void handleRead();
  void handleEventFdRead(int eventfd);
  void handleTimerFdRead();
  void handleWrite();
  void setReadTimerCb(ReadTimerCallback cb);

  private:
  ZkConnChannel* pConnChannel_;
  ReadTimerCallback timerReadCb_;

  std::string netName_;
  int threadId_;  //一个线程（thread_id）可以有多个sslClient，但一个sslclient只能属于一个线程
  int epollfd_;   //一个线程使用一个epollfd

  std::atomic<uint64_t> loopIndex_;
  std::atomic<uint64_t> loopIndexFunResetChannel_;
  std::atomic<uint64_t> loopIndexFunRetry_;
};
}  // namespace zkclient
