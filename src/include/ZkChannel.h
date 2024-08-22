#pragma once

#include <functional>
#include <memory>

#include <boost/noncopyable.hpp>

#include "muduo/base/Timestamp.h"

#include "ZkUtil.h"

using namespace muduo;

namespace zkclient {

class ZkEventLoop;

class ZkChannel : boost::noncopyable {
  public:
  using EventCallback = std::function<void()>;
  using ReadEventCallback = std::function<void(Timestamp)>;

  ZkChannel(ZkEventLoop* loop, int fd);
  ~ZkChannel();

  // Poller 通知 fd 处理事件
  void handleEvent(Timestamp receiveTime);

  // 设置回调函数对象
  void setReadCallback(ReadEventCallback cb) { readCallback_ = std::move(cb); }
  void setWriteCallback(EventCallback cb) { writeCallback_ = std::move(cb); }
  void setCloseCallback(EventCallback cb) { closeCallback_ = std::move(cb); }
  void setErrorCallback(EventCallback cb) { errorCallback_ = std::move(cb); }

  //
  void tie(const std::shared_ptr<void>&);

  int fd() const { return fd_; }
  int events() const { return events_; }
  void set_revents(int revt) { revents_ = revt; }

  // 设置fd相应的事件状态
  void enableReading() {
    events_ |= kReadEvent;
    update();
  }
  void disableReading() {
    events_ &= ~kReadEvent;
    update();
  }
  void enableWriting() {
    events_ |= kWriteEvent;
    update();
  }
  void disableWriting() {
    events_ &= ~kWriteEvent;
    update();
  }
  void disableAll() {
    events_ = kNoneEvent;
    update();
  }

  // 判断当前的状态
  bool isNoneEvent() const { return events_ == kNoneEvent; }
  bool isWriting() const { return events_ & kWriteEvent; }
  bool isReading() const { return events_ & kReadEvent; }

  int index() { return index_; }
  void set_index(int idx) { index_ = idx; }

  // one loop per thread
  ZkEventLoop* ownerLoop() { return loop_; }
  void remove();

  // for debug
  std::string reventsToString() const;
  std::string eventsToString() const;

  private:
  static std::string eventsToString(int fd, int ev);
  void update();
  void handleEventWithGuard(Timestamp receiveTime);

  static const int kNoneEvent;
  static const int kReadEvent;
  static const int kWriteEvent;

  ZkEventLoop* loop_;  // 事件循环
  const int fd_;       // fd，Poller监听的对象
  int events_;         // 注册fd感兴趣的事件
  int revents_;        // Poller返回的具体发生的事件
  int index_;

  std::weak_ptr<void> tie_;
  bool tied_;

  ReadEventCallback readCallback_;
  EventCallback writeCallback_;
  EventCallback closeCallback_;
  EventCallback errorCallback_;
};

}  // namespace zkclient
