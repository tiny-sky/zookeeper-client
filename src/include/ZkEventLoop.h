#pragma once

#include <atomic>
#include <functional>
#include <memory>

#include <boost/noncopyable.hpp>

#include <muduo/base/Timestamp.h>
#include <muduo/net/TimerId.h>
#include <muduo/base/CurrentThread.h>

#include "ZkChannel.h"
#include "ZkEpoller.h"
#include "ZkTimerQueue.h"
#include "callback.h"

using namespace muduo;
using namespace muduo::net;

namespace zkclient {

class ZkEpoller;

class ZkEventLoop : boost::noncopyable {
  public:
  using Functor = std::function<void()>;

  ZkEventLoop();
  ~ZkEventLoop();

  void loop();

  void quit();

  ///
  /// Runs callback at 'time'.
  /// Safe to call from other threads.
  ///
  TimerId runAt(Timestamp time, TimerCallback cb);
  ///
  /// Runs callback after @c delay seconds.
  /// Safe to call from other threads.
  ///
  TimerId runAfter(double delay, TimerCallback cb);
  ///
  /// Runs callback every @c interval seconds.
  /// Safe to call from other threads.
  ///
  TimerId runEvery(double interval, TimerCallback cb);
  ///
  /// Cancels the timer.
  /// Safe to call from other threads.
  ///
  void cancel(TimerId timerId);


  void runInLoop(Functor cb);
  void queueInLoop(Functor cb);

  void wakeup();

  bool hasChannel(ZkChannel* channel);
  void updateChannel(ZkChannel* channel);
  void removeChannel(ZkChannel* channel);

  bool isInLoopThread() const { return threadId_ == CurrentThread::tid(); }

  private:
  void handleRead();  // 处理来自唤醒文件描述符的读
  void printActiveChannels() const;
  void doPendingFunctors();  // 执行队列中的回调。

  using ChannelList = std::vector<ZkChannel*>;

  std::atomic<bool> looping_;  // 如果循环当前正在运行，则为真。
  std::atomic<bool> quit_;
  bool eventHandling_; /* atomic */

  const pid_t threadId_;

  Timestamp pollReturnTime_;
  std::unique_ptr<ZkEpoller> poller_;
  std::unique_ptr<ZkTimerQueue> timerQueue_;
  int wakeupFd_;
  // unlike in TimerQueue, which is an internal class,
  // we don't expose Channel to client.
  std::unique_ptr<ZkChannel> wakeupChannel_;

  ChannelList activeChannels_;
  ZkChannel* currentActiveChannel_;

  std::atomic<bool> callingPendingFunctors_;  // 如果有待执行的回调，则为真。
  std::vector<Functor> pendingFunctors_;  // 要执行的队列回调。
  std::mutex pendingMutex_;  // 保护`pendingFunctors`访问的互斥锁。
};

}  // namespace zkclient
