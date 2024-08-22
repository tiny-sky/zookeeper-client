#pragma once

#include <set>
#include <vector>

#include <boost/noncopyable.hpp>

#include "muduo/base/Timestamp.h"
#include "muduo/net/Callbacks.h"

#include "muduo/net/Timer.h"

#include "ZkClient.h"
#include "ZkChannel.h"

using namespace muduo::net;

namespace zkclient {

class ZkEventLoop;

class TimerId {
  public:
  TimerId() : timer_(nullptr), sequence_(0) {}

  TimerId(Timer* timer, int64_t seq) : timer_(timer), sequence_(seq) {}

  // default copy-ctor, dtor and assignment are okay

  friend class ZkTimerQueue;

  private:
  Timer* timer_;
  int64_t sequence_;
};

class ZkTimerQueue {
  public:
  ZkTimerQueue(ZkEventLoop* loop);
  ~ZkTimerQueue();

  TimerId addTimer(const TimerCallback& cb, Timestamp when, double interval);

  void cancel(TimerId timerId);

  private:
  using Entry = std::pair<Timestamp, Timer*>;
  using TimerList = std::set<Entry>;
  using ActiveTimer = std::pair<Timer*, int64_t>;
  using ActiveTimerSet = std::set<ActiveTimer>;

  void addTimerInLoop(Timer* timer);
  void cancelInLoop(TimerId timerId);

  void handleRead();

  std::vector<Entry> getExpired(Timestamp now);

  void reset(const std::vector<Entry>& expired, Timestamp now);

  bool insert(Timer* timer);

  ZkEventLoop* loop_;
  int timerfd_;

  ZkChannel timerfdChannel_;
  // Timer list sorted by expiration
  TimerList timers_;

  // for cancel()
  ActiveTimerSet activeTimers_;
  bool callingExpiredTimers_; /* atomic */
  ActiveTimerSet cancelingTimers_;
};
}  // namespace zkclient
