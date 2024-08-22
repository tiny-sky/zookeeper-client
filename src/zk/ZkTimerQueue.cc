#define once

#include "ZkTimerQueue.h"
#include "CbFunManager.h"
#include "ZkEventLoop.h"

#include <binders.h>
#include <sys/timerfd.h>

#include "ZkClientManager.h"
#include "ZkUtil.h"
#include "muduo/base/Logging.h"
#include "muduo/net/Callbacks.h"
#include "muduo/net/Timer.h"

using namespace muduo;

namespace {
int createTimerfd() {
  int timerfd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (timerfd < 0) {
    LOG_SYSFATAL << "Failed in timerfd_create";
  }
  return timerfd;
}

struct timespec howMuchTimeFromNow(Timestamp when) {
  int64_t microseconds =
      when.microSecondsSinceEpoch() - Timestamp::now().microSecondsSinceEpoch();
  if (microseconds < 100) {
    microseconds = 100;
  }
  struct timespec ts;
  ts.tv_sec =
      static_cast<time_t>(microseconds / Timestamp::kMicroSecondsPerSecond);
  ts.tv_nsec = static_cast<long>(
      (microseconds % Timestamp::kMicroSecondsPerSecond) * 1000);
  LOG_DEBUG << "ts.tv_nsec:[" << ts.tv_nsec << "]----ts.tv.sec:[" << ts.tv_sec
            << "]";
  return ts;
}

void resetTimerfd(int timerfd, Timestamp expiration) {
  // wake up loop by timerfd_settime()
  struct itimerspec newValue;
  struct itimerspec oldValue;
  bzero(&newValue, sizeof newValue);
  bzero(&oldValue, sizeof oldValue);
  newValue.it_value = howMuchTimeFromNow(expiration);
  int ret = ::timerfd_settime(timerfd, 0, &newValue, &oldValue);
  if (ret) {
    LOG_SYSERR << "timerfd_settime()";
  }
}

void readTimerfd(int timerfd, Timestamp now) {
  uint64_t howmany;
  ssize_t n = ::read(timerfd, &howmany, sizeof howmany);
  LOG_TRACE << "TimerQueue::handleRead() " << howmany << " at "
            << now.toString();
  if (n != sizeof howmany) {
    LOG_ERROR << "TimerQueue::handleRead() reads " << n
              << " bytes instead of 8";
  }
}

}  // namespace

namespace zkclient {

ZkTimerQueue::ZkTimerQueue(ZkEventLoop* loop)
    : loop_(loop),
      timerfd_(createTimerfd()),
      timerfdChannel_(loop, timerfd_),
      timers_(),
      callingExpiredTimers_(false) {
  timerfdChannel_.setReadCallback(std::bind(&ZkTimerQueue::handleRead, this));
  timerfdChannel_.enableReading();
}

ZkTimerQueue::~ZkTimerQueue() {
  timerfdChannel_.disableAll();
  timerfdChannel_.remove();
  ::close(timerfd_);
  // do not remove channel, since we're in EventLoop::dtor();
  for (const Entry& timer : timers_) {
    delete timer.second;
  }
}

TimerId ZkTimerQueue::addTimer(const TimerCallback& cb, Timestamp when,
                               double interval) {
  Timer* timer = new Timer(std::move(cb), when, interval);
  loop_->runInLoop(std::bind(&ZkTimerQueue::addTimerInLoop, this, timer));
  return TimerId(timer, timer->sequence());
}

void ZkTimerQueue::addTimerInLoop(Timer* timer) {
  bool earliestChanged = insert(timer);

  if (earliestChanged) {
    resetTimerfd(timerfd_, timer->expiration());
  }
}

bool ZkTimerQueue::insert(Timer* timer) {
  assert(timers_.size() == activeTimers_.size());
  bool earliestChanged = false;
  Timestamp when = timer->expiration();
  TimerList::iterator it = timers_.begin();
  if (it == timers_.end() || when < it->first) {
    earliestChanged = true;
  }
  {
    std::pair<TimerList::iterator, bool> result =
        timers_.insert(Entry(when, timer));
    assert(result.second);
    (void)result;
  }
  {
    std::pair<ActiveTimerSet::iterator, bool> result =
        activeTimers_.insert(ActiveTimer(timer, timer->sequence()));
    assert(result.second);
    (void)result;
  }
  assert(timers_.size() == activeTimers_.size());
  return earliestChanged;
}

void ZkTimerQueue::cancel(TimerId timerId) {
  loop_->runInLoop(std::bind(&ZkTimerQueue::cancelInLoop, this, timerId));
}

void ZkTimerQueue::cancelInLoop(TimerId timerId) {
  assert(timers_.size() == activeTimers_.size());
  ActiveTimer timer(timerId.timer_, timerId.sequence_);
  ActiveTimerSet::iterator it = activeTimers_.find(timer);
  if (it != activeTimers_.end()) {
    std::size_t n = timers_.erase(Entry(it->first->expiration(), it->first));
    assert(n == 1);
    (void)n;
    delete it->first;  // FIXME: no delete please
    activeTimers_.erase(it);
  } else if (callingExpiredTimers_) {
    cancelingTimers_.insert(timer);
  }
  assert(timers_.size() == activeTimers_.size());
}

void ZkTimerQueue::reset(const std::vector<Entry>& expired, Timestamp now) {
  Timestamp nextExpire;

  for (std::vector<Entry>::const_iterator it = expired.begin();
       it != expired.end(); ++it) {
    ActiveTimer timer(it->second, it->second->sequence());
    if (it->second->repeat() &&
        cancelingTimers_.find(timer) == cancelingTimers_.end()) {
      it->second->restart(now);
      insert(it->second);
    } else {
      // FIXME move to a free list
      delete it->second;  // FIXME: no delete please
    }
  }

  if (!timers_.empty()) {
    nextExpire = timers_.begin()->second->expiration();
  }

  if (nextExpire.valid()) {
    resetTimerfd(timerfd_, nextExpire);
  }
}

void ZkTimerQueue::handleRead() {
  //loop_->assertInLoopThread();
  LOG_DEBUG << "--func:[" << __FUNCTION__ << "]---line:[" << __LINE__ << "]";
  Timestamp now(Timestamp::now());
  readTimerfd(timerfd_, now);

  std::vector<Entry> expired = getExpired(now);

  callingExpiredTimers_ = true;
  cancelingTimers_.clear();
  // safe to callback outside critical section
  for (std::vector<Entry>::iterator it = expired.begin(); it != expired.end();
       ++it) {
    it->second->run();
  }
  callingExpiredTimers_ = false;

  reset(expired, now);
}

std::vector<ZkTimerQueue::Entry> ZkTimerQueue::getExpired(Timestamp now) {
  assert(timers_.size() == activeTimers_.size());
  std::vector<Entry> expired;
  Entry sentry(now, reinterpret_cast<Timer*>(UINTPTR_MAX));
  TimerList::iterator end = timers_.lower_bound(sentry);
  assert(end == timers_.end() || now < end->first);
  std::copy(timers_.begin(), end, back_inserter(expired));
  timers_.erase(timers_.begin(), end);

  for (std::vector<Entry>::iterator it = expired.begin(); it != expired.end();
       ++it) {
    ActiveTimer timer(it->second, it->second->sequence());
    std::size_t n = activeTimers_.erase(timer);
    assert(n == 1);
    (void)n;
  }

  assert(timers_.size() == activeTimers_.size());
  return expired;
}

}  // namespace zkclient
