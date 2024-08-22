#include "ZkEventLoop.h"
#include <sys/eventfd.h>
#include "ZkChannel.h"
#include "ZkEpoller.h"

#include <binders.h>
#include <errno.h>
#include <fcntl.h>

#include "muduo/base/CurrentThread.h"
#include "muduo/base/Logging.h"

#include "CbFunManager.h"
#include "ZkEventLoop.h"
#include "ZkTimerQueue.h"

namespace zkclient {

// 防止一个线程创建多个EventLoop
__thread ZkEventLoop* t_loopInThisThread = nullptr;

// 定义默认的Poller IO复用接口的超时时间
const int kPollTimeMs = 10000;  // 10000毫秒 = 10秒钟

int createEventfd() {
  int evtfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (evtfd < 0) {
    LOG_ERROR << "eventfd error: " << errno;
  }
  return evtfd;
}

ZkEventLoop::ZkEventLoop()
    : looping_(false),
      quit_(false),
      threadId_(muduo::CurrentThread::tid()),
      poller_(new ZkEpoller(this)),
      wakeupFd_(createEventfd()),
      timerQueue_(new ZkTimerQueue(this)),
      wakeupChannel_(new ZkChannel(this, wakeupFd_)) {

  LOG_INFO << "Create EventLoop in thread -> " << "[" << threadId_ << "]";
  if (t_loopInThisThread) {
    LOG_FATAL << "Another EventLoop " << t_loopInThisThread
              << " exists in this thread " << threadId_;
  } else {
    t_loopInThisThread = this;
  }

  wakeupChannel_->setReadCallback(std::bind(&ZkEventLoop::handleRead, this));
  wakeupChannel_->enableReading();
}

ZkEventLoop::~ZkEventLoop() {
  wakeupChannel_->disableAll();
  wakeupChannel_->remove();
  ::close(wakeupFd_);
  t_loopInThisThread = nullptr;
}

void ZkEventLoop::loop() {
  assert(!looping_);
  looping_ = true;
  quit_ = false;
  LOG_TRACE << "EventLoop " << this << " start looping";

  while (!quit_) {
    activeChannels_.clear();
    pollReturnTime_ = poller_->poll(kPollTimeMs, &activeChannels_);

    if (Logger::logLevel() <= Logger::TRACE) {
      printActiveChannels();
    }

    eventHandling_ = true;
    for (ZkChannel* channel : activeChannels_) {
      currentActiveChannel_ = channel;
      currentActiveChannel_->handleEvent(pollReturnTime_);
    }
    currentActiveChannel_ = nullptr;
    eventHandling_ = false;
    doPendingFunctors();
  }

  LOG_TRACE << "EventLoop " << this << " stop looping";
  looping_ = false;
}

void ZkEventLoop::runInLoop(Functor cb) {
  if (isInLoopThread()) {
    cb();
  } else {
    queueInLoop(std::move(cb));
  }
}

void ZkEventLoop::queueInLoop(Functor cb) {
  {
    std::lock_guard<std::mutex> lock(pendingMutex_);
    pendingFunctors_.push_back(std::move(cb));
  }

  if (!isInLoopThread() || callingPendingFunctors_) {
    wakeup();
  }
}

TimerId ZkEventLoop::runAt(Timestamp time, TimerCallback cb) {
  return timerQueue_->addTimer(std::move(cb), time, 0.0);
}

TimerId ZkEventLoop::runAfter(double delay, TimerCallback cb) {
  Timestamp time(addTime(Timestamp::now(), delay));
  return runAt(time, std::move(cb));
}

TimerId ZkEventLoop::runEvery(double interval, TimerCallback cb) {
  Timestamp time(addTime(Timestamp::now(), interval));
  return timerQueue_->addTimer(std::move(cb), time, interval);
}

void ZkEventLoop::cancel(TimerId timerId) {
  return timerQueue_->cancel(timerId);
}

void ZkEventLoop::handleRead() {
  uint64_t one = 1;
  ssize_t n = read(wakeupFd_, &one, sizeof(one));
  if (n != sizeof(one)) {
    LOG_ERROR << "EventLoop::wakeup() writes " << n << " bytes instead of 8";
  }
}

void ZkEventLoop::updateChannel(ZkChannel* channel) {
  poller_->updateChannel(channel);
}

void ZkEventLoop::removeChannel(ZkChannel* channel) {
  poller_->removeChannel(channel);
}

bool ZkEventLoop::hasChannel(ZkChannel* channel) {
  return poller_->hasChannel(channel);
}

void ZkEventLoop::printActiveChannels() const {
  for (const ZkChannel* channel : activeChannels_) {
    LOG_TRACE << "{" << channel->reventsToString() << "} ";
  }
}

void ZkEventLoop::quit() {
  quit_.store(true, std::memory_order_release);

  if (!isInLoopThread()) {
    wakeup();
  }
}

void ZkEventLoop::wakeup() {
  uint64_t one = 1;
  ssize_t n = write(wakeupFd_, &one, sizeof(one));
  if (n != sizeof(one)) {
    LOG_ERROR << "EventLoop::wakeup() writes " << n << "bytes instead of 8";
  }
}

void ZkEventLoop::doPendingFunctors() {
  std::vector<Functor> functors;
  callingPendingFunctors_.store(true, std::memory_order_release);

  {
    std::unique_lock<std::mutex> lock(pendingMutex_);
    functors.swap(pendingFunctors_);
  }

  for (const Functor& functor : functors) {
    functor();
  }

  callingPendingFunctors_.store(false, std::memory_order_release);
}

}  // namespace zkclient
