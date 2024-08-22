#include "ZkEpoller.h"

#include <assert.h>
#include <sys/epoll.h>

#include <muduo/base/Logging.h>

namespace {
const int kNew = -1;
const int kAdded = 1;
const int kDeleted = 2;
}  // namespace

namespace zkclient {

ZkEpoller::ZkEpoller(ZkEventLoop* loop)
    : loop_(loop),
      epollfd_(::epoll_create1(EPOLL_CLOEXEC)),
      events_(kInitEventListSize) {
  if (epollfd_ < 0) {
    LOG_SYSFATAL << "EPollPoller::EPollPoller";
  }
}

ZkEpoller::~ZkEpoller() {
  ::close(epollfd_);
}

bool ZkEpoller::hasChannel(ZkChannel *channel) const {
  auto it = channels_.find(channel->fd());
  return it != channels_.end() && it->second == channel;
}

void ZkEpoller::updateChannel(ZkChannel* channel) {
  const int index = channel->index();
  LOG_TRACE << "fd = " << channel->fd() << " events = " << channel->events()
            << " index = " << index;

  if (index == kNew || index == kDeleted) {
    // a new one, add with EPOLL_CTL_ADD
    int fd = channel->fd();
    if (index == kNew) {
      assert(channels_.find(fd) == channels_.end());
      channels_[fd] = channel;
    } else {
      assert(channels_.find(fd) != channels_.end());
      assert(channels_[fd] == channel);
    }

    channel->set_index(kAdded);
    update(EPOLL_CTL_ADD, channel);
  } else {
    int fd = channel->fd();
    assert(channels_.find(fd) != channels_.end());

    if (channel->isNoneEvent()) {
      update(EPOLL_CTL_DEL, channel);
      channel->set_index(kDeleted);
    } else {
      update(EPOLL_CTL_MOD, channel);
    }
  }
}

void ZkEpoller::removeChannel(ZkChannel* channel) {
  int fd = channel->fd();
  channels_.erase(fd);

  LOG_INFO << "func=" << __FUNCTION__ << " => fd=" << fd;

  int index = channel->index();
  if (index == kAdded) {
    update(EPOLL_CTL_DEL, channel);
  }

  channel->set_index(kNew);
}

void ZkEpoller::fillActiveChannels(int numEvents,
                                   ChannelList* activeChannels) const {
  for (int i = 0; i < numEvents; ++i) {
    ZkChannel* channel = static_cast<ZkChannel*>(events_[i].data.ptr);
    channel->set_revents(events_[i].events);
    activeChannels->push_back(channel);
  }
}

void ZkEpoller::update(int operation, ZkChannel* channel) {
  epoll_event event;
  ::memset(&event, 0, sizeof(event));

  int fd = channel->fd();

  event.events = channel->events();
  event.data.fd = fd;
  event.data.ptr = channel;

  if (::epoll_ctl(epollfd_, operation, fd, &event) < 0) {
    if (operation == EPOLL_CTL_DEL) {
      LOG_ERROR << "epoll_ctl del error: " << errno;
    } else {
      LOG_ERROR << "epoll_ctl add/mod error: " << errno;
    }
  }
}

Timestamp ZkEpoller::poll(int timeoutMs, ChannelList* activeChannels) {
  LOG_TRACE << "fd total count " << channels_.size();
  int numEvents = ::epoll_wait(epollfd_, &*events_.begin(),
                               static_cast<int>(events_.size()), timeoutMs);
  int savedErrno = errno;
  Timestamp now(Timestamp::now());
  if (numEvents > 0) {
    LOG_TRACE << numEvents << " events happened";
    fillActiveChannels(numEvents, activeChannels);
    if (implicit_cast<std::size_t>(numEvents) == events_.size()) {
      events_.resize(events_.size() * 2);
    }
  } else if (numEvents == 0) {
    LOG_TRACE << "nothing happened";
  } else {
    // error happens, log uncommon ones
    if (savedErrno != EINTR) {
      errno = savedErrno;
      LOG_SYSERR << "EPollPoller::poll()";
    }
  }
  return now;
}
}  // namespace zkclient
