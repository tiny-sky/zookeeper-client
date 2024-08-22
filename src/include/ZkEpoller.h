#pragma once

#include <boost/noncopyable.hpp>

#include "ZkEventLoop.h"
#include <map>
#include <vector>

struct epoll_event;

namespace zkclient {

class Channel;
class ZkEventLoop;

class ZkEpoller : boost::noncopyable {
  public:
  using ChannelList = std::vector<ZkChannel*>;

  ZkEpoller(ZkEventLoop* loop);
  ~ZkEpoller();

  Timestamp poll(int timeoutMs, ChannelList* activeChannels);
  void updateChannel(ZkChannel* channel);
  void removeChannel(ZkChannel* channel);

  bool hasChannel(ZkChannel* channel) const;

  private:
  static const int kInitEventListSize = 16;

  void fillActiveChannels(int numEvents, ChannelList* activeChannels) const;
  void update(int operation, ZkChannel* channel);

  using EventList = std::vector<struct epoll_event>;
  using ChannelMap = std::map<int, ZkChannel*>;

  ZkEventLoop* loop_;
  ChannelMap channels_;
  int epollfd_;
  EventList events_;
};

}  // namespace zkclient
