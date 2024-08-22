#pragma once

#include <atomic>
#include <boost/noncopyable.hpp>
#include <functional>
#include <map>
#include <mutex>
#include <vector>

#include "ZkClientManager.h"
#include "callback.h"

namespace zkclient {

//管理runInThread的回调函数
class CbFunManager : boost::noncopyable {
  public:
  CbFunManager();
  ~CbFunManager();

  using Functor = std::function<void()>;
  friend class ZkClientManager;

  // singleton
  static CbFunManager& instance();

  void runInThread(int thread_id, const Functor& cb);

  //为了加快速度，每个线程的epollfd、eventfd存两份，一份存线程私有数据，一份存threadDatas_
  struct threadData {
    threadData() {
      epollfd_ = 0;
      eventfd_ = 0;
      callingPendingFunctors_ = false;
    }
    int epollfd_;
    int eventfd_;
    volatile bool callingPendingFunctors_;
  };

  private:
  //返回eventfd，在外面 将eventfd的事件 注册到epoll中.
  int insertThreadData(int thread_id, int epollfd);

  bool isInCurrentThread(int thread_id);

  void queueInThreadFuns(int thread_id, const Functor& cb);

  void wakeup(int thread_id);

  void doPendingFunctors(int thread_id);

  // <thread_id, threadData>
  std::map<int, threadData> threadDatas_;
  std::mutex dataMutex_;

  // <thread_id, cbfun_list>
  std::map<int, std::vector<Functor> >* pendingFunctors_;
  std::mutex funsMutex_;
};
}  // namespace zkclient
