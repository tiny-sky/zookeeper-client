#include "CbFunManager.h"
#include "ZkUtil.h"

#include "muduo/base/Singleton.h"

#include "muduo/base/Logging.h"

#include <memory>

namespace zkclient {

CbFunManager::CbFunManager() {
  threadDatas_.clear();
  pendingFunctors_ = new std::map<int, std::vector<Functor> >();
}

CbFunManager::~CbFunManager() {
  dataMutex_.lock();
  std::unique_lock<std::mutex> data_lock(dataMutex_);
  threadDatas_.clear();
  data_lock.unlock();

  std::unique_lock<std::mutex> funs_lock(funsMutex_);
  if (pendingFunctors_) {
    pendingFunctors_->clear();
    delete pendingFunctors_;
    pendingFunctors_ = nullptr;
  }
  funs_lock.unlock();
}

void CbFunManager::runInThread(int thread_id, const Functor& cb) {
  if (isInCurrentThread(thread_id)) {
    cb();
  } else {
    queueInThreadFuns(thread_id, cb);
  }
}

void CbFunManager::queueInThreadFuns(int thread_id, const Functor& cb) {
  funsMutex_.lock();
  if (pendingFunctors_ != NULL) {
    if (pendingFunctors_->find(thread_id) == pendingFunctors_->end()) {
      pendingFunctors_->insert(
          std::make_pair(thread_id, std::vector<Functor>()));
      (*pendingFunctors_)[thread_id].clear();
    }
    (*pendingFunctors_)[thread_id].push_back(cb);
  } else {
    LOG_ERROR << "pendingFunctors_ is NULL!";
  }
  funsMutex_.unlock();

  wakeup(thread_id);
}

void CbFunManager::wakeup(int thread_id) {
  threadData data;
  bool getData = false;

  dataMutex_.lock();
  if (threadDatas_.find(thread_id) != threadDatas_.end()) {
    data = threadDatas_[thread_id];
    getData = true;
  }
  dataMutex_.unlock();

  if (getData == false) {
    LOG_ERROR << "Can't find this thread data.thread_id:" << thread_id;
    return;
  }

  if (!isInCurrentThread(thread_id) || data.callingPendingFunctors_) {
    uint64_t one = 1;
    ssize_t n = ::write(data.eventfd_, &one, sizeof one);
    if (n != sizeof one) {
      LOG_ERROR << "cbFunManager::wakeup() writes " << n
                << " bytes instead of 8";
    }
  }
}

bool CbFunManager::isInCurrentThread(int thread_id) {
  if (muduo::CurrentThread::tid() == thread_id) {
    return true;
  } else {
    return false;
  }
}

CbFunManager& CbFunManager::instance() {
  return muduo::Singleton<CbFunManager>::instance();
}

//返回eventfd，在外面 将eventfd的事件 注册到epoll中.
int CbFunManager::insertThreadData(int thread_id, int epollfd) {
  threadData data;
  data.eventfd_ = zkutil::createEventfd();
  data.epollfd_ = epollfd;
  data.callingPendingFunctors_ = false;

  std::lock_guard<std::mutex> lock(dataMutex_);
  threadDatas_[thread_id] = data;

  return data.eventfd_;
}

void CbFunManager::doPendingFunctors(int thread_id) {
  std::unique_lock<std::mutex> data_lock(dataMutex_);
  if (threadDatas_.find(thread_id) != threadDatas_.end()) {
    threadDatas_[thread_id].callingPendingFunctors_ = true;
  }
  data_lock.unlock();

  std::vector<Functor> functors;
  std::unique_lock<std::mutex> func_lock(funsMutex_);
  if (pendingFunctors_ != nullptr) {
    if (pendingFunctors_->find(thread_id) != pendingFunctors_->end()) {
      functors.swap((*pendingFunctors_)[thread_id]);
    }
  } else {
    LOG_ERROR << "pendingFunctors_ is NULL!";
  }
  func_lock.unlock();

  for (std::size_t i = 0; i < functors.size(); ++i) {
    functors[i]();
  }

  data_lock.lock();
  if (threadDatas_.find(thread_id) != threadDatas_.end()) {
    threadDatas_[thread_id].callingPendingFunctors_ = false;
  }
  data_lock.unlock();
}

}  // namespace zkclient
