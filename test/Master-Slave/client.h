#pragma once

#include "ZkClient.h"
#include "ZkClientManager.h"
#include "ZkUtil.h"

#include <binders.h>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <string>
#include <vector>

using namespace zkclient;

using std::placeholders::_4;
using std::placeholders::_5;
using std::placeholders::_6;

class Client : boost::noncopyable {
  public:
  Client();
  ~Client();

  bool init(const std::string& zkConnStr);

  bool submit_task(std::string& task);

  std::string task_wait();

  private:
  void submitTask_completion(zkutil::ZkErrorCode errcode,
                             const ZkClientPtr& client, const std::string& path,
                             const std::string& value, void* context);

  void task_watcher(zkutil::ZkNotifyType type, const ZkClientPtr& client,
                    const std::string& path, const std::string& value,
                    int32_t version, void* context);

  std::string task_;
  std::string zkConnStr_;
  ZkClientPtr zkClient_;

  std::string status_; // 本次任务执行结果
  std::mutex mutex_;
  std::condition_variable condition_;
};
