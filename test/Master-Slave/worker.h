#pragma once

#include "ZkClient.h"
#include "ZkClientManager.h"
#include "ZkUtil.h"

#include <binders.h>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

using namespace zkclient;

using std::placeholders::_4;
using std::placeholders::_5;
using std::placeholders::_6;

enum WorkerCode {
  Down = 0,  // 操作成功
  False,     // 操作失败
  Error,     //出现错误
};

class Worker : boost::noncopyable {
  public:
  Worker(std::string name);
  ~Worker();

  bool init(const std::string& zkConnStr);

  bool Register();

  std::string getip() { return ip_; }

  private:
  void register_completion(zkutil::ZkErrorCode errcode,
                           const ZkClientPtr& client, const std::string& path,
                           const std::string& value, void* context);

  void down_completion(zkutil::ZkErrorCode errcode, const ZkClientPtr& client,
                       const std::string& path, const std::string& value,
                       void* context);

  void task_watcher(zkutil::ZkNotifyType type, const ZkClientPtr& client,
                    const std::string& path,
                    const std::vector<std::string>& childNodes, void* context);
  std::string ip_;
  std::string zkConnStr_;
  ZkClientPtr zkClient_;
};