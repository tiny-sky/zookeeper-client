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

/*
 * Instances of this struct are
 * used when assigning a task to
 * a worker.
 */

struct task_info {

  task_info(std::string name_, std::string value_, std::string worker_)
      : name(name_), value(value_), worker(worker_) {}
  std::string name;
  std::string value;
  std::string worker;
};

class Master : boost::noncopyable {
  public:
  Master();
  ~Master();

  bool init(const std::string& zkConnStr);

  bool run_for_master();

  private:
  void check_master();
  void take_leadership();
  void master_exists();
  void get_workers();
  void get_tasks();

  void get_task_data(std::string& task);
  void task_assignment(task_info* task);

  /**
 *
 * Assign tasks, but first read task data. In this simple
 * implementation, there is no real task data in the znode,
 * but we include fetching the data to illustrate.
 *
 */
  void assign_tasks(std::vector<std::string>& strings);

  void create_parent_completion(zkutil::ZkErrorCode errcode,
                                const ZkClientPtr& client,
                                const std::string& path,
                                const std::string& value, void* context);

  void master_create_completion(zkutil::ZkErrorCode errcode,
                                const ZkClientPtr& client,
                                const std::string& path,
                                const std::string& value, void* context);

  void workers_completion(zkutil::ZkErrorCode errcode,
                          const ZkClientPtr& client, const std::string& path,
                          const std::vector<std::string>& childNodes,
                          void* context);

  void master_exists_completion(zkutil::ZkErrorCode errcode,
                                const ZkClientPtr& client,
                                const std::string& path, void* context);

  void get_task_data_completion(zkutil::ZkErrorCode errcode,
                                const ZkClientPtr& client,
                                const std::string& path,
                                const std::string& value, int32_t version,
                                void* context);

  void task_assignment_completion(zkutil::ZkErrorCode errcode,
                                  const ZkClientPtr& client,
                                  const std::string& path,
                                  const std::string& value, void* context);

  /*
 * In the case of errors when trying to create the /master lock, we
 * need to check if the znode is there and its content.
 */
  void master_check_completion(zkutil::ZkErrorCode errcode,
                               const ZkClientPtr& client,
                               const std::string& path,
                               const std::string& value, int32_t version,
                               void* context);

  void tasks_completion(zkutil::ZkErrorCode errcode, const ZkClientPtr& client,
                        const std::string& path,
                        const std::vector<std::string>& childNodes,
                        void* context);

  void workers_watcher(zkutil::ZkNotifyType type, const ZkClientPtr& client,
                       const std::string& path,
                       const std::vector<std::string>& childNodes,
                       void* context);

  void tasks_watcher(zkutil::ZkNotifyType type, const ZkClientPtr& client,
                     const std::string& path,
                     const std::vector<std::string>& childNodes, void* context);

  void master_exists_watcher(zkutil::ZkNotifyType type,
                             const ZkClientPtr& client, const std::string& path,
                             const std::string& value, int32_t version,
                             void* context);

  void set_workers(const std::vector<std::string>& current,
                       std::vector<std::string>& previous);

  /*
 * This function returns the elements that are new in current
 * compared to previous and update previous.
 */
  std::vector<std::string> added_and_set(
      const std::vector<std::string>& current,
      std::vector<std::string>& previous);

  std::string zkConnStr_;
  ZkClientPtr zkClient_;
  int serverid_;

  std::vector<std::string> workers;
  std::vector<std::string> tasks;
};

