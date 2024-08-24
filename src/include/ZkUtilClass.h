#pragma once

#include <string>
#include "ZkUtil.h"
#include "callback.h"

namespace zkclient {

struct ContextInCreateParentAndNodes {
  ContextInCreateParentAndNodes(const std::string& path,
                                const std::string& value, CreateHandler handler,
                                void* context, bool isTemp, bool isSeq,
                                ZkClientPtr zkclient) {
    path_ = path;
    value_ = value;
    create_handler_ = handler;
    context_ = context;
    isTemp_ = isTemp;
    isSequence_ = isSeq;
    zkclient_ = zkclient;
  }

  std::string path_;
  std::string value_;
  CreateHandler create_handler_;
  void* context_;
  bool isTemp_;
  bool isSequence_;
  ZkClientPtr zkclient_;
};

struct ContextInDeleteRecursive {
  ContextInDeleteRecursive(const std::string& path, DeleteHandler handler,
                           void* context, int32_t version,
                           ZkClientPtr zkclient) {
    path_ = path;
    delete_handler_ = handler;
    context_ = context;
    version_ = version;
    zkclient_ = zkclient;
  }

  std::string path_;
  DeleteHandler delete_handler_;
  void* context_;
  int32_t version_;
  ZkClientPtr zkclient_;
};

struct NodeWatchData {
  NodeWatchData();
  NodeWatchData(const NodeWatchData& data);
  NodeWatchData& operator=(const NodeWatchData& data);

  std::string path_;
  NodeChangeHandler handler_;
  void* context_;
  std::string value_;
  int32_t version_;
  bool isSupportAutoReg_;  //当watcher触发后，是否支持自动再注册watcher
};

struct ChildWatchData {
  ChildWatchData();
  ChildWatchData(const ChildWatchData& data);
  ChildWatchData& operator=(const ChildWatchData& data);

  std::string path_;
  ChildChangeHandler handler_;
  void* context_;
  std::vector<std::string> childList_;
  bool isSupportAutoReg_;  //当watcher触发后，是否支持自动再注册watcher
};

struct ContextInNodeWatcher {
  ContextInNodeWatcher(const std::string& path, ZkClientPtr zkclient,
                       NodeChangeHandler handler, zkutil::ZkNotifyType type,
                       void* context) {
    path_ = path;
    zkclient_ = zkclient;
    node_notify_handler_ = handler;
    notifyType_ = type;
    contextInOrignalWatcher_ = context;
  }

  std::string path_;
  ZkClientPtr zkclient_;
  NodeChangeHandler node_notify_handler_;
  zkutil::ZkNotifyType notifyType_;
  void* contextInOrignalWatcher_;
};

struct ContextInChildWatcher {
  ContextInChildWatcher(const std::string& path, ZkClientPtr zkclient,
                        ChildChangeHandler handler, zkutil::ZkNotifyType type,
                        void* context) {
    path_ = path;
    zkclient_ = zkclient;
    child_notify_handler = handler;
    notifyType_ = type;
    contextInOrignalWatcher_ = context;
  }

  std::string path_;
  ZkClientPtr zkclient_;
  ChildChangeHandler child_notify_handler;
  zkutil::ZkNotifyType notifyType_;
  void* contextInOrignalWatcher_;
};

}  // namespace zkclient
