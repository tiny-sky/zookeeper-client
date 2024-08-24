#include "ZkUtilClass.h"

namespace zkclient {

NodeWatchData::NodeWatchData() {
  path_ = "";
  handler_ = nullptr;
  context_ = nullptr;
  value_ = "";
  version_ = zkutil::kInvalidDataVersion;
  isSupportAutoReg_ = true;
}

NodeWatchData::NodeWatchData(const NodeWatchData& data) {
  if (this == &data)
    return;

  this->path_ = data.path_;
  this->handler_ = data.handler_;
  this->context_ = data.context_;
  this->value_ = data.value_;
  this->version_ = data.version_;
  this->isSupportAutoReg_ = data.isSupportAutoReg_;
}

NodeWatchData& NodeWatchData::operator=(const NodeWatchData& data) {
  if (this == &data)
    return *this;

  this->path_ = data.path_;
  this->handler_ = data.handler_;
  this->context_ = data.context_;
  this->value_ = data.value_;
  this->version_ = data.version_;
  this->isSupportAutoReg_ = data.isSupportAutoReg_;

  return *this;
}

ChildWatchData::ChildWatchData() {
  path_ = "";
  handler_ = nullptr;
  context_ = nullptr;
  childList_.clear();
  isSupportAutoReg_ = true;
}

ChildWatchData::ChildWatchData(const ChildWatchData& data) {
  if (this == &data)
    return;

  this->path_ = data.path_;
  this->handler_ = data.handler_;
  this->context_ = data.context_;
  this->childList_.assign(data.childList_.begin(), data.childList_.end());
  this->isSupportAutoReg_ = data.isSupportAutoReg_;
}

ChildWatchData& ChildWatchData::operator=(
    const ChildWatchData& data) {
  if (this == &data)
    return *this;

  this->path_ = data.path_;
  this->handler_ = data.handler_;
  this->context_ = data.context_;
  this->childList_.assign(data.childList_.begin(), data.childList_.end());
  this->isSupportAutoReg_ = data.isSupportAutoReg_;

  return *this;
}
}  // namespace zkclient
