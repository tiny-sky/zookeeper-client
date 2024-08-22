#pragma once

#include <functional>
#include <memory>

namespace zkclient {

class ZkClient;
using ZkClientPtr = std::shared_ptr<ZkClient>;
using SessionExpiredHandler = std::function<void (const ZkClientPtr& client, void* context)>;
using TimerCallback = std::function<void()>;
}  // namespace zkclient

