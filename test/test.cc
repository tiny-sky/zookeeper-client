#include <assert.h>
#include <unistd.h>
#include <iostream>

#include <algorithm>
#include <string>
#include <vector>
#include <assert.h>

#include "ZkClient.h"
#include "ZkClientManager.h"

#define ZOOKEEPER_SERVER_CONN_STRING \
  "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"

using namespace std;
using namespace zkclient;

uint32_t ZkClientHandle;

void getHandle_test(uint32_t original_handle, ZkClientPtr cli) {
  assert(original_handle == cli->getHandle());

  //从一个不存在的handle 获取zkclient，应返回nullptr
  uint32_t uninit_handle = 0xFFFFFFF0;
  assert(ZkClientManager::instance().getZkClient(uninit_handle).get() == nullptr);
  std::cout << "getHandle_test succeed!" << std::endl;
}

int main() {
  if (ZkClientManager::setLogConf(true, "./zk_log") == false) {
    std::cout << "setLogConf failed!" << std::endl;
  }

  uint32_t handle = ZkClientManager::instance().createZkClient(
      ZOOKEEPER_SERVER_CONN_STRING, 30000, nullptr, nullptr, nullptr);
  if (handle == 0) {
    std::cout << "create session failed! connStr: "
              << ZOOKEEPER_SERVER_CONN_STRING << std::endl;
    return 0;
  }

  ZkClientHandle = handle;

  //通过session handle，获取ZkClient
  ZkClientPtr cli = ZkClientManager::instance().getZkClient(ZkClientHandle);

  // Test
  getHandle_test(ZkClientHandle, cli);
}
