#include <assert.h>
#include <unistd.h>
#include <iostream>

#include <assert.h>
#include <algorithm>
#include <string>
#include <vector>

#include "ZkClient.h"
#include "ZkClientManager.h"
#include "ZkUtil.h"

using namespace std;
using namespace zkclient;

#define ZOOKEEPER_SERVER_CONN_STRING \
  "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"

uint32_t ZkClientHandle;

void printChild(const std::string& path,
                const std::vector<std::string>& childnodes) {
  printf(
      "\n\n-----------[printChild] ParentPath: %s, child size:%zu-------------- "
      "\n",
      path.c_str(), childnodes.size());
  std::vector<std::string>::const_iterator iter = childnodes.begin();
  for (; iter != childnodes.end(); iter++) {
    printf("child name:%s\n", (*iter).c_str());
  }
  printf(
      "-------------------------------------------------------------------- "
      "\n");
}

bool isEqualChildren(const std::vector<std::string>& left,
                     const std::vector<std::string>& right) {
  if (left.size() != right.size()) {
    return false;
  }

  std::vector<std::string>::const_iterator iter = left.begin();
  for (; iter != left.end(); iter++) {
    if (find(right.begin(), right.end(), (*iter)) == right.end()) {
      printf("[isEqualChildren] left vec elem: %s don't exist in right vec.\n",
             (*iter).c_str());
      return false;
    }
  }

  return true;
}

void printClientId(const SessionClientId& cliId) {
  printf("\ncliendId:\n");
  printf("cliId.id:%ld\n", cliId.client_id);
  printf("cliId.passwd:%s\n", cliId.passwd);
}

void getHandle_test(uint32_t original_handle, ZkClientPtr cli) {
  assert(original_handle == cli->getHandle());

  //从一个不存在的handle 获取zkclient，应返回nullptr
  uint32_t uninit_handle = 0xFFFFFFF0;
  assert(ZkClientManager::instance().getZkClient(uninit_handle).get() ==
         nullptr);
  std::cout << "getHandle_test succeed!" << std::endl;
}

void getClientId_test() {
  //创建一个session
  uint handle = ZkClientManager::instance().createZkClient(
      ZOOKEEPER_SERVER_CONN_STRING, 30000, NULL, NULL, NULL);
  if (handle == 0) {
    printf("[getClientId_test] create session failed! connStr:%s\n",
           ZOOKEEPER_SERVER_CONN_STRING);
    return;
  }

  //获取当前session的clientId
  ZkClientPtr cli = ZkClientManager::instance().getZkClient(handle);
  SessionClientId cliId;
  if (cli->getClientId(cliId) == true) {
    printClientId(cliId);
  }

  //利用之前session的clientId 来 创建一个session
  uint32_t handle_new = ZkClientManager::instance().createZkClient(
      ZOOKEEPER_SERVER_CONN_STRING, 90000, &cliId, nullptr, nullptr);
  if (handle_new == 0) {
    printf("[getClientId_test]create session failed! connStr:%s\n",
           ZOOKEEPER_SERVER_CONN_STRING);
    return;
  }
  ZkClientPtr cli_new = ZkClientManager::instance().getZkClient(handle_new);
  SessionClientId cliId_new;
  if (cli_new->getClientId(cliId_new) == true) {
    printClientId(cliId_new);
    assert(cliId.client_id == cliId_new.client_id);
    assert(strncmp(cliId.passwd, cliId_new.passwd, sizeof(cliId.passwd)) == 0);
  }
  printf("getClientId_test succeed!\n");
}

void sync_getNode_test() {
#define SYNC_GETNODE_PATH_NAME "/sync_getNode_test"
#define SYNC_GETNODE_PATH_VALUE "sync_getNode_test_value"

  ZkClientPtr cli = ZkClientManager::instance().getZkClient(ZkClientHandle);
  std::string retPath = "";
  zkutil::ZkErrorCode ec = cli->create(
      SYNC_GETNODE_PATH_NAME, SYNC_GETNODE_PATH_VALUE, false, false, retPath);
  if (ec == zkutil::kZKSucceed || ec == zkutil::kZKExisted) {
    std::string value = "";
    int32_t version;
    if (cli->getNode(SYNC_GETNODE_PATH_NAME, value, version) ==
        zkutil::kZKSucceed) {
      printf("\n[sync_getNode_test] path:%s, value:%s, version:%d\n",
             SYNC_GETNODE_PATH_NAME, value.c_str(), version);
      assert(value == SYNC_GETNODE_PATH_VALUE);
    } else {
      printf(
          "\n[sync_getNode_test] getNode:%s failed! sync_getNode_test "
          "failed!\n",
          SYNC_GETNODE_PATH_NAME);
      return;
    }
  } else {
    printf(
        "\n[sync_getNode_test] create path:%s failed! sync_getNode_test "
        "failed!\n",
        SYNC_GETNODE_PATH_NAME);
    return;
  }
  printf("sync_getNode_test succeed!\n");
}

void sync_getChildren_test() {
#define SYNC_GETCHILDREN_BASE_NAME "/sync_getChildren_test_base"
#define SYNC_GETCHILDREN_CHILD_NAME "sync_getChildren_test_child"
#define SYNC_GETCHILDREN_CHILD_NUM 10

  std::vector<std::string> orignalChildList;
  std::vector<std::string> childNodes;

  ZkClientPtr cli = ZkClientManager::instance().getZkClient(ZkClientHandle);
  std::string retPath = "";
  zkutil::ZkErrorCode ec =
      cli->create(SYNC_GETCHILDREN_BASE_NAME, "", false, false, retPath);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
    printf("\n[sync_getChildren_test] create path:%s failed! \n",
           SYNC_GETCHILDREN_BASE_NAME);
    goto TAG_SYNC_GETCHILDREN_TEST_FAILED;
  }

  for (int i = 0; i < SYNC_GETCHILDREN_CHILD_NUM; i++) {
    char path[100] = {0};
    char childName[100] = {0};
    snprintf(childName, sizeof(childName), "%s_%d", SYNC_GETCHILDREN_CHILD_NAME,
             i);
    snprintf(path, sizeof(path), "%s/%s_%d", SYNC_GETCHILDREN_BASE_NAME,
             SYNC_GETCHILDREN_CHILD_NAME, i);
    orignalChildList.push_back(childName);

    zkutil::ZkErrorCode ec_chd = cli->create(path, "", false, false, retPath);
    if (ec_chd != zkutil::kZKSucceed && ec_chd != zkutil::kZKExisted) {
      printf("\n[sync_getChildren_test] create path:%s failed! \n", path);
      goto TAG_SYNC_GETCHILDREN_TEST_FAILED;
    }
  }

  if (cli->getChildren(SYNC_GETCHILDREN_BASE_NAME, childNodes) !=
      zkutil::kZKSucceed) {
    printf("\n[sync_getChildren_test] getChildren failed! path:%s \n",
           SYNC_GETCHILDREN_BASE_NAME);
    goto TAG_SYNC_GETCHILDREN_TEST_FAILED;
  }

  printChild(SYNC_GETCHILDREN_BASE_NAME, childNodes);
  assert(isEqualChildren(orignalChildList, childNodes) == true);

  printf("sync_getChildren_test succeed!\n");
  return;

TAG_SYNC_GETCHILDREN_TEST_FAILED:
  printf("\n[sync_getChildren_test] failed! \n");
  return;
}

void sync_isExist_test() {
#define SYNC_ISEXIST_PATH_NAME "/sync_isExist_test_base"

  ZkClientPtr cli = ZkClientManager::instance().getZkClient(ZkClientHandle);
  std::string retPath = "";
  zkutil::ZkErrorCode ec_exist;

  zkutil::ZkErrorCode ec =
      cli->create(SYNC_ISEXIST_PATH_NAME, "", false, false, retPath);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
    printf("\n[sync_isExist_test] create path:%s failed! \n",
           SYNC_ISEXIST_PATH_NAME);
    goto TAG_SYNC_ISEXIST_TEST_FAILED;
  }

  ec_exist = cli->isExist(SYNC_ISEXIST_PATH_NAME);
  assert(ec_exist == zkutil::kZKSucceed);

  printf("sync_isExist_test succeed!\n");
  return;

TAG_SYNC_ISEXIST_TEST_FAILED:
  printf("\n[sync_isExist_test] failed! \n");
  return;
}

void sync_create_test_1(std::string test_path) {
  ZkClientPtr cli = ZkClientManager::instance().getZkClient(ZkClientHandle);
  std::string retPath = "";
  zkutil::ZkErrorCode ec;
  zkutil::ZkErrorCode ec_other;
  bool isTemp = false;
  bool isSeq = false;

  ec = cli->create(test_path, "", isTemp, isSeq, retPath);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
    printf("\n[sync_create_test] create path:%s failed! \n", test_path.c_str());
    goto TAG_SYNC_CREATE_TEST_FAILED_1;
  }

  //测试 返回的路径 正确
  printf("[not temp, not seq ] retPath:%s, original path:%s\n", retPath.c_str(),
         test_path.c_str());
  assert(retPath == test_path);

  //测试 创建成功了
  ec_other = cli->isExist(test_path);
  assert(ec_other == zkutil::kZKSucceed);

  printf("sync_create_test_1 succeed!\n");
  return;

TAG_SYNC_CREATE_TEST_FAILED_1:
  printf("\n[sync_create_test_1] failed! \n");
  return;
}

void sync_create_test_2(std::string test_path) {
  ZkClientPtr cli = ZkClientManager::instance().getZkClient(ZkClientHandle);
  std::string retPath = "";
  zkutil::ZkErrorCode ec;
  zkutil::ZkErrorCode ec_other;
  uint handle_temp;
  ZkClientPtr cli_temp;
  bool isTemp = true;
  bool isSeq = false;

  //创建一个session
  handle_temp = ZkClientManager::instance().createZkClient(
      ZOOKEEPER_SERVER_CONN_STRING, 30000, NULL, NULL, NULL);
  if (handle_temp == 0) {
    printf("[sync_create_test] create session failed! connStr:%s\n",
           ZOOKEEPER_SERVER_CONN_STRING);
    goto TAG_SYNC_CREATE_TEST_FAILED_2;
  }

  cli_temp = ZkClientManager::instance().getZkClient(handle_temp);
  ec = cli_temp->create(test_path, "", isTemp, isSeq, retPath);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
    printf("\n[sync_create_test] create path:%s failed! \n", test_path.c_str());
    goto TAG_SYNC_CREATE_TEST_FAILED_2;
  }

  //测试 返回的路径 正确
  assert(retPath == test_path);

  //测试 创建成功了
  ec_other = cli_temp->isExist(test_path);
  assert(ec_other == zkutil::kZKSucceed);

  //销毁当前session
  ZkClientManager::instance().destroyClient(handle_temp);
  cli_temp.reset();

  printf("distroy this zkclient, session Handle:%d\n", handle_temp);
  sleep(5);  //等待5秒，temp session 销毁

  ec_other = cli->isExist(test_path);
  assert(ec_other ==
         zkutil::kZKNotExist);  //测试 session销毁后，临时的node 应自动删除

  printf("sync_create_test_2 succeed!\n");
  return;

TAG_SYNC_CREATE_TEST_FAILED_2:
  printf("\n[sync_create_test_2] failed! \n");
  return;
}

void sync_create_test_3(std::string test_path) {
  ZkClientPtr cli = ZkClientManager::instance().getZkClient(ZkClientHandle);
  std::string retPath = "";
  zkutil::ZkErrorCode ec;
  zkutil::ZkErrorCode ec_other;
  bool isTemp = false;
  bool isSeq = true;

  ec = cli->create(test_path, "", isTemp, isSeq, retPath);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
    printf("\n[sync_create_test] create path:%s failed! \n", test_path.c_str());
    goto TAG_SYNC_CREATE_TEST_FAILED_3;
  }

  //测试 返回的路径 正确
  printf("[not temp, seq] retPath:%s, original path:%s\n", retPath.c_str(),
         test_path.c_str());
  assert(retPath != test_path);

  //测试 创建成功了
  ec_other = cli->isExist(retPath);
  assert(ec_other == zkutil::kZKSucceed);

  printf("sync_create_test_3 succeed!\n");
  return;

TAG_SYNC_CREATE_TEST_FAILED_3:
  printf("\n[sync_create_test_3] failed! \n");
  return;
}

void sync_create_test_4(std::string test_path) {
  ZkClientPtr cli = ZkClientManager::instance().getZkClient(ZkClientHandle);
  std::string retPath = "";
  zkutil::ZkErrorCode ec;
  zkutil::ZkErrorCode ec_other;
  uint handle_temp;
  ZkClientPtr cli_temp;
  bool isTemp = true;
  bool isSeq = true;

  //创建一个session
  handle_temp = ZkClientManager::instance().createZkClient(
      ZOOKEEPER_SERVER_CONN_STRING, 30000, NULL, NULL, NULL);
  if (handle_temp == 0) {
    printf("[sync_create_test] create session failed! connStr:%s\n",
           ZOOKEEPER_SERVER_CONN_STRING);
    goto TAG_SYNC_CREATE_TEST_FAILED_4;
  }

  cli_temp = ZkClientManager::instance().getZkClient(handle_temp);
  ec = cli_temp->create(test_path, "", isTemp, isSeq, retPath);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
    printf("\n[sync_create_test] create path:%s failed! \n", test_path.c_str());
    goto TAG_SYNC_CREATE_TEST_FAILED_4;
  }

  //测试 返回的路径 正确
  printf("[temp, seq] retPath:%s, original path:%s\n", retPath.c_str(),
         test_path.c_str());
  assert(retPath != test_path);

  //测试 创建成功了
  ec_other = cli_temp->isExist(retPath);
  assert(ec_other == zkutil::kZKSucceed);

  //销毁当前session
  ZkClientManager::instance().destroyClient(handle_temp);
  cli_temp.reset();

  printf("distroy this zkclient, session Handle:%d\n", handle_temp);
  sleep(5);  //等待5秒，temp session 销毁

  ec_other = cli->isExist(retPath);
  assert(ec_other ==
         zkutil::kZKNotExist);  //测试 session销毁后，临时的node 应自动删除

  printf("sync_create_test_4 succeed!\n");
  return;

TAG_SYNC_CREATE_TEST_FAILED_4:
  printf("\n[sync_create_test_4] failed! \n");
  return;
}

void sync_create_test_5(std::string parentPath, std::string childPath) {
  ZkClientPtr cli = ZkClientManager::instance().getZkClient(ZkClientHandle);
  std::string retPath = "";
  zkutil::ZkErrorCode ec;
  zkutil::ZkErrorCode ec_other;
  bool isTemp = false;
  bool isSeq = false;

  //测试 如果创建的结点中，父结点还未创建，应返回失败
  ec = cli->create(childPath, "", isTemp, isSeq, retPath);
  assert(ec == zkutil::kZKNotExist);

  //先创建父结点
  ec = cli->create(parentPath, "", isTemp, isSeq, retPath);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
    printf("\n[sync_create_test] create path:%s failed! \n",
           parentPath.c_str());
    goto TAG_SYNC_CREATE_TEST_FAILED_5;
  }

  //测试 返回的路径 正确
  printf("[not temp, not seq ] retPath:%s, original path:%s\n", retPath.c_str(),
         parentPath.c_str());
  assert(retPath == parentPath);

  //测试 创建成功了
  ec_other = cli->isExist(parentPath);
  assert(ec_other == zkutil::kZKSucceed);

  //再创建子结点
  ec = cli->create(childPath, "", false, false, retPath);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
    printf("\n[sync_create_test] create path:%s failed! \n", childPath.c_str());
    goto TAG_SYNC_CREATE_TEST_FAILED_5;
  }

  //测试 返回的路径 正确
  printf("[not temp, not seq ] retPath:%s, original path:%s\n", retPath.c_str(),
         childPath.c_str());
  assert(retPath == childPath);

  //测试 创建成功了
  ec_other = cli->isExist(childPath);
  assert(ec_other == zkutil::kZKSucceed);

  printf("sync_create_test_5 succeed!\n");
  return;

TAG_SYNC_CREATE_TEST_FAILED_5:
  printf("\n[sync_create_test_5] failed! \n");
  return;
}

void sync_create_test() {
#define SYNC_CREATE_PATH_NAME_1 "/sync_create_test_no_temp_no_seq"
#define SYNC_CREATE_PATH_NAME_2 "/sync_create_test_temp_no_seq"
#define SYNC_CREATE_PATH_NAME_3 "/sync_create_test_no_temp_seq"
#define SYNC_CREATE_PATH_NAME_4 "/sync_create_test_temp_seq"
#define SYNC_CREATE_PATH_NAME_5 "/sync_create_test_root"
#define SYNC_CREATE_PATH_NAME_6 "/sync_create_test_root/sync_create_child"

  ZkClientPtr cli = ZkClientManager::instance().getZkClient(ZkClientHandle);
  std::string retPath = "";
  zkutil::ZkErrorCode ec;

  //先删除 5 个结点
  ec = cli->deleteNode(SYNC_CREATE_PATH_NAME_1);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKNotExist) {
    printf("\n[sync_create_test] delete path:%s failed! \n",
           SYNC_CREATE_PATH_NAME_1);
    goto TAG_SYNC_CREATE_TEST_FAILED;
  }

  ec = cli->deleteNode(SYNC_CREATE_PATH_NAME_2);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKNotExist) {
    printf("\n[sync_create_test] delete path:%s failed! \n",
           SYNC_CREATE_PATH_NAME_2);
    goto TAG_SYNC_CREATE_TEST_FAILED;
  }

  ec = cli->deleteNode(SYNC_CREATE_PATH_NAME_3);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKNotExist) {
    printf("\n[sync_create_test] delete path:%s failed! \n",
           SYNC_CREATE_PATH_NAME_3);
    goto TAG_SYNC_CREATE_TEST_FAILED;
  }

  ec = cli->deleteNode(SYNC_CREATE_PATH_NAME_4);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKNotExist) {
    printf("\n[sync_create_test] delete path:%s failed! \n",
           SYNC_CREATE_PATH_NAME_4);
    goto TAG_SYNC_CREATE_TEST_FAILED;
  }

  //应先 删除 叶子结点，再删除 分支结点
  ec = cli->deleteNode(SYNC_CREATE_PATH_NAME_6);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKNotExist) {
    printf("\n[sync_create_test] delete path:%s failed! \n",
           SYNC_CREATE_PATH_NAME_6);
    goto TAG_SYNC_CREATE_TEST_FAILED;
  }

  ec = cli->deleteNode(SYNC_CREATE_PATH_NAME_5);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKNotExist) {
    printf("\n[sync_create_test] delete path:%s failed! \n",
           SYNC_CREATE_PATH_NAME_5);
    goto TAG_SYNC_CREATE_TEST_FAILED;
  }

  //测试not temp, not seq 的情况//////////////////////////////
  sync_create_test_1(SYNC_CREATE_PATH_NAME_1);

  //测试temp, not seq 的情况////////////////////////////////
  sync_create_test_2(SYNC_CREATE_PATH_NAME_2);

  //测试not temp, seq 的情况////////////////////////////////
  sync_create_test_3(SYNC_CREATE_PATH_NAME_3);

  //测试temp, seq 的情况////////////////////////////////
  sync_create_test_4(SYNC_CREATE_PATH_NAME_4);

  //测试 如果创建的结点中，父结点还未创建，应返回失败
  sync_create_test_5(SYNC_CREATE_PATH_NAME_5, SYNC_CREATE_PATH_NAME_6);

  printf("sync_create_test succeed!\n");
  return;

TAG_SYNC_CREATE_TEST_FAILED:
  printf("\n[sync_create_test] failed! \n");
  return;
}

void sync_createIfNeedCreateParents_test() {
#define SYNC_CREATEIFNEEDCREATEPARENT_PATH_NAME_1 \
  "/sync_createIfNeedCreateParents_test_parent"
#define SYNC_CREATEIFNEEDCREATEPARENT_PATH_NAME_2 \
  "/sync_createIfNeedCreateParents_test_parent/"  \
  "sync_createIfNeedCreateParents_test_child"

  ZkClientPtr cli = ZkClientManager::instance().getZkClient(ZkClientHandle);
  std::string retPath = "";
  zkutil::ZkErrorCode ec;
  zkutil::ZkErrorCode ec_other;
  bool isTemp = false;
  bool isSeq = false;

  //应先 删除 叶子结点，再删除 分支结点
  ec = cli->deleteNode(SYNC_CREATEIFNEEDCREATEPARENT_PATH_NAME_2);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKNotExist) {
    printf("\n[sync_createIfNeedCreateParents_test] delete path:%s failed! \n",
           SYNC_CREATEIFNEEDCREATEPARENT_PATH_NAME_2);
    goto TAG_SYNC_CREATEIFNEEDCREATEPARENT_TEST_FAILED;
  }

  ec = cli->deleteNode(SYNC_CREATEIFNEEDCREATEPARENT_PATH_NAME_1);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKNotExist) {
    printf("\n[sync_createIfNeedCreateParents_test] delete path:%s failed! \n",
           SYNC_CREATEIFNEEDCREATEPARENT_PATH_NAME_1);
    goto TAG_SYNC_CREATEIFNEEDCREATEPARENT_TEST_FAILED;
  }

  //直接创建叶子结点
  ec = cli->createIfNeedCreateParents(SYNC_CREATEIFNEEDCREATEPARENT_PATH_NAME_2,
                                      "", isTemp, isSeq, retPath);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
    printf("\n[sync_createIfNeedCreateParents_test] create path:%s failed! \n",
           SYNC_CREATEIFNEEDCREATEPARENT_PATH_NAME_2);
    goto TAG_SYNC_CREATEIFNEEDCREATEPARENT_TEST_FAILED;
  }

  //测试 返回的路径 正确
  printf("retPath:%s, original path:%s\n", retPath.c_str(),
         SYNC_CREATEIFNEEDCREATEPARENT_PATH_NAME_2);
  assert(retPath == SYNC_CREATEIFNEEDCREATEPARENT_PATH_NAME_2);

  //测试 创建成功了
  ec_other = cli->isExist(SYNC_CREATEIFNEEDCREATEPARENT_PATH_NAME_2);
  assert(ec_other == zkutil::kZKSucceed);

  printf("sync_createIfNeedCreateParents_test succeed!\n");
  return;

TAG_SYNC_CREATEIFNEEDCREATEPARENT_TEST_FAILED:
  printf("\n[sync_createIfNeedCreateParents_test] failed! \n");
  return;
}

void sync_set_test() {
#define SYNC_SET_PATH_NAME "/sync_set_test"

  ZkClientPtr cli = ZkClientManager::instance().getZkClient(ZkClientHandle);
  std::string retPath = "";
  zkutil::ZkErrorCode ec;
  zkutil::ZkErrorCode ec_other;
  bool isTemp = false;
  bool isSeq = false;
  std::string value1 = "";
  int32_t version1;
  std::string value2 = "";
  int32_t version2;
  std::string value3 = "";
  int32_t version3;

  //创建测试结点
  ec = cli->create(SYNC_SET_PATH_NAME, "", isTemp, isSeq, retPath);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
    printf("\n[sync_set_test] create path:%s failed! \n", SYNC_SET_PATH_NAME);
    goto TAG_SYNC_SET_TEST_FAILED;
  }

  //获取测试结点的值，版本号
  if (cli->getNode(SYNC_SET_PATH_NAME, value1, version1) !=
      zkutil::kZKSucceed) {
    printf("\n[sync_set_test] getNode:%s failed! sync_getNode_test failed!\n",
           SYNC_GETNODE_PATH_NAME);
    goto TAG_SYNC_SET_TEST_FAILED;
  }

  //设置结点的 版本号不对，应返回失败
  ec_other = cli->set(SYNC_SET_PATH_NAME, "test_value_1", version1 + 1);
  assert(ec_other == zkutil::kZKError);

  //设置结点的 版本号正确，应返回成功
  ec_other = cli->set(SYNC_SET_PATH_NAME, "test_value_2", version1);
  assert(ec_other == zkutil::kZKSucceed);

  //获取测试结点的值，版本号
  if (cli->getNode(SYNC_SET_PATH_NAME, value2, version2) !=
      zkutil::kZKSucceed) {
    printf("\n[sync_set_test] getNode:%s failed! sync_getNode_test failed!\n",
           SYNC_SET_PATH_NAME);
    goto TAG_SYNC_SET_TEST_FAILED;
  }
  assert(value2 == "test_value_2");

  //不输入版本号，则默认 设置最近的版本号
  ec_other = cli->set(SYNC_SET_PATH_NAME, "test_value_3");
  assert(ec_other == zkutil::kZKSucceed);

  //获取测试结点的值，版本号
  if (cli->getNode(SYNC_SET_PATH_NAME, value3, version3) !=
      zkutil::kZKSucceed) {
    printf("\n[sync_set_test] getNode:%s failed! sync_getNode_test failed!\n",
           SYNC_GETNODE_PATH_NAME);
    goto TAG_SYNC_SET_TEST_FAILED;
  }
  assert(value3 == "test_value_3");

  printf("sync_set_test succeed!\n");
  return;
TAG_SYNC_SET_TEST_FAILED:
  printf("\n[sync_set_test] failed! \n");
  return;
}

void sync_deleteNode_test() {
#define SYNC_DELETENODE_PATH_NAME_1 "/sync_deleteNode_test_parent"
#define SYNC_DELETENODE_PATH_NAME_2 \
  "/sync_deleteNode_test_parent/sync_deleteNode_test_child"
#define SYNC_DELETENODE_PATH_NAME_3 "/sync_deleteNode_test_node"

  ZkClientPtr cli = ZkClientManager::instance().getZkClient(ZkClientHandle);
  std::string retPath = "";
  zkutil::ZkErrorCode ec;
  zkutil::ZkErrorCode ec_other;
  bool isTemp = false;
  bool isSeq = false;
  std::string value = "";
  int32_t version = 0;

  //直接创建叶子结点
  ec = cli->createIfNeedCreateParents(SYNC_DELETENODE_PATH_NAME_2, "", isTemp,
                                      isSeq, retPath);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
    printf("\n[sync_deleteNode_test] create path:%s failed! \n",
           SYNC_DELETENODE_PATH_NAME_2);
    goto TAG_SYNC_DELETENODE_TEST_FAILED;
  }

  ec = cli->createIfNeedCreateParents(SYNC_DELETENODE_PATH_NAME_3, "", isTemp,
                                      isSeq, retPath);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
    printf("\n[sync_deleteNode_test] create path:%s failed! \n",
           SYNC_DELETENODE_PATH_NAME_3);
    goto TAG_SYNC_DELETENODE_TEST_FAILED;
  }

  //若删除的结点中 含有子结点，则删除失败
  ec_other = cli->deleteNode(SYNC_DELETENODE_PATH_NAME_1);
  assert(ec_other == zkutil::kZKNotEmpty);

  //先删除子结点，再删除分支结点
  ec_other = cli->deleteNode(SYNC_DELETENODE_PATH_NAME_2);
  assert(ec_other == zkutil::kZKSucceed);

  ec_other = cli->deleteNode(SYNC_DELETENODE_PATH_NAME_1);
  assert(ec_other == zkutil::kZKSucceed);

  //测试 删除成功了
  ec_other = cli->isExist(SYNC_DELETENODE_PATH_NAME_1);
  assert(ec_other == zkutil::kZKNotExist);

  ec_other = cli->isExist(SYNC_DELETENODE_PATH_NAME_2);
  assert(ec_other == zkutil::kZKNotExist);

  //获取测试结点的值，版本号
  if (cli->getNode(SYNC_DELETENODE_PATH_NAME_3, value, version) !=
      zkutil::kZKSucceed) {
    printf(
        "\n[sync_deleteNode_test] getNode:%s failed! sync_getNode_test "
        "failed!\n",
        SYNC_DELETENODE_PATH_NAME_3);
    goto TAG_SYNC_DELETENODE_TEST_FAILED;
  }

  //若删除的结点 的版本号不对，则返回失败
  ec_other = cli->deleteNode(SYNC_DELETENODE_PATH_NAME_3, version + 1);
  assert(ec_other == zkutil::kZKError);

  //若删除的结点 的版本号正确，则删除成功
  ec_other = cli->deleteNode(SYNC_DELETENODE_PATH_NAME_3, version);
  assert(ec_other == zkutil::kZKSucceed);

  //测试 删除成功了
  ec_other = cli->isExist(SYNC_DELETENODE_PATH_NAME_3);
  assert(ec_other == zkutil::kZKNotExist);

  printf("sync_deleteNode_test succeed!\n");
  return;

TAG_SYNC_DELETENODE_TEST_FAILED:
  printf("\n[sync_deleteNode_test] failed! \n");
  return;
}

void sync_deleteRecursive_test() {
#define SYNC_DELETERECURISIVE_PATH_NAME_1 "/sync_deleteRecursive_test_parent"
#define SYNC_DELETERECURISIVE_PATH_NAME_2 \
  "/sync_deleteRecursive_test_parent/sync_deleteRecursive_test_child"

  ZkClientPtr cli = ZkClientManager::instance().getZkClient(ZkClientHandle);
  std::string retPath = "";
  zkutil::ZkErrorCode ec;
  zkutil::ZkErrorCode ec_other;
  bool isTemp = false;
  bool isSeq = false;
  std::string value = "";
  int32_t version = 0;

  //直接创建叶子结点
  ec = cli->createIfNeedCreateParents(SYNC_DELETERECURISIVE_PATH_NAME_2, "",
                                      isTemp, isSeq, retPath);
  if (ec != zkutil::kZKSucceed && ec != zkutil::kZKExisted) {
    printf("\n[sync_deleteNode_test] create path:%s failed! \n",
           SYNC_DELETERECURISIVE_PATH_NAME_2);
    goto TAG_SYNC_DELETERECRUSIVE_TEST_FAILED;
  }

  //若删除的结点 的版本号正确，则删除成功
  ec_other = cli->deleteRecursive(SYNC_DELETERECURISIVE_PATH_NAME_1);
  assert(ec_other == zkutil::kZKSucceed);

  //测试 删除成功了
  ec_other = cli->isExist(SYNC_DELETERECURISIVE_PATH_NAME_1);
  assert(ec_other == zkutil::kZKNotExist);

  ec_other = cli->isExist(SYNC_DELETERECURISIVE_PATH_NAME_2);
  assert(ec_other == zkutil::kZKNotExist);

  printf("sync_deleteRecursive_test succeed!\n");
  return;

TAG_SYNC_DELETERECRUSIVE_TEST_FAILED:
  printf("\n[sync_deleteRecursive_test] failed! \n");
  return;
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

  getClientId_test();

  sync_getNode_test();

  sync_getChildren_test();

  sync_isExist_test();

  sync_create_test();

  sync_createIfNeedCreateParents_test();

  sync_set_test();

  sync_deleteNode_test();

  sync_deleteRecursive_test();
}
