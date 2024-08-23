#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "ZkUtil.h"

namespace zkclient {

class ZkClient;
using ZkClientPtr = std::shared_ptr<ZkClient>;

using SessionExpiredHandler =
    std::function<void(const ZkClientPtr& client, void* context)>;
using TimerCallback = std::function<void()>;

/* errcode 返回：
        kZKSucceed: 获取成功, value 结点的值，version 结点的版本号(之后可根据这个值，在delete,set时，进行CAS操作)
        kZKNotExist: 结点不存在, value 为空串，version 为kInvalidDataVersion
        kZKError: 其它错误, value 为空串，version 为kInvalidDataVersion
    */
using GetNodeHandler =
    std::function<void(zkutil::ZkErrorCode errcode, const ZkClientPtr& client,
                       const std::string& path, const std::string& value,
                       int32_t version, void* context)>;

/* errcode 返回：
        kZKSucceed: 获取成功, childNode返回 所有子结点的 结点名，path 返回 分支路径
        kZKNotExist: 结点不存在, childNode 为空
        kZKError: 其它错误, childNode 为空
    */
using GetChildrenHandler = std::function<void(
    zkutil::ZkErrorCode errcode, const ZkClientPtr& client,
    const std::string& path, const std::vector<std::string>& childNodes,
    void* context)>;

/* errcode 返回：
        kZKSucceed: 结点存在
        kZKNotExist: 结点不存在
        kZKError: 其它错误
    */
using ExistHandler =
    std::function<void(zkutil::ZkErrorCode errcode, const ZkClientPtr& client,
                       const std::string& path, void* context)>;

/* errcode 返回：
        kZKSucceed: 创建成功, value 结点的值
        kZKNotExist: 子路径不存在（创建失败 value 空串），需要先创建子路径，再创建结点
        kZKExisted: 结点已存在（创建失败 value 空串）
        kZKError: 其它错误（创建失败 value 空串）
		注：这里的 path 并不是 创建后的结点名，而是 指定创建的结点名。
			如果创建的是 顺序型 节点，则返回的路径 path 与 真实创建的结点名 会有不同(这时getChildren来获取它真实的结点名)，
			其它情况 返回的路径 path 与 真实创建的结点名 都相同.
    */
using CreateHandler = std::function<void(
    zkutil::ZkErrorCode errcode, const ZkClientPtr& client,
    const std::string& path, const std::string& value, void* context)>;

/* errcode 返回：
        kZKSucceed: set成功, version 所设置修改结点的版本号
        kZKNotExist: 结点不存在, version 为kInvalidDataVersion
        kZKError: 其它错误, version 为kInvalidDataVersion
    */
using SetHandler = std::function<void(
    zkutil::ZkErrorCode errcode, const ZkClientPtr& client,
    const std::string& path, int32_t version, void* context)>;

/* errcode 返回：
        kZKSucceed: 删除成功
        kZKNotExist: 要删除的节点 不存在
        kZKNotEmpty: 要删除的节点 含有 子节点，需要先删除子节点.
        kZKError: 其它错误
    */
using DeleteHandler =
    std::function<void(zkutil::ZkErrorCode errcode, const ZkClientPtr& client,
                       const std::string& path, void* context)>;

/* type 返回：
        kNodeDelete = 0,    // path: 注册监听的路径 value: 空串 version: kInvalidDataVersion
        kNodeCreate,    // path: 注册监听的路径 value: 结点最新的值 version: 结点最新的版本号
        kNodeChange,    // path: 注册监听的路径 value: 结点最新的值 version: 结点最新的版本号
        kGetNodeValue_NodeNotExist, //path: 注册监听的路径 value: 空串 version: kInvalidDataVersion
        kGetNodeValueFailed,  //path: 注册监听的路径 value: 空串 version: kInvalidDataVersion
        kTypeError, // path 注册监听的路径 value 空串 version kInvalidDataVersion
    */

using NodeChangeHandler =
    std::function<void(zkutil::ZkNotifyType type, const ZkClientPtr& client,
                       const std::string& path, const std::string& value,
                       int32_t version, void* context)>;
/* type 返回：
        kChildChange,    // path: 注册监听的路径  childNodes: 最新的子结点列表(注:不是完整的路径，而是子结点的结点名)
        kGetChildListFailed_ParentNotExist， // path: 注册监听的路径  childNodes: 空集合
        kGetChildListFailed,  //path: 注册监听的路径  childNodes: 空集合
        kTypeError, //path: 注册监听的路径  childNodes: 空集合
    */
using ChildChangeHandler = std::function<void(
    zkutil::ZkNotifyType type, const ZkClientPtr& client,
    const std::string& path, const std::vector<std::string>& childNodes,
    void* context)>;

}  // namespace zkclient
