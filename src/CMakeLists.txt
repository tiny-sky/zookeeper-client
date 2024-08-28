file(GLOB ZK_SRC_ZK ${CMAKE_CURRENT_SOURCE_DIR}/zk/*.cc)
file(GLOB ZK_SRC_RPC ${CMAKE_CURRENT_SOURCE_DIR}/rpc/*.cc)

list(APPEND ZK_SRC ${ZK_SRC_ZK} ${ZK_SRC_RPC})

# add_library(lock STATIC ${ZK_SRC})
add_library(zk_object OBJECT ${ZK_SRC})
add_library(zkclient STATIC $<TARGET_OBJECTS:zk_object>)

message(STATUS "Static lib done")

find_package(Threads REQUIRED)

set(ZK_THIRDPARTY_LIBS
        Threads::Threads
        zookeeper_mt
        muduo_base
        muduo_net
        )

target_link_libraries(zkclient ${ZK_THIRDPARTY_LIBS})

target_include_directories(
    zkclient PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include)