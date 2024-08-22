file(GLOB LOCK_SRC ${CMAKE_CURRENT_SOURCE_DIR}/zk/*.cc)

# add_library(lock STATIC ${LOCK_SRC})
add_library(zk_object OBJECT ${LOCK_SRC})
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