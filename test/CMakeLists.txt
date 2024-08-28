cmake_minimum_required(VERSION 3.14)

link_directories(${CMAKE_ARCHIVE_OUTPUT_DIRECTORY})

file(GLOB ZK_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*.cc")

# "make all-tests"
add_custom_target(show-tests COMMAND ${CMAKE_CTEST_COMMAND} --show-only)
add_custom_target(all-tests COMMAND ${CMAKE_CTEST_COMMAND} --verbose)

# "make XXX_test"
foreach(zk_test_source ${ZK_TEST_SOURCES})
    get_filename_component(zk_test_filename ${zk_test_source} NAME)
    string(REPLACE ".cc" "" zk_test_name ${zk_test_filename})

    add_executable(${zk_test_name} EXCLUDE_FROM_ALL ${zk_test_source})
    add_dependencies(show-tests ${zk_test_name})
    add_dependencies(all-tests ${zk_test_name})


    target_link_libraries(${zk_test_name} zkclient pthread)

    # Set test target properties
    set_target_properties(${zk_test_name}
            PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
            COMMAND ${zk_test_name}
            )

endforeach()