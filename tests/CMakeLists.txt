cmake_minimum_required(VERSION 3.28)

find_package(doctest CONFIG REQUIRED)

set(test_target "test_sparrow_ipc_lib")

add_executable(${test_target} test.cpp)
target_link_libraries(${test_target}
    PRIVATE
        sparrow-ipc
        doctest::doctest
)
target_include_directories(${test_target}
    PRIVATE
        ${CMAKE_BINARY_DIR}/generated
)
add_dependencies(${test_target} generate_flatbuffers_headers)
add_test(NAME sparrow-ipc-tests COMMAND ${test_target})
