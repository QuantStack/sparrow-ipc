cmake_minimum_required(VERSION 3.28)

find_package(doctest CONFIG REQUIRED)

set(test_target "test_sparrow_ipc_lib")

add_executable(${test_target} main.cpp test.cpp test_utils.cpp)
target_link_libraries(${test_target}
    PRIVATE
        sparrow-ipc
        doctest::doctest
)
target_include_directories(${test_target}
    PRIVATE
        ${CMAKE_BINARY_DIR}/generated
        ${CMAKE_SOURCE_DIR}/include
)
add_dependencies(${test_target} generate_flatbuffers_headers)
add_test(NAME sparrow-ipc-tests COMMAND ${test_target})
