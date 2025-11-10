cmake_minimum_required(VERSION 3.28)

# Create executable for file_to_stream integration test
add_executable(file_to_stream file_to_stream.cpp)

target_link_libraries(file_to_stream
    PRIVATE
        sparrow-ipc
        sparrow::sparrow
        sparrow::json_reader
)

set_target_properties(file_to_stream
    PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
)

target_include_directories(file_to_stream
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_BINARY_DIR}/generated
)

add_dependencies(file_to_stream generate_flatbuffers_headers)

# Create executable for stream_to_file integration test
add_executable(stream_to_file stream_to_file.cpp)

target_link_libraries(stream_to_file
    PRIVATE
        sparrow-ipc
        sparrow::sparrow
)

set_target_properties(stream_to_file
    PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
)

target_include_directories(stream_to_file
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_BINARY_DIR}/generated
)

add_dependencies(stream_to_file generate_flatbuffers_headers)

# Create test executable for integration tools
add_executable(test_integration_tools main.cpp test_integration_tools.cpp)

target_link_libraries(test_integration_tools
    PRIVATE
        sparrow-ipc
        sparrow::sparrow
        sparrow::json_reader
        doctest::doctest
        arrow-testing-data
)

target_compile_definitions(test_integration_tools
    PRIVATE
        INTEGRATION_TOOLS_DIR="${CMAKE_CURRENT_BINARY_DIR}"
)

set_target_properties(test_integration_tools
    PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
)

target_include_directories(test_integration_tools
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_BINARY_DIR}/generated
)

add_dependencies(test_integration_tools generate_flatbuffers_headers file_to_stream stream_to_file)

# Register with CTest
enable_testing()
add_test(NAME integration_tools_test COMMAND test_integration_tools)

# On Windows, copy required DLLs
if(WIN32)
    add_custom_command(
        TARGET file_to_stream POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "$<TARGET_FILE:sparrow::sparrow>"
            "$<TARGET_FILE_DIR:file_to_stream>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "$<TARGET_FILE:sparrow-ipc>"
            "$<TARGET_FILE_DIR:file_to_stream>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "$<TARGET_FILE:sparrow::json_reader>"
            "$<TARGET_FILE_DIR:file_to_stream>"
        COMMENT "Copying DLLs to file_to_stream executable directory"
    )

    add_custom_command(
        TARGET stream_to_file POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "$<TARGET_FILE:sparrow::sparrow>"
            "$<TARGET_FILE_DIR:stream_to_file>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "$<TARGET_FILE:sparrow-ipc>"
            "$<TARGET_FILE_DIR:stream_to_file>"
        COMMENT "Copying DLLs to stream_to_file executable directory"
    )

    add_custom_command(
        TARGET test_integration_tools POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "$<TARGET_FILE:sparrow::sparrow>"
            "$<TARGET_FILE_DIR:test_integration_tools>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "$<TARGET_FILE:sparrow-ipc>"
            "$<TARGET_FILE_DIR:test_integration_tools>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "$<TARGET_FILE:sparrow::json_reader>"
            "$<TARGET_FILE_DIR:test_integration_tools>"
        COMMENT "Copying DLLs to test_integration_tools executable directory"
    )
endif()

set_target_properties(file_to_stream stream_to_file test_integration_tools PROPERTIES FOLDER "Integration Tests")
