cmake_minimum_required(VERSION 3.28)

# Create executable for the write_and_read_streams example
add_executable(write_and_read_streams write_and_read_streams.cpp)

# Link against sparrow-ipc and its dependencies
target_link_libraries(write_and_read_streams
    PRIVATE
        sparrow-ipc
        sparrow::sparrow
        arrow-testing-data 
)

# Set C++ standard to match the main project
set_target_properties(write_and_read_streams
    PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
)

# Include directories for headers
target_include_directories(write_and_read_streams
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_BINARY_DIR}/generated
)

# Ensure generated flatbuffer headers are available
add_dependencies(write_and_read_streams generate_flatbuffers_headers)

# Optional: Copy to build directory for easy execution
if(WIN32)
    # On Windows, copy required DLLs
    add_custom_command(
        TARGET write_and_read_streams POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "$<TARGET_FILE:sparrow::sparrow>"
            "$<TARGET_FILE_DIR:write_and_read_streams>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "$<TARGET_FILE:sparrow-ipc>"
            "$<TARGET_FILE_DIR:write_and_read_streams>"
        COMMENT "Copying sparrow and sparrow-ipc DLLs to example executable directory"
    )
endif()

# Create a custom target to easily run the example
add_custom_target(run_example
    COMMAND write_and_read_streams
    DEPENDS write_and_read_streams
    COMMENT "Running write_and_read_streams example"
    USES_TERMINAL
)

set_target_properties(run_example PROPERTIES FOLDER "Examples")
