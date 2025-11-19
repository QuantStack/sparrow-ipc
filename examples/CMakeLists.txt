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
    set(ZSTD_DLL_TARGET "")
    if(TARGET libzstd_shared AND SPARROW_IPC_BUILD_SHARED) # Building deps from src case: shared target without namespace
        set(ZSTD_DLL_TARGET libzstd_shared)
    elseif(TARGET libzstd_static AND NOT SPARROW_IPC_BUILD_SHARED) # Building deps from src case: static target without namespace
        set(ZSTD_DLL_TARGET libzstd_static)
    endif()

    # On Windows, copy required DLLs
    set(DLL_COPY_COMMANDS "") # Initialize a list to hold all copy commands
    # Add unconditional copy commands
    list(APPEND DLL_COPY_COMMANDS
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "$<TARGET_FILE:sparrow::sparrow>"
            "$<TARGET_FILE_DIR:write_and_read_streams>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "$<TARGET_FILE:sparrow-ipc>"
            "$<TARGET_FILE_DIR:write_and_read_streams>"
    )

    # Conditionally add ZSTD copy command
    if(ZSTD_DLL_TARGET)
        list(APPEND DLL_COPY_COMMANDS
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
                "$<TARGET_FILE:${ZSTD_DLL_TARGET}>"
                "$<TARGET_FILE_DIR:write_and_read_streams>"
        )
    else()
        message(WARNING "ZSTD DLL will not be copied for examples.")
    endif()

    add_custom_command(
        TARGET write_and_read_streams POST_BUILD
        ${DLL_COPY_COMMANDS}
        COMMENT "Copying required DLLs to example executable directory"
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
