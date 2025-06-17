cmake_minimum_required(VERSION 3.28)

project(sparrow-ipc CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
#set(CMAKE_CXX_SCAN_FOR_MODULES OFF) # We don't use modules?

set(SCHEMA_DIR ${CMAKE_SOURCE_DIR}/format)
set(FLATBUFFERS_GENERATED_DIR ${CMAKE_SOURCE_DIR}/src/generated)

find_program(FLATC_EXECUTABLE flatc)

if(NOT FLATC_EXECUTABLE)
    message(FATAL_ERROR "flatc not found. Please install Flatbuffers.")
endif()

# Fetch schemas from apache arrow
# TODO Automate this? or use our own?
set(SCHEMA_URLS
    "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/format/File.fbs"
    "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/format/Message.fbs"
    "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/format/Schema.fbs"
    "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/format/Tensor.fbs"
    "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/format/SparseTensor.fbs"
    # TODO what about feather.fbs?
)

file(MAKE_DIRECTORY ${SCHEMA_DIR})

# Download schemas
set(FLATBUFFERS_SCHEMAS "")
foreach(url IN LISTS SCHEMA_URLS)
    get_filename_component(filename ${url} NAME)
    message(STATUS "Downloading schema: ${url}")
    file(DOWNLOAD ${url} ${SCHEMA_DIR}/${filename}
        STATUS status
        SHOW_PROGRESS)
    list(APPEND FLATBUFFERS_SCHEMAS ${SCHEMA_DIR}/${filename})
endforeach()

# Generate Flatbuffers C++ headers from the schemas
file(MAKE_DIRECTORY ${FLATBUFFERS_GENERATED_DIR})

# Generate output files list
set(FLATBUFFERS_GENERATED_HEADERS "")
foreach(fbs_file IN LISTS FLATBUFFERS_SCHEMAS)
    # Generate the corresponding header file name
    get_filename_component(header_name ${fbs_file} NAME_WE)
    list(APPEND FLATBUFFERS_GENERATED_HEADERS "${FLATBUFFERS_GENERATED_DIR}/${header_name}_generated.h")
endforeach()

add_custom_command(
    OUTPUT ${FLATBUFFERS_GENERATED_HEADERS}
    COMMAND ${FLATC_EXECUTABLE} --cpp -o ${FLATBUFFERS_GENERATED_DIR} --cpp-std c++17 --scoped-enums ${FLATBUFFERS_SCHEMAS}
    DEPENDS ${FLATBUFFERS_SCHEMAS}
    COMMENT "Generating FlatBuffers C++ headers from schemas"
)

add_custom_target(generate_flatbuffers_headers
    DEPENDS ${FLATBUFFERS_GENERATED_HEADERS}
)

# Interface target for generated headers
add_library(flatbuffers_interface INTERFACE)
target_include_directories(flatbuffers_interface INTERFACE ${FLATBUFFERS_GENERATED_DIR})
add_dependencies(flatbuffers_interface generate_flatbuffers_headers)

# Main executable target
# TODO change to example later
add_executable(sparrow-ipc src/main.cpp)
target_link_libraries(sparrow-ipc PRIVATE flatbuffers_interface)

# Find and link FlatBuffers library
find_package(FlatBuffers REQUIRED)
target_link_libraries(sparrow-ipc PRIVATE flatbuffers::flatbuffers)

add_dependencies(sparrow-ipc generate_flatbuffers_headers)

