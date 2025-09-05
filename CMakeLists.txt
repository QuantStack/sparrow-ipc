cmake_minimum_required(VERSION 3.28)

project(sparrow-ipc CXX)

set(CMAKE_CXX_STANDARD 20 CACHE STRING "C++ Standard")
set(CMAKE_CXX_STANDARD_REQUIRED ON CACHE BOOL "C++ Standard Required")
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)
include(CMakeDependentOption)

list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")
message(DEBUG "CMake module path: ${CMAKE_MODULE_PATH}")

include(external_dependencies)

set(SPARROW_IPC_COMPILE_DEFINITIONS "" CACHE STRING "List of public compile definitions of the sparrow-ipc target")

# Linter options
# =============
OPTION(ACTIVATE_LINTER "Create targets to run clang-format" OFF)
MESSAGE(STATUS "🔧 Activate linter: ${ACTIVATE_LINTER}")
cmake_dependent_option(ACTIVATE_LINTER_DURING_COMPILATION "Run linter during the compilation" ON "ACTIVATE_LINTER" OFF)

if(ACTIVATE_LINTER)
    include(clang-format)
    include(clang-tidy)
endif()


# Build options
# =============

OPTION(SPARROW_IPC_BUILD_SHARED "Build sparrow-ipc as a shared library" ON)

if(SPARROW_IPC_BUILD_SHARED)
    message(STATUS "🔧 Build shared library")
    set(SPARROW_IPC_LIBRARY_TYPE SHARED)
else()
    message(STATUS "🔧 Build static library")
    set(SPARROW_IPC_LIBRARY_TYPE STATIC)
    list(APPEND SPARROW_IPC_COMPILE_DEFINITIONS SPARROW_IPC_STATIC_LIB)
endif()

OPTION(SPARROW_IPC_BUILD_TESTS "Build sparrow-ipc test suite" OFF)
MESSAGE(STATUS "🔧 Build tests: ${SPARROW_IPC_BUILD_TESTS}")

OPTION(BUILD_DOCS  "Build sparrow-ipc documentation" OFF)
MESSAGE(STATUS "🔧 Build docs: ${BUILD_DOCS}")

# Code coverage
# =============
OPTION(SPARROW_IPC_ENABLE_COVERAGE "Enable sparrow-ipc test coverage" OFF)
MESSAGE(STATUS "🔧 Enable coverage: ${SPARROW_IPC_ENABLE_COVERAGE}")

if(SPARROW_IPC_ENABLE_COVERAGE)
    include(code_coverage)
endif()

set(SPARROW_IPC_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
set(SPARROW_IPC_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)

set(SPARROW_IPC_HEADERS
    ${SPARROW_IPC_INCLUDE_DIR}/config/config.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/serialize.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/serialize_primitive_array.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/serialize_null_array.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/utils.hpp
)

set(SPARROW_IPC_SRC
    ${SPARROW_IPC_SOURCE_DIR}/serialize.cpp
    ${SPARROW_IPC_SOURCE_DIR}/serialize_null_array.cpp
    ${SPARROW_IPC_SOURCE_DIR}/utils.cpp
)

# Fetch schemas from apache arrow
set(SCHEMA_DIR ${CMAKE_BINARY_DIR}/format)
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
    if(NOT EXISTS ${SCHEMA_DIR}/${filename})
        message(STATUS "Downloading schema: ${url}")
        file(DOWNLOAD ${url} ${SCHEMA_DIR}/${filename}
            STATUS status)
    endif()

    list(APPEND FLATBUFFERS_SCHEMAS ${SCHEMA_DIR}/${filename})
endforeach()

# Generate Flatbuffers C++ headers from the schemas
set(FLATBUFFERS_GENERATED_DIR ${CMAKE_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${FLATBUFFERS_GENERATED_DIR})

# Generate output files list
set(FLATBUFFERS_GENERATED_HEADERS "")
foreach(fbs_file IN LISTS FLATBUFFERS_SCHEMAS)
    # Generate the corresponding header file name
    get_filename_component(header_name ${fbs_file} NAME_WE)
    list(APPEND FLATBUFFERS_GENERATED_HEADERS "${FLATBUFFERS_GENERATED_DIR}/${header_name}_generated.h")
endforeach()

if(TARGET flatbuffers::flatc)
    set(FLATC_EXECUTABLE flatbuffers::flatc)
else()
    set(FLATC_EXECUTABLE flatc)
endif()

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

add_library(sparrow-ipc ${SPARROW_IPC_LIBRARY_TYPE} ${SPARROW_IPC_SRC} ${SPARROW_IPC_HEADERS})

target_compile_options(sparrow-ipc
    PRIVATE
        ${compile_options}
)

target_compile_definitions(sparrow-ipc
    PUBLIC
        ${SPARROW_IPC_COMPILE_DEFINITIONS})

if(UNIX)
    target_compile_options(sparrow-ipc PRIVATE "-fvisibility=hidden")
else()
    target_compile_definitions(sparrow-ipc PRIVATE SPARROW_IPC_EXPORTS)
endif()

target_include_directories(sparrow-ipc
    PUBLIC
        ${SPARROW_IPC_INCLUDE_DIR}
    PRIVATE
        ${SPARROW_IPC_SOURCE_DIR} )

target_link_libraries(sparrow-ipc 
    PUBLIC
        sparrow::sparrow
        flatbuffers::flatbuffers
    PRIVATE
        flatbuffers_interface)

add_dependencies(sparrow-ipc generate_flatbuffers_headers)

# Tests
# =====
if(SPARROW_IPC_BUILD_TESTS)
    message(STATUS "🧪 Create tests targets")
    enable_testing()
    add_subdirectory(tests)
endif()

# Docs
# ====
if(BUILD_DOCS)
    message(STATUS "📚 Create docs targets")
    add_subdirectory(docs)
endif()
