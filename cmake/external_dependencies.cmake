include(FetchContent)

OPTION(FETCH_DEPENDENCIES_WITH_CMAKE "Fetch dependencies with CMake: Can be OFF, ON, or MISSING. If the latter, CMake will download only dependencies which are not previously found." OFF)
MESSAGE(STATUS "🔧 FETCH_DEPENDENCIES_WITH_CMAKE: ${FETCH_DEPENDENCIES_WITH_CMAKE}")

if(FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "OFF")
    set(FIND_PACKAGE_OPTIONS REQUIRED)
else()
    set(FIND_PACKAGE_OPTIONS QUIET)
endif()

function(find_package_or_fetch)
    set(options)
    set(oneValueArgs CONAN_PKG_NAME PACKAGE_NAME GIT_REPOSITORY TAG)
    set(multiValueArgs)
    cmake_parse_arguments(PARSE_ARGV 0 arg
        "${options}" "${oneValueArgs}" "${multiValueArgs}"
    )

    set(actual_pkg_name ${arg_PACKAGE_NAME})
    if(arg_CONAN_PKG_NAME)
        set(actual_pkg_name ${arg_CONAN_PKG_NAME})
    endif()

    if(NOT FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "ON")
        find_package(${actual_pkg_name} ${FIND_PACKAGE_OPTIONS})
    endif()

    if(FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "ON" OR FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "MISSING")
        if(NOT ${actual_pkg_name}_FOUND)
            message(STATUS "📦 Fetching ${arg_PACKAGE_NAME}")
            FetchContent_Declare(
                ${arg_PACKAGE_NAME}
                GIT_SHALLOW TRUE
                GIT_REPOSITORY ${arg_GIT_REPOSITORY}
                GIT_TAG ${arg_TAG}
                GIT_PROGRESS TRUE
                SYSTEM
                EXCLUDE_FROM_ALL)
            FetchContent_MakeAvailable(${arg_PACKAGE_NAME})
            message(STATUS "\t✅ Fetched ${arg_PACKAGE_NAME}")
        else()
            message(STATUS "📦 ${actual_pkg_name} found here: ${actual_pkg_name}_DIR")
        endif()
    endif()
endfunction()

set(SPARROW_BUILD_SHARED ${SPARROW_IPC_BUILD_SHARED})
if(${SPARROW_IPC_BUILD_TESTS})
    set(CREATE_JSON_READER_TARGET ON)
endif()
find_package_or_fetch(
    PACKAGE_NAME sparrow
    GIT_REPOSITORY https://github.com/man-group/sparrow.git
    TAG 1.1.0
)
unset(CREATE_JSON_READER_TARGET)

if(NOT TARGET sparrow::sparrow)
    add_library(sparrow::sparrow ALIAS sparrow)
endif()
if(${SPARROW_IPC_BUILD_TESTS})
    if(NOT TARGET sparrow::json_reader)
        add_library(sparrow::json_reader ALIAS json_reader)
    endif()
endif()

set(FLATBUFFERS_BUILD_TESTS OFF)
set(FLATBUFFERS_BUILD_SHAREDLIB ${SPARROW_IPC_BUILD_SHARED})
find_package_or_fetch(
    CONAN_PKG_NAME flatbuffers
    PACKAGE_NAME FlatBuffers
    GIT_REPOSITORY https://github.com/google/flatbuffers.git
    TAG v25.2.10
)

if(NOT TARGET flatbuffers::flatbuffers)
    add_library(flatbuffers::flatbuffers ALIAS flatbuffers)
endif()
unset(FLATBUFFERS_BUILD_TESTS CACHE)

if(SPARROW_IPC_BUILD_TESTS)
    find_package_or_fetch(
        PACKAGE_NAME doctest
        GIT_REPOSITORY https://github.com/doctest/doctest.git
        TAG v2.4.12
    )
    
    message(STATUS "📦 Fetching arrow-testing")
    cmake_policy(PUSH)
    cmake_policy(SET CMP0174 NEW)  # Suppress warning about FetchContent_Declare GIT_REPOSITORY
    # Fetch arrow-testing data (no CMake build needed)
    FetchContent_Declare(
        arrow-testing
        GIT_REPOSITORY https://github.com/apache/arrow-testing.git
        GIT_SHALLOW TRUE
        # CONFIGURE_COMMAND ""
        # BUILD_COMMAND ""
        # INSTALL_COMMAND ""
    )
    FetchContent_MakeAvailable(arrow-testing)
    cmake_policy(POP)
    
    # Create interface library for easy access to test data
    add_library(arrow-testing-data INTERFACE)
    message(STATUS "Arrow testing data directory: ${arrow-testing_SOURCE_DIR}")
    target_compile_definitions(arrow-testing-data INTERFACE
        ARROW_TESTING_DATA_DIR="${arrow-testing_SOURCE_DIR}"
    )
    message(STATUS "\t✅ Fetched arrow-testing")

    # Iterate over all the files in the arrow-testing-data source directiory. When it's a gz, extract in place.
    file(GLOB_RECURSE arrow_testing_data_targz_files CONFIGURE_DEPENDS
        "${arrow-testing_SOURCE_DIR}/data/arrow-ipc-stream/integration/1.0.0-littleendian/*.json.gz"
    )
    foreach(file_path IN LISTS arrow_testing_data_targz_files)
            cmake_path(GET file_path PARENT_PATH parent_dir)
            cmake_path(GET file_path STEM filename)
            set(destination_file_path "${parent_dir}/${filename}.json")
            if(EXISTS "${destination_file_path}")
                message(VERBOSE "File already extracted: ${destination_file_path}")
            else()
                message(STATUS "Extracting ${file_path}")
                if(WIN32)
                    execute_process(COMMAND powershell -Command "$i=\"${file_path}\"; $o=\"${destination_file_path}\"; [IO.Compression.GZipStream]::new([IO.File]::OpenRead($i),[IO.Compression.CompressionMode]::Decompress).CopyTo([IO.File]::Create($o))")
                else()
                    execute_process(COMMAND gunzip -kf "${file_path}")
                endif()
            endif()
    endforeach()

endif()
