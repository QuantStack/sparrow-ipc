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
    set(oneValueArgs PACKAGE_NAME VERSION GIT_REPOSITORY TAG)
    set(multiValueArgs)
    cmake_parse_arguments(PARSE_ARGV 0 arg
        "${options}" "${oneValueArgs}" "${multiValueArgs}"
    )
    if(NOT FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "ON")
        find_package(${arg_PACKAGE_NAME} ${FIND_PACKAGE_OPTIONS})
    endif()
    if(FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "ON" OR FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "MISSING")
        if(NOT ${arg_PACKAGE_NAME}_FOUND)
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
            message(STATUS "📦 ${arg_PACKAGE_NAME} found here: ${arg_PACKAGE_NAME}_DIR")
        endif()
    endif()
endfunction()

set(SPARROW_BUILD_SHARED ${SPARROW_IPC_BUILD_SHARED})
find_package_or_fetch(
    PACKAGE_NAME sparrow
    VERSION 1.0.0
    GIT_REPOSITORY https://github.com/man-group/sparrow.git
    TAG 1.0.0
)

if(NOT TARGET sparrow::sparrow)
    add_library(sparrow::sparrow ALIAS sparrow)
endif()

set(FLATBUFFERS_BUILD_TESTS OFF)
set(FLATBUFFERS_BUILD_SHAREDLIB ${SPARROW_IPC_BUILD_SHARED})
find_package_or_fetch(
    PACKAGE_NAME FlatBuffers
    VERSION v25.2.10
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
        VERSION v2.4.12
        GIT_REPOSITORY https://github.com/doctest/doctest.git
        TAG v2.4.12
    )
endif()