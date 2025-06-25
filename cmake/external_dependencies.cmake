include(FetchContent)

OPTION(FETCH_DEPENDENCIES_WITH_CMAKE "Fetch dependencies with CMake: Can be OFF, ON, or MISSING. If the latter, CMake will download only dependencies which are not previously found." OFF)
MESSAGE(STATUS "🔧 FETCH_DEPENDENCIES_WITH_CMAKE: ${FETCH_DEPENDENCIES_WITH_CMAKE}")

if(FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "OFF")
    set(FIND_PACKAGE_OPTIONS REQUIRED)
else()
    set(FIND_PACKAGE_OPTIONS QUIET)
endif()

if(NOT FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "ON")
    find_package(sparrow CONFIG ${FIND_PACKAGE_OPTIONS})
endif()

if(FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "ON" OR FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "MISSING")
    if(NOT sparrow_FOUND)
        # Fetching from `main` while waiting for a new tag
        # (https://github.com/man-group/sparrow/pull/463 needed)
        # TODO Fetch last version when available
        set(SPARROW_TAG main)
        message(STATUS "📦 Fetching sparrow ${SPARROW_TAG}")
        FetchContent_Declare(
            sparrow
            GIT_SHALLOW TRUE
            GIT_REPOSITORY https://github.com/man-group/sparrow.git
            GIT_TAG ${SPARROW_TAG}
            GIT_PROGRESS TRUE
            SYSTEM
            EXCLUDE_FROM_ALL)
        FetchContent_MakeAvailable(sparrow)
        message(STATUS "\t✅ Fetched sparrow ${SPARROW_TAG}")
    else()
        message(STATUS "📦 sparrow found here: ${sparrow_DIR}")
    endif()
endif()

if(BUILD_TESTS)
    if(NOT FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "ON")
        find_package(doctest CONFIG ${FIND_PACKAGE_OPTIONS})
    endif()
    if(FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "ON" OR FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "MISSING")
        if(NOT doctest_FOUND)
            set(DOCTEST_VERSION "v2.4.12")
            message(STATUS "📦 Fetching doctest ${DOCTEST_VERSION}")
            FetchContent_Declare(
                doctest
                GIT_SHALLOW TRUE
                GIT_REPOSITORY https://github.com/doctest/doctest.git
                GIT_TAG ${DOCTEST_VERSION}
                GIT_PROGRESS TRUE
                SYSTEM
                EXCLUDE_FROM_ALL)
            FetchContent_MakeAvailable(doctest)
            message(STATUS "\t✅ Fetched doctest ${DOCTEST_VERSION}")
        else()
            message(STATUS "📦 doctest found here: ${doctest_DIR}")
        endif()
    endif()
endif()
