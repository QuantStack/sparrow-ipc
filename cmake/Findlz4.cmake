# Find LZ4 library and headers

# This module defines:
#  LZ4_FOUND          - True if lz4 is found
#  LZ4_INCLUDE_DIRS   - LZ4 include directories
#  LZ4_LIBRARIES      - Libraries needed to use LZ4
#  LZ4_VERSION        - LZ4 version number
#

find_package(PkgConfig)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(LZ4 QUIET liblz4)
    if(NOT LZ4_FOUND)
        message(STATUS "Did not find 'liblz4.pc', trying 'lz4.pc'")
        pkg_check_modules(LZ4 QUIET lz4)
    endif()
endif()

find_path(LZ4_INCLUDE_DIR lz4.h)
#      HINTS ${LZ4_INCLUDEDIR} ${LZ4_INCLUDE_DIRS})
find_library(LZ4_LIBRARY NAMES lz4 liblz4)
#     HINTS ${LZ4_LIBDIR} ${LZ4_LIBRARY_DIRS})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(lz4 DEFAULT_MSG
    LZ4_LIBRARY LZ4_INCLUDE_DIR)
mark_as_advanced(LZ4_INCLUDE_DIR LZ4_LIBRARY)

set(LZ4_LIBRARIES ${LZ4_LIBRARY})
set(LZ4_INCLUDE_DIRS ${LZ4_INCLUDE_DIR})

if(LZ4_FOUND AND NOT TARGET lz4::lz4)
    add_library(lz4::lz4 UNKNOWN IMPORTED)
    set_target_properties(lz4::lz4 PROPERTIES
        IMPORTED_LOCATION "${LZ4_LIBRARIES}"
        INTERFACE_INCLUDE_DIRECTORIES "${LZ4_INCLUDE_DIRS}")
    if (NOT TARGET LZ4::LZ4 AND TARGET lz4::lz4)
      add_library(LZ4::LZ4 ALIAS lz4::lz4)
    endif ()
endif()

#TODO add version?
###########

# find_path(LZ4_INCLUDE_DIR
#   NAMES lz4.h
#   DOC "lz4 include directory")
# mark_as_advanced(LZ4_INCLUDE_DIR)
# find_library(LZ4_LIBRARY
#   NAMES lz4 liblz4
#   DOC "lz4 library")
# mark_as_advanced(LZ4_LIBRARY)
# 
# if (LZ4_INCLUDE_DIR)
#   file(STRINGS "${LZ4_INCLUDE_DIR}/lz4.h" _lz4_version_lines
#     REGEX "#define[ \t]+LZ4_VERSION_(MAJOR|MINOR|RELEASE)")
#   string(REGEX REPLACE ".*LZ4_VERSION_MAJOR *\([0-9]*\).*" "\\1" _lz4_version_major "${_lz4_version_lines}")
#   string(REGEX REPLACE ".*LZ4_VERSION_MINOR *\([0-9]*\).*" "\\1" _lz4_version_minor "${_lz4_version_lines}")
#   string(REGEX REPLACE ".*LZ4_VERSION_RELEASE *\([0-9]*\).*" "\\1" _lz4_version_release "${_lz4_version_lines}")
#   set(LZ4_VERSION "${_lz4_version_major}.${_lz4_version_minor}.${_lz4_version_release}")
#   unset(_lz4_version_major)
#   unset(_lz4_version_minor)
#   unset(_lz4_version_release)
#   unset(_lz4_version_lines)
# endif ()
# 
# include(FindPackageHandleStandardArgs)
# find_package_handle_standard_args(LZ4
#   REQUIRED_VARS LZ4_LIBRARY LZ4_INCLUDE_DIR
#   VERSION_VAR LZ4_VERSION)
# 
# if (LZ4_FOUND)
#   # `lz4_FOUND` needs to be defined because:
#   #  - Other dependencies also resolve LZ4 using `find_package(lz4 ...)`
#   #  - CMake's syntax is case-sensitive
#   #
#   # See:
#   #  - https://github.com/facebook/rocksdb/blob/0836a2b26dfbbe30c15e8cebf47771917d55e760/cmake/RocksDBConfig.cmake.in#L36
#   #  - https://github.com/facebook/rocksdb/blob/0836a2b26dfbbe30c15e8cebf47771917d55e760/cmake/modules/Findlz4.cmake#L17
#   #  - https://github.com/man-group/ArcticDB/pull/961
#   set(lz4_FOUND TRUE)
#   set(LZ4_INCLUDE_DIRS "${LZ4_INCLUDE_DIR}")
#   set(LZ4_LIBRARIES "${LZ4_LIBRARY}")
# 
#   if (NOT TARGET LZ4::LZ4)
#     add_library(LZ4::LZ4 UNKNOWN IMPORTED)
#     set_target_properties(LZ4::LZ4 PROPERTIES
#       IMPORTED_LOCATION "${LZ4_LIBRARY}"
#       INTERFACE_INCLUDE_DIRECTORIES "${LZ4_INCLUDE_DIR}")
#     if (NOT TARGET lz4::lz4 AND TARGET LZ4::LZ4)
#       add_library(lz4::lz4 ALIAS LZ4::LZ4)
#     endif ()
#   endif ()
# endif ()
