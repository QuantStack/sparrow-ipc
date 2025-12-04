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
