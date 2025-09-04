find_package(Doxygen REQUIRED)

set(DOXYGEN_AWESOME_DIR ${CMAKE_CURRENT_BINARY_DIR}/doxygen-awesome-css)
file(MAKE_DIRECTORY ${DOXYGEN_AWESOME_DIR})

set(BASE_URL "https://raw.githubusercontent.com/jothepro/doxygen-awesome-css/v2.3.4")

set(FILES_TO_DOWNLOAD
    LICENSE
    doxygen-awesome-darkmode-toggle.js
    doxygen-awesome-fragment-copy-button.js
    doxygen-awesome-interactive-toc.js
    doxygen-awesome-paragraph-link.js
    doxygen-awesome-sidebar-only-darkmode-toggle.css
    doxygen-awesome-sidebar-only.css
    doxygen-awesome-tabs.js
    doxygen-awesome.css
)

foreach(FILE ${FILES_TO_DOWNLOAD})
    if(NOT EXISTS ${DOXYGEN_AWESOME_DIR}/${FILE})
        message(STATUS "Downloading ${FILE}")
        file(DOWNLOAD ${BASE_URL}/${FILE} ${DOXYGEN_AWESOME_DIR}/${FILE} STATUS status)
    endif()
endforeach()

set(DOXYGEN_AWESOME_CSS_PATH "${DOXYGEN_AWESOME_DIR}/doxygen-awesome.css")

# Configure the doxygen.in file
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)

# Add a target to generate API documentation with Doxygen
add_custom_target(docs
    COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generating API documentation with Doxygen"
    VERBATIM
)

# Add install capabilities
install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/html DESTINATION share/doc)
