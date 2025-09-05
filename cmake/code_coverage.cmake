if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(WARNING "Code coverage results with an optimised (non-Debug) build may be misleading")
endif()

set(COVERAGE_REPORT_PATH "${CMAKE_BINARY_DIR}/coverage_reports" CACHE PATH "Path to store coverage reports")
set(COBERTURA_REPORT_PATH "${COVERAGE_REPORT_PATH}/cobertura.xml" CACHE PATH "Path to store cobertura report")
set(COVERAGE_TARGETS_FOLDER "Tests utilities/Code Coverage")

if(CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
    find_program(OpenCPPCoverage OpenCppCoverage.exe opencppcoverage.exe REQUIRED
        PATHS "C:/Program Files/OpenCppCoverage" "C:/Program Files (x86)/OpenCppCoverage")

    cmake_path(CONVERT ${CMAKE_SOURCE_DIR} TO_NATIVE_PATH_LIST OPENCPPCOVERAGE_SOURCES)
    set(OPENCPPCOVERAGE_COMMON_ARGS --sources=${OPENCPPCOVERAGE_SOURCES} --modules=${OPENCPPCOVERAGE_SOURCES} --excluded_sources=test*)

    add_custom_target(sparrow_ipc_generate_cobertura
        COMMAND ${OpenCPPCoverage}
            ${OPENCPPCOVERAGE_COMMON_ARGS}
            --export_type=cobertura:${COBERTURA_REPORT_PATH}
            -- $<TARGET_FILE:test_sparrow_ipc_lib>
        DEPENDS test_sparrow_ipc_lib
        COMMENT "Generating coverage cobertura report with OpenCppCoverage: ${COBERTURA_REPORT_PATH}"
    )
    set(TARGET_PROPERTIES sparrow_ipc_generate_cobertura PROPERTIES FOLDER ${COVERAGE_TARGETS_FOLDER})

    add_custom_target(sparrow_ipc_generate_html_coverage_report
        COMMAND ${OpenCPPCoverage}
            ${OPENCPPCOVERAGE_COMMON_ARGS}
            --export_type=html:${COVERAGE_REPORT_PATH}
            -- $<TARGET_FILE:test_sparrow_ipc_lib>
        DEPENDS test_sparrow_ipc_lib
        COMMENT "Generating coverage report with OpenCppCoverage: ${COVERAGE_REPORT_PATH}"
    )
    set(TARGET_PROPERTIES sparrow_ipc_generate_cobertura PROPERTIES FOLDER "Tests utilities/Code Coverage")
endif()
