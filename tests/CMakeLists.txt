cmake_minimum_required(VERSION 3.28)

set(test_target "test_sparrow_ipc_lib")

set(
    SPARROW_IPC_TESTS_SRC
    include/sparrow_ipc_tests_helpers.hpp
    # TODO move all the files below under src?
    main.cpp
    test_utils.cpp
    test_primitive_array_serialization.cpp
    test_null_array_serialization.cpp
)

add_executable(${test_target} ${SPARROW_IPC_TESTS_SRC})

target_link_libraries(${test_target}
    PRIVATE
        sparrow-ipc
        doctest::doctest
)

if(WIN32)
    add_custom_command(
        TARGET ${test_target} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
            "$<TARGET_FILE:sparrow::sparrow>"
            "$<TARGET_FILE_DIR:${test_target}>"
        COMMAND ${CMAKE_COMMAND} -E copy
            "$<TARGET_FILE:sparrow-ipc>"
            "$<TARGET_FILE_DIR:${test_target}>"
        COMMENT "Copying sparrow and sparrow-ipc DLLs to executable directory"
    )
endif()

target_include_directories(${test_target}
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_BINARY_DIR}/generated
        ${CMAKE_SOURCE_DIR}/include
)
add_dependencies(${test_target} generate_flatbuffers_headers)
add_test(NAME sparrow-ipc-tests COMMAND ${test_target})

add_custom_target(run_tests
    COMMAND ${test_target}
    DEPENDS 
        ${test_target}
    COMMENT "Running tests"
    USES_TERMINAL
)

set_target_properties(run_tests PROPERTIES FOLDER "Tests utilities")

set(JUNIT_REPORT_FILE_DOCTEST ${CMAKE_CURRENT_BINARY_DIR}/test_sparrow-ipc_lib_report_doctest.xml)

add_custom_target(run_tests_with_junit_report
    COMMAND ${test_target} --reporters=junit --out=${JUNIT_REPORT_FILE_DOCTEST} --no-path-filenames=true # TODO: use better junit as in sparrow
    DEPENDS
        ${test_target} 
    COMMENT "Running tests with JUnit reports saved to: ${JUNIT_REPORT_FILE_DOCTEST}"
    USES_TERMINAL
)

set_target_properties(run_tests_with_junit_report PROPERTIES FOLDER "Tests utilities")
