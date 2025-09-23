#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN

#include <string>

#include "doctest/doctest.h"

#include "sparrow_ipc/config/sparrow_ipc_version.hpp"

TEST_CASE("versions exist and are readable")
{
    using namespace sparrow_ipc;
    [[maybe_unused]] const std::string printable_version = std::string("sparrow-ipc version: ")
                                                           + std::to_string(SPARROW_IPC_VERSION_MAJOR) + "."
                                                           + std::to_string(SPARROW_IPC_VERSION_MINOR) + "."
                                                           + std::to_string(SPARROW_IPC_VERSION_PATCH);

    [[maybe_unused]] const std::string printable_binary_version = std::string("sparrow-ipc binary version: ")
                                                                  + std::to_string(SPARROW_IPC_BINARY_CURRENT) + "."
                                                                  + std::to_string(SPARROW_IPC_BINARY_REVISION) + "."
                                                                  + std::to_string(SPARROW_IPC_BINARY_AGE);
}
