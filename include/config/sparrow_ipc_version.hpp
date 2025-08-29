#pragma once

namespace sparrow_ipc
{
    constexpr int SPARROW_IPC_VERSION_MAJOR = 0;
    constexpr int SPARROW_IPC_VERSION_MINOR = 0;
    constexpr int SPARROW_IPC_VERSION_PATCH = 1;

    constexpr int SPARROW_IPC_BINARY_CURRENT = 0;
    constexpr int SPARROW_IPC_BINARY_REVISION = 0;
    constexpr int SPARROW_IPC_BINARY_AGE = 0;

    static_assert(SPARROW_IPC_BINARY_AGE <= SPARROW_IPC_BINARY_CURRENT, "SPARROW_IPC_BINARY_AGE cannot be greater than SPARROW_IPC_BINARY_CURRENT");
}
