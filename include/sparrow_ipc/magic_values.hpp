#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <ranges>


namespace sparrow_ipc
{
    /**
     * Continuation value defined in the Arrow IPC specification:
     * https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format
     */
    constexpr std::array<uint8_t, 4> continuation = {0xFF, 0xFF, 0xFF, 0xFF};

    /**
     * End-of-stream marker defined in the Arrow IPC specification:
     * https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
     */
    constexpr std::array<uint8_t, 8> end_of_stream = {0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00};

    template <std::ranges::input_range R>
    [[nodiscard]] bool is_continuation(const R& buf)
    {
        return std::ranges::equal(buf, continuation);
    }

    template <std::ranges::input_range R>
    [[nodiscard]] bool is_end_of_stream(const R& buf)
    {
        return std::ranges::equal(buf, end_of_stream);
    }
}