#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <istream>
#include <ranges>

namespace sparrow_ipc
{
    /**
     * Continuation value defined in the Arrow IPC specification:
     * https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format
     */
    inline constexpr std::array<std::uint8_t, 4> continuation = {0xFF, 0xFF, 0xFF, 0xFF};

    /**
     * End-of-stream marker defined in the Arrow IPC specification:
     * https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
     */
    inline constexpr std::array<std::uint8_t, 8> end_of_stream = {0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00};

    /**
     * Magic bytes for Arrow file format defined in the Arrow IPC specification:
     * https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format
     * The magic string is "ARROW1" (6 bytes) followed by 2 padding bytes to reach 8-byte alignment
     */
    inline constexpr std::array<std::uint8_t, 6> arrow_file_magic = {'A', 'R', 'R', 'O', 'W', '1'};
    inline constexpr std::size_t arrow_file_magic_size = 6;
    
    /**
     * Magic bytes with padding for file header (8 bytes total for alignment)
     */
    inline constexpr std::array<std::uint8_t, 8> arrow_file_header_magic = {'A', 'R', 'R', 'O', 'W', '1', 0x00, 0x00};

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

    template <std::ranges::input_range R>
    [[nodiscard]] bool is_arrow_file_magic(const R& buf)
    {
        if (std::ranges::size(buf) < arrow_file_magic_size)
        {
            return false;
        }
        auto buf_begin = std::ranges::begin(buf);
        return std::equal(buf_begin, buf_begin + arrow_file_magic_size, arrow_file_magic.begin());
    }
}
