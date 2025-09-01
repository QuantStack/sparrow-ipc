#pragma once

#include <algorithm>
#include <array>
#include <istream>

namespace sparrow_ipc
{
    constexpr std::array<uint8_t, 4> continuation = {0xFF, 0xFF, 0xFF, 0xFF};

    constexpr std::array<uint8_t, 8> end_of_stream = {0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00};

    template <std::ranges::input_range R>
    [[nodiscard]] bool is_continuation(const R& buf)
    {
        return std::ranges::equal(buf, continuation);
    }

    [[nodiscard]] bool is_continuation(std::istream& stream);

    template <std::ranges::input_range R>
    [[nodiscard]] bool is_end_of_stream(const R& buf)
    {
        return std::ranges::equal(buf, end_of_stream);
    }

    [[nodiscard]] bool is_end_of_stream(std::istream& stream);
}