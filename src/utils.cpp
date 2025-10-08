#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc::utils
{
    std::optional<int32_t> parse_format(std::string_view format_str, std::string_view sep)
    {
        // Find the position of the delimiter
        const auto sep_pos = format_str.find(sep);
        if (sep_pos == std::string_view::npos)
        {
            return std::nullopt;
        }

        std::string_view substr_str(format_str.data() + sep_pos + 1, format_str.size() - sep_pos - 1);

        int32_t substr_size = 0;
        const auto [ptr, ec] = std::from_chars(
            substr_str.data(),
            substr_str.data() + substr_str.size(),
            substr_size
        );

        if (ec != std::errc() || ptr != substr_str.data() + substr_str.size())
        {
            return std::nullopt;
        }
        return substr_size;
    }

    size_t align_to_8(const size_t n)
    {
        return (n + 7) & -8;
    }
}
