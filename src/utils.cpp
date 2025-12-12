#include "sparrow_ipc/utils.hpp"

#include <charconv>

namespace sparrow_ipc::utils
{
    std::optional<std::string_view> parse_after_separator(std::string_view format_str, std::string_view sep)
    {
        const auto sep_pos = format_str.find(sep);
        if (sep_pos == std::string_view::npos)
        {
            return std::nullopt;
        }
        return format_str.substr(sep_pos + sep.length());
    }

    std::optional<int32_t> parse_format(std::string_view format_str, std::string_view sep)
    {
        const auto substr_opt = parse_after_separator(format_str, sep);
        if (!substr_opt)
        {
            return std::nullopt;
        }
        const auto& substr_str = substr_opt.value();

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

    std::optional<std::string_view> parse_format_string(std::string_view format_str, std::string_view sep)
    {
        const size_t sep_pos = format_str.find(sep);
        if (sep_pos == std::string_view::npos)
        {
            return std::nullopt;
        }
        return format_str.substr(sep_pos + sep.length());
    }

    size_t align_to_8(const size_t n)
    {
        return (n + 7) & -8;
    }

    std::optional<std::tuple<int32_t, int32_t, std::optional<int32_t>>> parse_decimal_format(std::string_view format_str)
    {
        // Format can be "d:precision,scale" or "d:precision,scale,bitWidth"
        // First, find the colon
        const auto colon_pos = format_str.find(':');
        if (colon_pos == std::string_view::npos)
        {
            return std::nullopt;
        }

        // Extract the part after the colon
        std::string_view params_str(format_str.data() + colon_pos + 1, format_str.size() - colon_pos - 1);

        // Find the first comma (between precision and scale)
        const auto first_comma_pos = params_str.find(',');
        if (first_comma_pos == std::string_view::npos)
        {
            return std::nullopt;
        }

        // Parse precision
        std::string_view precision_str(params_str.data(), first_comma_pos);
        int32_t precision = 0;
        auto [ptr1, ec1] = std::from_chars(
            precision_str.data(),
            precision_str.data() + precision_str.size(),
            precision
        );
        if (ec1 != std::errc() || ptr1 != precision_str.data() + precision_str.size())
        {
            return std::nullopt;
        }

        // Find the second comma (between scale and bitWidth, if present)
        const auto remaining_str = params_str.substr(first_comma_pos + 1);
        const auto second_comma_pos = remaining_str.find(',');

        std::string_view scale_str;
        std::optional<int32_t> bit_width;

        if (second_comma_pos == std::string_view::npos)
        {
            // Format is "d:precision,scale"
            scale_str = remaining_str;
        }
        else
        {
            // Format is "d:precision,scale,bitWidth"
            scale_str = std::string_view(remaining_str.data(), second_comma_pos);
            std::string_view bitwidth_str(remaining_str.data() + second_comma_pos + 1,
                                         remaining_str.size() - second_comma_pos - 1);

            // Parse bitWidth
            int32_t bw = 0;
            auto [ptr3, ec3] = std::from_chars(
                bitwidth_str.data(),
                bitwidth_str.data() + bitwidth_str.size(),
                bw
            );
            if (ec3 != std::errc() || ptr3 != bitwidth_str.data() + bitwidth_str.size())
            {
                return std::nullopt;
            }
            bit_width = bw;
        }

        // Parse scale
        int32_t scale = 0;
        auto [ptr2, ec2] = std::from_chars(
            scale_str.data(),
            scale_str.data() + scale_str.size(),
            scale
        );
        if (ec2 != std::errc() || ptr2 != scale_str.data() + scale_str.size())
        {
            return std::nullopt;
        }

        return std::make_tuple(precision, scale, bit_width);
    }

    std::vector<std::string_view> extract_words_after_colon(std::string_view str)
    {
        std::vector<std::string_view> result;

        // Find the position of ':'
        const auto colon_pos = str.find(':');
        if (colon_pos == std::string_view::npos)
        {
            return result;  // Return empty vector if ':' not found
        }

        // Get the substring after ':'
        std::string_view remaining = str.substr(colon_pos + 1);

        // If nothing after ':', return empty vector
        if (remaining.empty())
        {
            return result;
        }

        // Split by ','
        size_t start = 0;
        size_t comma_pos = remaining.find(',');

        while (comma_pos != std::string_view::npos)
        {
            result.push_back(remaining.substr(start, comma_pos - start));
            start = comma_pos + 1;
            comma_pos = remaining.find(',', start);
        }

        // Add the last word (or the only word if no comma was found)
        result.push_back(remaining.substr(start));

        return result;
    }

    std::optional<int32_t> parse_to_int32(std::string_view str)
    {
        int32_t value = 0;
        const auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), value);

        if (ec != std::errc() || ptr != str.data() + str.size())
        {
            return std::nullopt;
        }
        return value;
    }
}
