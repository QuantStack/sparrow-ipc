#include "sparrow_ipc/utils.hpp"

#include <charconv>
#include <stdexcept>
#include <string>
#include <vector>

namespace sparrow_ipc::utils
{
    namespace
    {
        // Parse the format string
        // The format string is expected to be "w:size", "+w:size", "d:precision,scale", etc
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
    }

    namespace utils
    {
        int64_t align_to_8(const int64_t n)
        {
            return (n + 7) & -8;
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
            const auto [ptr, ec] = std::from_chars(
                str.data(),
                str.data() + str.size(),
                value
            );

            if (ec != std::errc() || ptr != str.data() + str.size())
            {
                return std::nullopt;
            }
            return value;
        }
    }
}
