#include "sparrow_ipc/utils.hpp"

#include <charconv>
#include <stdexcept>
#include <string>

#include "sparrow.hpp"

namespace sparrow_ipc
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

        // Creates a Flatbuffers Decimal type from a format string
        // The format string is expected to be in the format "d:precision,scale"
        std::pair<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>> get_flatbuffer_decimal_type(
            flatbuffers::FlatBufferBuilder& builder,
            std::string_view format_str,
            const int32_t bitWidth
        )
        {
            // Decimal requires precision and scale. We need to parse the format_str.
            // Format: "d:precision,scale"
            const auto scale = parse_format(format_str, ",");
            if (!scale.has_value())
            {
                throw std::runtime_error(
                    "Failed to parse Decimal " + std::to_string(bitWidth)
                    + " scale from format string: " + std::string(format_str)
                );
            }
            const size_t comma_pos = format_str.find(',');
            const auto precision = parse_format(format_str.substr(0, comma_pos), ":");
            if (!precision.has_value())
            {
                throw std::runtime_error(
                    "Failed to parse Decimal " + std::to_string(bitWidth)
                    + " precision from format string: " + std::string(format_str)
                );
            }
            const auto decimal_type = org::apache::arrow::flatbuf::CreateDecimal(
                builder,
                precision.value(),
                scale.value(),
                bitWidth
            );
            return {org::apache::arrow::flatbuf::Type::Decimal, decimal_type.Union()};
        }
    }

    namespace utils
    {
        int64_t align_to_8(const int64_t n)
        {
            return (n + 7) & -8;
        }

        std::pair<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>>
        get_flatbuffer_type(flatbuffers::FlatBufferBuilder& builder, std::string_view format_str)
        {
            const auto type = sparrow::format_to_data_type(format_str);
            switch (type)
            {
                case sparrow::data_type::NA:
                {
                    const auto null_type = org::apache::arrow::flatbuf::CreateNull(builder);
                    return {org::apache::arrow::flatbuf::Type::Null, null_type.Union()};
                }
                case sparrow::data_type::BOOL:
                {
                    const auto bool_type = org::apache::arrow::flatbuf::CreateBool(builder);
                    return {org::apache::arrow::flatbuf::Type::Bool, bool_type.Union()};
                }
                case sparrow::data_type::UINT8:
                {
                    const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 8, false);
                    return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
                }
                case sparrow::data_type::INT8:
                {
                    const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 8, true);
                    return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
                }
                case sparrow::data_type::UINT16:
                {
                    const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 16, false);
                    return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
                }
                case sparrow::data_type::INT16:
                {
                    const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 16, true);
                    return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
                }
                case sparrow::data_type::UINT32:
                {
                    const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 32, false);
                    return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
                }
                case sparrow::data_type::INT32:
                {
                    const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 32, true);
                    return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
                }
                case sparrow::data_type::UINT64:
                {
                    const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 64, false);
                    return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
                }
                case sparrow::data_type::INT64:
                {
                    const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 64, true);
                    return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
                }
                case sparrow::data_type::HALF_FLOAT:
                {
                    const auto fp_type = org::apache::arrow::flatbuf::CreateFloatingPoint(
                        builder,
                        org::apache::arrow::flatbuf::Precision::HALF
                    );
                    return {org::apache::arrow::flatbuf::Type::FloatingPoint, fp_type.Union()};
                }
                case sparrow::data_type::FLOAT:
                {
                    const auto fp_type = org::apache::arrow::flatbuf::CreateFloatingPoint(
                        builder,
                        org::apache::arrow::flatbuf::Precision::SINGLE
                    );
                    return {org::apache::arrow::flatbuf::Type::FloatingPoint, fp_type.Union()};
                }
                case sparrow::data_type::DOUBLE:
                {
                    const auto fp_type = org::apache::arrow::flatbuf::CreateFloatingPoint(
                        builder,
                        org::apache::arrow::flatbuf::Precision::DOUBLE
                    );
                    return {org::apache::arrow::flatbuf::Type::FloatingPoint, fp_type.Union()};
                }
                case sparrow::data_type::STRING:
                {
                    const auto string_type = org::apache::arrow::flatbuf::CreateUtf8(builder);
                    return {org::apache::arrow::flatbuf::Type::Utf8, string_type.Union()};
                }
                case sparrow::data_type::LARGE_STRING:
                {
                    const auto large_string_type = org::apache::arrow::flatbuf::CreateLargeUtf8(builder);
                    return {org::apache::arrow::flatbuf::Type::LargeUtf8, large_string_type.Union()};
                }
                case sparrow::data_type::BINARY:
                {
                    const auto binary_type = org::apache::arrow::flatbuf::CreateBinary(builder);
                    return {org::apache::arrow::flatbuf::Type::Binary, binary_type.Union()};
                }
                case sparrow::data_type::LARGE_BINARY:
                {
                    const auto large_binary_type = org::apache::arrow::flatbuf::CreateLargeBinary(builder);
                    return {org::apache::arrow::flatbuf::Type::LargeBinary, large_binary_type.Union()};
                }
                case sparrow::data_type::STRING_VIEW:
                {
                    const auto string_view_type = org::apache::arrow::flatbuf::CreateUtf8View(builder);
                    return {org::apache::arrow::flatbuf::Type::Utf8View, string_view_type.Union()};
                }
                case sparrow::data_type::BINARY_VIEW:
                {
                    const auto binary_view_type = org::apache::arrow::flatbuf::CreateBinaryView(builder);
                    return {org::apache::arrow::flatbuf::Type::BinaryView, binary_view_type.Union()};
                }
                case sparrow::data_type::DATE_DAYS:
                {
                    const auto date_type = org::apache::arrow::flatbuf::CreateDate(
                        builder,
                        org::apache::arrow::flatbuf::DateUnit::DAY
                    );
                    return {org::apache::arrow::flatbuf::Type::Date, date_type.Union()};
                }
                case sparrow::data_type::DATE_MILLISECONDS:
                {
                    const auto date_type = org::apache::arrow::flatbuf::CreateDate(
                        builder,
                        org::apache::arrow::flatbuf::DateUnit::MILLISECOND
                    );
                    return {org::apache::arrow::flatbuf::Type::Date, date_type.Union()};
                }
                case sparrow::data_type::TIMESTAMP_SECONDS:
                {
                    const auto timestamp_type = org::apache::arrow::flatbuf::CreateTimestamp(
                        builder,
                        org::apache::arrow::flatbuf::TimeUnit::SECOND
                    );
                    return {org::apache::arrow::flatbuf::Type::Timestamp, timestamp_type.Union()};
                }
                case sparrow::data_type::TIMESTAMP_MILLISECONDS:
                {
                    const auto timestamp_type = org::apache::arrow::flatbuf::CreateTimestamp(
                        builder,
                        org::apache::arrow::flatbuf::TimeUnit::MILLISECOND
                    );
                    return {org::apache::arrow::flatbuf::Type::Timestamp, timestamp_type.Union()};
                }
                case sparrow::data_type::TIMESTAMP_MICROSECONDS:
                {
                    const auto timestamp_type = org::apache::arrow::flatbuf::CreateTimestamp(
                        builder,
                        org::apache::arrow::flatbuf::TimeUnit::MICROSECOND
                    );
                    return {org::apache::arrow::flatbuf::Type::Timestamp, timestamp_type.Union()};
                }
                case sparrow::data_type::TIMESTAMP_NANOSECONDS:
                {
                    const auto timestamp_type = org::apache::arrow::flatbuf::CreateTimestamp(
                        builder,
                        org::apache::arrow::flatbuf::TimeUnit::NANOSECOND
                    );
                    return {org::apache::arrow::flatbuf::Type::Timestamp, timestamp_type.Union()};
                }
                case sparrow::data_type::DURATION_SECONDS:
                {
                    const auto duration_type = org::apache::arrow::flatbuf::CreateDuration(
                        builder,
                        org::apache::arrow::flatbuf::TimeUnit::SECOND
                    );
                    return {org::apache::arrow::flatbuf::Type::Duration, duration_type.Union()};
                }
                case sparrow::data_type::DURATION_MILLISECONDS:
                {
                    const auto duration_type = org::apache::arrow::flatbuf::CreateDuration(
                        builder,
                        org::apache::arrow::flatbuf::TimeUnit::MILLISECOND
                    );
                    return {org::apache::arrow::flatbuf::Type::Duration, duration_type.Union()};
                }
                case sparrow::data_type::DURATION_MICROSECONDS:
                {
                    const auto duration_type = org::apache::arrow::flatbuf::CreateDuration(
                        builder,
                        org::apache::arrow::flatbuf::TimeUnit::MICROSECOND
                    );
                    return {org::apache::arrow::flatbuf::Type::Duration, duration_type.Union()};
                }
                case sparrow::data_type::DURATION_NANOSECONDS:
                {
                    const auto duration_type = org::apache::arrow::flatbuf::CreateDuration(
                        builder,
                        org::apache::arrow::flatbuf::TimeUnit::NANOSECOND
                    );
                    return {org::apache::arrow::flatbuf::Type::Duration, duration_type.Union()};
                }
                case sparrow::data_type::INTERVAL_MONTHS:
                {
                    const auto interval_type = org::apache::arrow::flatbuf::CreateInterval(
                        builder,
                        org::apache::arrow::flatbuf::IntervalUnit::YEAR_MONTH
                    );
                    return {org::apache::arrow::flatbuf::Type::Interval, interval_type.Union()};
                }
                case sparrow::data_type::INTERVAL_DAYS_TIME:
                {
                    const auto interval_type = org::apache::arrow::flatbuf::CreateInterval(
                        builder,
                        org::apache::arrow::flatbuf::IntervalUnit::DAY_TIME
                    );
                    return {org::apache::arrow::flatbuf::Type::Interval, interval_type.Union()};
                }
                case sparrow::data_type::INTERVAL_MONTHS_DAYS_NANOSECONDS:
                {
                    const auto interval_type = org::apache::arrow::flatbuf::CreateInterval(
                        builder,
                        org::apache::arrow::flatbuf::IntervalUnit::MONTH_DAY_NANO
                    );
                    return {org::apache::arrow::flatbuf::Type::Interval, interval_type.Union()};
                }
                case sparrow::data_type::TIME_SECONDS:
                {
                    const auto time_type = org::apache::arrow::flatbuf::CreateTime(
                        builder,
                        org::apache::arrow::flatbuf::TimeUnit::SECOND,
                        32
                    );
                    return {org::apache::arrow::flatbuf::Type::Time, time_type.Union()};
                }
                case sparrow::data_type::TIME_MILLISECONDS:
                {
                    const auto time_type = org::apache::arrow::flatbuf::CreateTime(
                        builder,
                        org::apache::arrow::flatbuf::TimeUnit::MILLISECOND,
                        32
                    );
                    return {org::apache::arrow::flatbuf::Type::Time, time_type.Union()};
                }
                case sparrow::data_type::TIME_MICROSECONDS:
                {
                    const auto time_type = org::apache::arrow::flatbuf::CreateTime(
                        builder,
                        org::apache::arrow::flatbuf::TimeUnit::MICROSECOND,
                        64
                    );
                    return {org::apache::arrow::flatbuf::Type::Time, time_type.Union()};
                }
                case sparrow::data_type::TIME_NANOSECONDS:
                {
                    const auto time_type = org::apache::arrow::flatbuf::CreateTime(
                        builder,
                        org::apache::arrow::flatbuf::TimeUnit::NANOSECOND,
                        64
                    );
                    return {org::apache::arrow::flatbuf::Type::Time, time_type.Union()};
                }
                case sparrow::data_type::LIST:
                {
                    const auto list_type = org::apache::arrow::flatbuf::CreateList(builder);
                    return {org::apache::arrow::flatbuf::Type::List, list_type.Union()};
                }
                case sparrow::data_type::LARGE_LIST:
                {
                    const auto large_list_type = org::apache::arrow::flatbuf::CreateLargeList(builder);
                    return {org::apache::arrow::flatbuf::Type::LargeList, large_list_type.Union()};
                }
                case sparrow::data_type::LIST_VIEW:
                {
                    const auto list_view_type = org::apache::arrow::flatbuf::CreateListView(builder);
                    return {org::apache::arrow::flatbuf::Type::ListView, list_view_type.Union()};
                }
                case sparrow::data_type::LARGE_LIST_VIEW:
                {
                    const auto large_list_view_type = org::apache::arrow::flatbuf::CreateLargeListView(builder);
                    return {org::apache::arrow::flatbuf::Type::LargeListView, large_list_view_type.Union()};
                }
                case sparrow::data_type::FIXED_SIZED_LIST:
                {
                    // FixedSizeList requires listSize. We need to parse the format_str.
                    // Format: "+w:size"
                    const auto list_size = parse_format(format_str, ":");
                    if (!list_size.has_value())
                    {
                        throw std::runtime_error(
                            "Failed to parse FixedSizeList size from format string: " + std::string(format_str)
                        );
                    }

                    const auto fixed_size_list_type = org::apache::arrow::flatbuf::CreateFixedSizeList(
                        builder,
                        list_size.value()
                    );
                    return {org::apache::arrow::flatbuf::Type::FixedSizeList, fixed_size_list_type.Union()};
                }
                case sparrow::data_type::STRUCT:
                {
                    const auto struct_type = org::apache::arrow::flatbuf::CreateStruct_(builder);
                    return {org::apache::arrow::flatbuf::Type::Struct_, struct_type.Union()};
                }
                case sparrow::data_type::MAP:
                {
                    // not sorted keys
                    const auto map_type = org::apache::arrow::flatbuf::CreateMap(builder, false);
                    return {org::apache::arrow::flatbuf::Type::Map, map_type.Union()};
                }
                case sparrow::data_type::DENSE_UNION:
                {
                    const auto union_type = org::apache::arrow::flatbuf::CreateUnion(
                        builder,
                        org::apache::arrow::flatbuf::UnionMode::Dense,
                        0
                    );
                    return {org::apache::arrow::flatbuf::Type::Union, union_type.Union()};
                }
                case sparrow::data_type::SPARSE_UNION:
                {
                    const auto union_type = org::apache::arrow::flatbuf::CreateUnion(
                        builder,
                        org::apache::arrow::flatbuf::UnionMode::Sparse,
                        0
                    );
                    return {org::apache::arrow::flatbuf::Type::Union, union_type.Union()};
                }
                case sparrow::data_type::RUN_ENCODED:
                {
                    const auto run_end_encoded_type = org::apache::arrow::flatbuf::CreateRunEndEncoded(builder);
                    return {org::apache::arrow::flatbuf::Type::RunEndEncoded, run_end_encoded_type.Union()};
                }
                case sparrow::data_type::DECIMAL32:
                {
                    return get_flatbuffer_decimal_type(builder, format_str, 32);
                }
                case sparrow::data_type::DECIMAL64:
                {
                    return get_flatbuffer_decimal_type(builder, format_str, 64);
                }
                case sparrow::data_type::DECIMAL128:
                {
                    return get_flatbuffer_decimal_type(builder, format_str, 128);
                }
                case sparrow::data_type::DECIMAL256:
                {
                    return get_flatbuffer_decimal_type(builder, format_str, 256);
                }
                case sparrow::data_type::FIXED_WIDTH_BINARY:
                {
                    // FixedSizeBinary requires byteWidth. We need to parse the format_str.
                    // Format: "w:size"
                    const auto byte_width = parse_format(format_str, ":");
                    if (!byte_width.has_value())
                    {
                        throw std::runtime_error(
                            "Failed to parse FixedWidthBinary size from format string: "
                            + std::string(format_str)
                        );
                    }

                    const auto fixed_width_binary_type = org::apache::arrow::flatbuf::CreateFixedSizeBinary(
                        builder,
                        byte_width.value()
                    );
                    return {org::apache::arrow::flatbuf::Type::FixedSizeBinary, fixed_width_binary_type.Union()};
                }
                default:
                {
                    throw std::runtime_error("Unsupported data type for serialization");
                }
            }
        }
    }
}
