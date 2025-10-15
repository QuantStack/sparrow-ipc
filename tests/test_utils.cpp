#include <doctest/doctest.h>
#include <sparrow.hpp>

#include "sparrow_ipc/arrow_interface/arrow_array_schema_common_release.hpp"
#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    TEST_CASE("align_to_8")
    {
        CHECK_EQ(utils::align_to_8(0), 0);
        CHECK_EQ(utils::align_to_8(1), 8);
        CHECK_EQ(utils::align_to_8(7), 8);
        CHECK_EQ(utils::align_to_8(8), 8);
        CHECK_EQ(utils::align_to_8(9), 16);
        CHECK_EQ(utils::align_to_8(15), 16);
        CHECK_EQ(utils::align_to_8(16), 16);
    }

    TEST_CASE("extract_words_after_colon")
    {
        SUBCASE("Basic case with multiple words")
        {
            auto result = utils::extract_words_after_colon("d:128,10");
            REQUIRE_EQ(result.size(), 2);
            CHECK_EQ(result[0], "128");
            CHECK_EQ(result[1], "10");
        }

        SUBCASE("Single word after colon")
        {
            auto result = utils::extract_words_after_colon("w:256");
            REQUIRE_EQ(result.size(), 1);
            CHECK_EQ(result[0], "256");
        }

        SUBCASE("Three words")
        {
            auto result = utils::extract_words_after_colon("d:10,5,128");
            REQUIRE_EQ(result.size(), 3);
            CHECK_EQ(result[0], "10");
            CHECK_EQ(result[1], "5");
            CHECK_EQ(result[2], "128");
        }

        SUBCASE("No colon in string")
        {
            auto result = utils::extract_words_after_colon("no_colon");
            CHECK_EQ(result.size(), 0);
        }

        SUBCASE("Colon at end")
        {
            auto result = utils::extract_words_after_colon("prefix:");
            CHECK_EQ(result.size(), 0);
        }

        SUBCASE("Empty string")
        {
            auto result = utils::extract_words_after_colon("");
            CHECK_EQ(result.size(), 0);
        }

        SUBCASE("Only colon and comma")
        {
            auto result = utils::extract_words_after_colon(":,");
            REQUIRE_EQ(result.size(), 2);
            CHECK_EQ(result[0], "");
            CHECK_EQ(result[1], "");
        }

        SUBCASE("Complex prefix")
        {
            auto result = utils::extract_words_after_colon("prefix:word1,word2,word3");
            REQUIRE_EQ(result.size(), 3);
            CHECK_EQ(result[0], "word1");
            CHECK_EQ(result[1], "word2");
            CHECK_EQ(result[2], "word3");
        }
    }

    TEST_CASE("parse_to_int32")
    {
        SUBCASE("Valid positive integer")
        {
            auto result = utils::parse_to_int32("123");
            REQUIRE(result.has_value());
            CHECK_EQ(result.value(), 123);
        }

        SUBCASE("Valid negative integer")
        {
            auto result = utils::parse_to_int32("-456");
            REQUIRE(result.has_value());
            CHECK_EQ(result.value(), -456);
        }

        SUBCASE("Zero")
        {
            auto result = utils::parse_to_int32("0");
            REQUIRE(result.has_value());
            CHECK_EQ(result.value(), 0);
        }

        SUBCASE("Large valid number")
        {
            auto result = utils::parse_to_int32("2147483647");  // INT32_MAX
            REQUIRE(result.has_value());
            CHECK_EQ(result.value(), 2147483647);
        }

        SUBCASE("Invalid - not a number")
        {
            auto result = utils::parse_to_int32("abc");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - empty string")
        {
            auto result = utils::parse_to_int32("");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - partial number with text")
        {
            auto result = utils::parse_to_int32("123abc");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - text with number")
        {
            auto result = utils::parse_to_int32("abc123");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - just a sign")
        {
            auto result = utils::parse_to_int32("-");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Valid with leading zeros")
        {
            auto result = utils::parse_to_int32("00123");
            REQUIRE(result.has_value());
            CHECK_EQ(result.value(), 123);
        }
    }

    TEST_CASE("get_flatbuffer_type")
    {
        flatbuffers::FlatBufferBuilder builder;
        SUBCASE("Null and Boolean types")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::NA)).first,
                org::apache::arrow::flatbuf::Type::Null
            );
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::BOOL)).first,
                org::apache::arrow::flatbuf::Type::Bool
            );
        }

        SUBCASE("Integer types")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::INT8)).first,
                org::apache::arrow::flatbuf::Type::Int
            );  // INT8
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::UINT8)).first,
                org::apache::arrow::flatbuf::Type::Int
            );  // UINT8
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::INT16)).first,
                org::apache::arrow::flatbuf::Type::Int
            );  // INT16
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::UINT16)).first,
                org::apache::arrow::flatbuf::Type::Int
            );  // UINT16
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::INT32)).first,
                org::apache::arrow::flatbuf::Type::Int
            );  // INT32
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::UINT32)).first,
                org::apache::arrow::flatbuf::Type::Int
            );  // UINT32
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::INT64)).first,
                org::apache::arrow::flatbuf::Type::Int
            );  // INT64
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::UINT64)).first,
                org::apache::arrow::flatbuf::Type::Int
            );  // UINT64
        }

        SUBCASE("Floating Point types")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::HALF_FLOAT))
                    .first,
                org::apache::arrow::flatbuf::Type::FloatingPoint
            );  // HALF_FLOAT
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::FLOAT)).first,
                org::apache::arrow::flatbuf::Type::FloatingPoint
            );  // FLOAT
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::DOUBLE)).first,
                org::apache::arrow::flatbuf::Type::FloatingPoint
            );  // DOUBLE
        }

        SUBCASE("String and Binary types")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::STRING)).first,
                org::apache::arrow::flatbuf::Type::Utf8
            );  // STRING
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::LARGE_STRING))
                    .first,
                org::apache::arrow::flatbuf::Type::LargeUtf8
            );  // LARGE_STRING
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::BINARY)).first,
                org::apache::arrow::flatbuf::Type::Binary
            );  // BINARY
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::LARGE_BINARY))
                    .first,
                org::apache::arrow::flatbuf::Type::LargeBinary
            );  // LARGE_BINARY
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, "vu").first,
                org::apache::arrow::flatbuf::Type::Utf8View
            );  // STRING_VIEW
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, "vz").first,
                org::apache::arrow::flatbuf::Type::BinaryView
            );  // BINARY_VIEW
        }

        SUBCASE("Date types")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::DATE_DAYS))
                    .first,
                org::apache::arrow::flatbuf::Type::Date
            );  // DATE_DAYS
            CHECK_EQ(
                utils::get_flatbuffer_type(
                    builder,
                    sparrow::data_type_to_format(sparrow::data_type::DATE_MILLISECONDS)
                )
                    .first,
                org::apache::arrow::flatbuf::Type::Date
            );  // DATE_MILLISECONDS
        }

        SUBCASE("Timestamp types")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(
                    builder,
                    sparrow::data_type_to_format(sparrow::data_type::TIMESTAMP_SECONDS)
                )
                    .first,
                org::apache::arrow::flatbuf::Type::Timestamp
            );  // TIMESTAMP_SECONDS
            CHECK_EQ(
                utils::get_flatbuffer_type(
                    builder,
                    sparrow::data_type_to_format(sparrow::data_type::TIMESTAMP_MILLISECONDS)
                )
                    .first,
                org::apache::arrow::flatbuf::Type::Timestamp
            );  // TIMESTAMP_MILLISECONDS
            CHECK_EQ(
                utils::get_flatbuffer_type(
                    builder,
                    sparrow::data_type_to_format(sparrow::data_type::TIMESTAMP_MICROSECONDS)
                )
                    .first,
                org::apache::arrow::flatbuf::Type::Timestamp
            );  // TIMESTAMP_MICROSECONDS
            CHECK_EQ(
                utils::get_flatbuffer_type(
                    builder,
                    sparrow::data_type_to_format(sparrow::data_type::TIMESTAMP_NANOSECONDS)
                )
                    .first,
                org::apache::arrow::flatbuf::Type::Timestamp
            );  // TIMESTAMP_NANOSECONDS
        }

        SUBCASE("Duration types")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(
                    builder,
                    sparrow::data_type_to_format(sparrow::data_type::DURATION_SECONDS)
                )
                    .first,
                org::apache::arrow::flatbuf::Type::Duration
            );  // DURATION_SECONDS
            CHECK_EQ(
                utils::get_flatbuffer_type(
                    builder,
                    sparrow::data_type_to_format(sparrow::data_type::DURATION_MILLISECONDS)
                )
                    .first,
                org::apache::arrow::flatbuf::Type::Duration
            );  // DURATION_MILLISECONDS
            CHECK_EQ(
                utils::get_flatbuffer_type(
                    builder,
                    sparrow::data_type_to_format(sparrow::data_type::DURATION_MICROSECONDS)
                )
                    .first,
                org::apache::arrow::flatbuf::Type::Duration
            );  // DURATION_MICROSECONDS
            CHECK_EQ(
                utils::get_flatbuffer_type(
                    builder,
                    sparrow::data_type_to_format(sparrow::data_type::DURATION_NANOSECONDS)
                )
                    .first,
                org::apache::arrow::flatbuf::Type::Duration
            );  // DURATION_NANOSECONDS
        }

        SUBCASE("Interval types")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::INTERVAL_MONTHS))
                    .first,
                org::apache::arrow::flatbuf::Type::Interval
            );  // INTERVAL_MONTHS
            CHECK_EQ(
                utils::get_flatbuffer_type(
                    builder,
                    sparrow::data_type_to_format(sparrow::data_type::INTERVAL_DAYS_TIME)
                )
                    .first,
                org::apache::arrow::flatbuf::Type::Interval
            );  // INTERVAL_DAYS_TIME
            CHECK_EQ(
                utils::get_flatbuffer_type(
                    builder,
                    sparrow::data_type_to_format(sparrow::data_type::INTERVAL_MONTHS_DAYS_NANOSECONDS)
                )
                    .first,
                org::apache::arrow::flatbuf::Type::Interval
            );  // INTERVAL_MONTHS_DAYS_NANOSECONDS
        }

        SUBCASE("Time types")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::TIME_SECONDS))
                    .first,
                org::apache::arrow::flatbuf::Type::Time
            );  // TIME_SECONDS
            CHECK_EQ(
                utils::get_flatbuffer_type(
                    builder,
                    sparrow::data_type_to_format(sparrow::data_type::TIME_MILLISECONDS)
                )
                    .first,
                org::apache::arrow::flatbuf::Type::Time
            );  // TIME_MILLISECONDS
            CHECK_EQ(
                utils::get_flatbuffer_type(
                    builder,
                    sparrow::data_type_to_format(sparrow::data_type::TIME_MICROSECONDS)
                )
                    .first,
                org::apache::arrow::flatbuf::Type::Time
            );  // TIME_MICROSECONDS
            CHECK_EQ(
                utils::get_flatbuffer_type(
                    builder,
                    sparrow::data_type_to_format(sparrow::data_type::TIME_NANOSECONDS)
                )
                    .first,
                org::apache::arrow::flatbuf::Type::Time
            );  // TIME_NANOSECONDS
        }

        SUBCASE("List types")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::LIST)).first,
                org::apache::arrow::flatbuf::Type::List
            );  // LIST
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::LARGE_LIST))
                    .first,
                org::apache::arrow::flatbuf::Type::LargeList
            );  // LARGE_LIST
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, "+vl").first,
                org::apache::arrow::flatbuf::Type::ListView
            );  // LIST_VIEW
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, "+vL").first,
                org::apache::arrow::flatbuf::Type::LargeListView
            );  // LARGE_LIST_VIEW
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, "+w:16").first,
                org::apache::arrow::flatbuf::Type::FixedSizeList
            );                                                         // FIXED_SIZED_LIST
            CHECK_THROWS(utils::get_flatbuffer_type(builder, "+w:"));  // Invalid FixedSizeList format
        }

        SUBCASE("Struct and Map types")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::STRUCT)).first,
                org::apache::arrow::flatbuf::Type::Struct_
            );  // STRUCT
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::MAP)).first,
                org::apache::arrow::flatbuf::Type::Map
            );  // MAP
        }

        SUBCASE("Union types")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, "+ud:").first,
                org::apache::arrow::flatbuf::Type::Union
            );  // DENSE_UNION
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, "+us:").first,
                org::apache::arrow::flatbuf::Type::Union
            );  // SPARSE_UNION
        }

        SUBCASE("Run-End Encoded type")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, "+r").first,
                org::apache::arrow::flatbuf::Type::RunEndEncoded
            );  // RUN_ENCODED
        }

        SUBCASE("Decimal types")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, "d:10,5").first,
                org::apache::arrow::flatbuf::Type::Decimal
            );                                                          // DECIMAL (general)
            CHECK_THROWS(utils::get_flatbuffer_type(builder, "d:10"));  // Invalid Decimal format
        }

        SUBCASE("Fixed Width Binary type")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, "w:32").first,
                org::apache::arrow::flatbuf::Type::FixedSizeBinary
            );                                                        // FIXED_WIDTH_BINARY
            CHECK_THROWS(utils::get_flatbuffer_type(builder, "w:"));  // Invalid FixedSizeBinary format
        }

        SUBCASE("Unsupported type returns Null")
        {
            CHECK_EQ(
                utils::get_flatbuffer_type(builder, "unsupported_format").first,
                org::apache::arrow::flatbuf::Type::Null
            );
        }
    }
}
