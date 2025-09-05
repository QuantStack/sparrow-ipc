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
