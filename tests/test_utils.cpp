#include "doctest/doctest.h"

#include "utils.hpp"

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
            CHECK_EQ(utils::get_flatbuffer_type(builder, "n").first, org::apache::arrow::flatbuf::Type::Null);
            CHECK_EQ(utils::get_flatbuffer_type(builder, "b").first, org::apache::arrow::flatbuf::Type::Bool);
        }

        SUBCASE("Integer types")
        {
            CHECK_EQ(utils::get_flatbuffer_type(builder, "c").first, org::apache::arrow::flatbuf::Type::Int); // INT8
            CHECK_EQ(utils::get_flatbuffer_type(builder, "C").first, org::apache::arrow::flatbuf::Type::Int); // UINT8
            CHECK_EQ(utils::get_flatbuffer_type(builder, "s").first, org::apache::arrow::flatbuf::Type::Int); // INT16
            CHECK_EQ(utils::get_flatbuffer_type(builder, "S").first, org::apache::arrow::flatbuf::Type::Int); // UINT16
            CHECK_EQ(utils::get_flatbuffer_type(builder, "i").first, org::apache::arrow::flatbuf::Type::Int); // INT32
            CHECK_EQ(utils::get_flatbuffer_type(builder, "I").first, org::apache::arrow::flatbuf::Type::Int); // UINT32
            CHECK_EQ(utils::get_flatbuffer_type(builder, "l").first, org::apache::arrow::flatbuf::Type::Int); // INT64
            CHECK_EQ(utils::get_flatbuffer_type(builder, "L").first, org::apache::arrow::flatbuf::Type::Int); // UINT64
        }

        SUBCASE("Floating Point types")
        {
            CHECK_EQ(utils::get_flatbuffer_type(builder, "e").first, org::apache::arrow::flatbuf::Type::FloatingPoint); // HALF_FLOAT
            CHECK_EQ(utils::get_flatbuffer_type(builder, "f").first, org::apache::arrow::flatbuf::Type::FloatingPoint); // FLOAT
            CHECK_EQ(utils::get_flatbuffer_type(builder, "g").first, org::apache::arrow::flatbuf::Type::FloatingPoint); // DOUBLE
        }

        SUBCASE("String and Binary types")
        {
            CHECK_EQ(utils::get_flatbuffer_type(builder, "u").first, org::apache::arrow::flatbuf::Type::Utf8); // STRING
            CHECK_EQ(utils::get_flatbuffer_type(builder, "U").first, org::apache::arrow::flatbuf::Type::LargeUtf8); // LARGE_STRING
            CHECK_EQ(utils::get_flatbuffer_type(builder, "z").first, org::apache::arrow::flatbuf::Type::Binary); // BINARY
            CHECK_EQ(utils::get_flatbuffer_type(builder, "Z").first, org::apache::arrow::flatbuf::Type::LargeBinary); // LARGE_BINARY
            CHECK_EQ(utils::get_flatbuffer_type(builder, "vu").first, org::apache::arrow::flatbuf::Type::Utf8View); // STRING_VIEW
            CHECK_EQ(utils::get_flatbuffer_type(builder, "vz").first, org::apache::arrow::flatbuf::Type::BinaryView); // BINARY_VIEW
        }

        SUBCASE("Date types")
        {
            CHECK_EQ(utils::get_flatbuffer_type(builder, "tdD").first, org::apache::arrow::flatbuf::Type::Date); // DATE_DAYS
            CHECK_EQ(utils::get_flatbuffer_type(builder, "tdm").first, org::apache::arrow::flatbuf::Type::Date); // DATE_MILLISECONDS
        }

        SUBCASE("Timestamp types")
        {
            CHECK_EQ(utils::get_flatbuffer_type(builder, "tss:").first, org::apache::arrow::flatbuf::Type::Timestamp); // TIMESTAMP_SECONDS
            CHECK_EQ(utils::get_flatbuffer_type(builder, "tsm:").first, org::apache::arrow::flatbuf::Type::Timestamp); // TIMESTAMP_MILLISECONDS
            CHECK_EQ(utils::get_flatbuffer_type(builder, "tsu:").first, org::apache::arrow::flatbuf::Type::Timestamp); // TIMESTAMP_MICROSECONDS
            CHECK_EQ(utils::get_flatbuffer_type(builder, "tsn:").first, org::apache::arrow::flatbuf::Type::Timestamp); // TIMESTAMP_NANOSECONDS
        }

        SUBCASE("Duration types")
        {
            CHECK_EQ(utils::get_flatbuffer_type(builder, "tDs").first, org::apache::arrow::flatbuf::Type::Duration); // DURATION_SECONDS
            CHECK_EQ(utils::get_flatbuffer_type(builder, "tDm").first, org::apache::arrow::flatbuf::Type::Duration); // DURATION_MILLISECONDS
            CHECK_EQ(utils::get_flatbuffer_type(builder, "tDu").first, org::apache::arrow::flatbuf::Type::Duration); // DURATION_MICROSECONDS
            CHECK_EQ(utils::get_flatbuffer_type(builder, "tDn").first, org::apache::arrow::flatbuf::Type::Duration); // DURATION_NANOSECONDS
        }

        SUBCASE("Interval types")
        {
            CHECK_EQ(utils::get_flatbuffer_type(builder, "tiM").first, org::apache::arrow::flatbuf::Type::Interval); // INTERVAL_MONTHS
            CHECK_EQ(utils::get_flatbuffer_type(builder, "tiD").first, org::apache::arrow::flatbuf::Type::Interval); // INTERVAL_DAYS_TIME
            CHECK_EQ(utils::get_flatbuffer_type(builder, "tin").first, org::apache::arrow::flatbuf::Type::Interval); // INTERVAL_MONTHS_DAYS_NANOSECONDS
        }

        SUBCASE("Time types")
        {
            CHECK_EQ(utils::get_flatbuffer_type(builder, "tts").first, org::apache::arrow::flatbuf::Type::Time); // TIME_SECONDS
            CHECK_EQ(utils::get_flatbuffer_type(builder, "ttm").first, org::apache::arrow::flatbuf::Type::Time); // TIME_MILLISECONDS
            CHECK_EQ(utils::get_flatbuffer_type(builder, "ttu").first, org::apache::arrow::flatbuf::Type::Time); // TIME_MICROSECONDS
            CHECK_EQ(utils::get_flatbuffer_type(builder, "ttn").first, org::apache::arrow::flatbuf::Type::Time); // TIME_NANOSECONDS
        }

        SUBCASE("List types")
        {
            CHECK_EQ(utils::get_flatbuffer_type(builder, "+l").first, org::apache::arrow::flatbuf::Type::List); // LIST
            CHECK_EQ(utils::get_flatbuffer_type(builder, "+L").first, org::apache::arrow::flatbuf::Type::LargeList); // LARGE_LIST
            CHECK_EQ(utils::get_flatbuffer_type(builder, "+vl").first, org::apache::arrow::flatbuf::Type::ListView); // LIST_VIEW
            CHECK_EQ(utils::get_flatbuffer_type(builder, "+vL").first, org::apache::arrow::flatbuf::Type::LargeListView); // LARGE_LIST_VIEW
            CHECK_EQ(utils::get_flatbuffer_type(builder, "+w:16").first, org::apache::arrow::flatbuf::Type::FixedSizeList); // FIXED_SIZED_LIST
            CHECK_THROWS(utils::get_flatbuffer_type(builder, "+w:")); // Invalid FixedSizeList format
        }

        SUBCASE("Struct and Map types")
        {
            CHECK_EQ(utils::get_flatbuffer_type(builder, "+s").first, org::apache::arrow::flatbuf::Type::Struct_); // STRUCT
            CHECK_EQ(utils::get_flatbuffer_type(builder, "+m").first, org::apache::arrow::flatbuf::Type::Map); // MAP
        }

        SUBCASE("Union types")
        {
            CHECK_EQ(utils::get_flatbuffer_type(builder, "+ud:").first, org::apache::arrow::flatbuf::Type::Union); // DENSE_UNION
            CHECK_EQ(utils::get_flatbuffer_type(builder, "+us:").first, org::apache::arrow::flatbuf::Type::Union); // SPARSE_UNION
        }

        SUBCASE("Run-End Encoded type")
        {
            CHECK_EQ(utils::get_flatbuffer_type(builder, "+r").first, org::apache::arrow::flatbuf::Type::RunEndEncoded); // RUN_ENCODED
        }

        SUBCASE("Decimal types")
        {
            CHECK_EQ(utils::get_flatbuffer_type(builder, "d:10,5").first, org::apache::arrow::flatbuf::Type::Decimal); // DECIMAL (general)
            CHECK_THROWS(utils::get_flatbuffer_type(builder, "d:10")); // Invalid Decimal format
        }

        SUBCASE("Fixed Width Binary type")
        {
            CHECK_EQ(utils::get_flatbuffer_type(builder, "w:32").first, org::apache::arrow::flatbuf::Type::FixedSizeBinary); // FIXED_WIDTH_BINARY
            CHECK_THROWS(utils::get_flatbuffer_type(builder, "w:")); // Invalid FixedSizeBinary format
        }

        SUBCASE("Unsupported type returns Null")
        {
            CHECK_EQ(utils::get_flatbuffer_type(builder, "unsupported_format").first, org::apache::arrow::flatbuf::Type::Null);
        }
    }
}
