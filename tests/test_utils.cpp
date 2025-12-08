#include <doctest/doctest.h>

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
}
