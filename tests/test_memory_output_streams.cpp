#include <array>
#include <limits>
#include <numeric>
#include <span>
#include <string>
#include <vector>

#include <sparrow_ipc/memory_output_stream.hpp>

#include "doctest/doctest.h"

namespace sparrow_ipc
{
    TEST_SUITE("memory_output_stream")
    {
        TEST_CASE("basic construction")
        {
            SUBCASE("Construction with std::vector")
            {
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);
                CHECK_EQ(stream.size(), 0);
            }

            SUBCASE("Construction with non-empty buffer")
            {
                std::vector<uint8_t> buffer = {1, 2, 3, 4, 5};
                memory_output_stream stream(buffer);
                CHECK_EQ(stream.size(), 5);
            }
        }

        TEST_CASE("write operations")
        {
            SUBCASE("Write single byte span")
            {
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                uint8_t data[] = {42};
                std::span<const uint8_t> span(data, 1);

                stream.write(span);

                CHECK_EQ(stream.size(), 1);
                CHECK_EQ(buffer.size(), 1);
                CHECK_EQ(buffer[0], 42);
            }

            SUBCASE("Write multiple bytes span")
            {
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                uint8_t data[] = {1, 2, 3, 4, 5};
                std::span<const uint8_t> span(data, 5);

                stream.write(span);

                CHECK_EQ(stream.size(), 5);
                CHECK_EQ(buffer.size(), 5);
                for (size_t i = 0; i < 5; ++i)
                {
                    CHECK_EQ(buffer[i], i + 1);
                }
            }

            SUBCASE("Write empty span")
            {
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                std::span<const uint8_t> empty_span;

                stream.write(empty_span);

                CHECK_EQ(stream.size(), 0);
                CHECK_EQ(buffer.size(), 0);
            }

            SUBCASE("Write single byte (convenience method)")
            {
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                uint8_t single_byte = 123;
                stream.write(std::span<const uint8_t, 1>{&single_byte, 1});

                CHECK_EQ(stream.size(), 1);
                CHECK_EQ(buffer.size(), 1);
                CHECK_EQ(buffer[0], 123);
            }

            SUBCASE("Write value multiple times")
            {
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                stream.write(static_cast<uint8_t>(255), 3);


                CHECK_EQ(stream.size(), 3);
                CHECK_EQ(buffer.size(), 3);
                CHECK_EQ(buffer[0], 255);
                CHECK_EQ(buffer[1], 255);
                CHECK_EQ(buffer[2], 255);
            }

            SUBCASE("Write value zero times")
            {
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                stream.write(static_cast<uint8_t>(42), 0);

                CHECK_EQ(stream.size(), 0);
                CHECK_EQ(buffer.size(), 0);
            }
        }

        TEST_CASE("sequential writes")
        {
            std::vector<uint8_t> buffer;
            memory_output_stream stream(buffer);

            // First write
            uint8_t data1[] = {10, 20, 30};
            std::span<const uint8_t> span1(data1, 3);
            stream.write(span1);

            CHECK_EQ(stream.size(), 3);

            // Second write
            uint8_t data2[] = {40, 50};
            std::span<const uint8_t> span2(data2, 2);
            stream.write(span2);


            CHECK_EQ(stream.size(), 5);

            // Third write with repeated value
            stream.write(static_cast<uint8_t>(60), 2);

            CHECK_EQ(stream.size(), 7);

            // Verify final buffer content
            std::vector<uint8_t> expected = {10, 20, 30, 40, 50, 60, 60};
            CHECK_EQ(buffer, expected);
        }

        TEST_CASE("reserve functionality")
        {
            std::vector<uint8_t> buffer;
            memory_output_stream stream(buffer);

            // Reserve space
            stream.reserve(100);

            // Buffer should have reserved capacity but size should remain 0
            CHECK_GE(buffer.capacity(), 100);
            CHECK_EQ(stream.size(), 0);
            CHECK_EQ(buffer.size(), 0);

            // Writing should work normally after reserve
            uint8_t data[] = {1, 2, 3};
            std::span<const uint8_t> span(data, 3);
            stream.write(span);

            CHECK_EQ(stream.size(), 3);
            CHECK_EQ(buffer.size(), 3);
        }


        TEST_CASE("large data handling")
        {
            std::vector<uint8_t> buffer;
            memory_output_stream stream(buffer);

            // Write a large amount of data
            const size_t large_size = 10000;
            std::vector<uint8_t> large_data(large_size);
            std::iota(large_data.begin(), large_data.end(), 0);  // Fill with 0, 1, 2, ...

            std::span<const uint8_t> span(large_data);
            stream.write(span);

            CHECK_EQ(stream.size(), large_size);
            CHECK_EQ(buffer.size(), large_size);

            // Verify data integrity
            for (size_t i = 0; i < large_size; ++i)
            {
                CHECK_EQ(buffer[i], static_cast<uint8_t>(i));
            }
        }

        TEST_CASE("edge cases")
        {
            SUBCASE("Maximum value repeated writes")
            {
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                stream.write(std::numeric_limits<uint8_t>::max(), 255);

                CHECK_EQ(stream.size(), 255);
                for (size_t i = 0; i < 255; ++i)
                {
                    CHECK_EQ(buffer[i], std::numeric_limits<uint8_t>::max());
                }
            }

            SUBCASE("Zero byte repeated writes")
            {
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                stream.write(static_cast<uint8_t>(0), 100);

                CHECK_EQ(stream.size(), 100);
                for (size_t i = 0; i < 100; ++i)
                {
                    CHECK_EQ(buffer[i], 0);
                }
            }
        }

        TEST_CASE("different container types")
        {
            SUBCASE("With pre-filled vector")
            {
                std::vector<uint8_t> buffer = {100, 200};
                memory_output_stream stream(buffer);

                CHECK_EQ(stream.size(), 2);

                uint8_t data[] = {1, 2, 3};
                std::span<const uint8_t> span(data, 3);
                stream.write(span);

                CHECK_EQ(stream.size(), 5);
                std::vector<uint8_t> expected = {100, 200, 1, 2, 3};
                CHECK_EQ(buffer, expected);
            }
        }
    }
}