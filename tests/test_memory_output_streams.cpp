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

                CHECK(stream.is_open());
                CHECK_EQ(stream.size(), 0);
            }

            SUBCASE("Construction with non-empty buffer")
            {
                std::vector<uint8_t> buffer = {1, 2, 3, 4, 5};
                memory_output_stream stream(buffer);

                CHECK(stream.is_open());
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

                auto written = stream.write(span);

                CHECK_EQ(written, 1);
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

                auto written = stream.write(span);

                CHECK_EQ(written, 5);
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

                auto written = stream.write(empty_span);

                CHECK_EQ(written, 0);
                CHECK_EQ(stream.size(), 0);
                CHECK_EQ(buffer.size(), 0);
            }

            SUBCASE("Write single byte (convenience method)")
            {
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                uint8_t single_byte = 123;
                auto written = stream.write(std::span<const uint8_t, 1>{&single_byte, 1});

                CHECK_EQ(written, 1);
                CHECK_EQ(stream.size(), 1);
                CHECK_EQ(buffer.size(), 1);
                CHECK_EQ(buffer[0], 123);
            }

            SUBCASE("Write value multiple times")
            {
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                auto written = stream.write(static_cast<uint8_t>(255), 3);

                CHECK_EQ(written, 3);
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

                auto written = stream.write(static_cast<uint8_t>(42), 0);

                CHECK_EQ(written, 0);
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
            auto written1 = stream.write(span1);

            CHECK_EQ(written1, 3);
            CHECK_EQ(stream.size(), 3);

            // Second write
            uint8_t data2[] = {40, 50};
            std::span<const uint8_t> span2(data2, 2);
            auto written2 = stream.write(span2);

            CHECK_EQ(written2, 2);
            CHECK_EQ(stream.size(), 5);

            // Third write with repeated value
            auto written3 = stream.write(static_cast<uint8_t>(60), 2);

            CHECK_EQ(written3, 2);
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

        TEST_CASE("add_padding functionality")
        {
            std::vector<uint8_t> buffer;
            memory_output_stream stream(buffer);

            SUBCASE("No padding needed when size is multiple of 8")
            {
                // Write 8 bytes
                uint8_t data[] = {1, 2, 3, 4, 5, 6, 7, 8};
                std::span<const uint8_t> span(data, 8);
                stream.write(span);

                auto size_before = stream.size();
                stream.add_padding();

                CHECK_EQ(stream.size(), size_before);
                CHECK_EQ(buffer.size(), 8);
            }

            SUBCASE("Padding needed when size is not multiple of 8")
            {
                // Write 5 bytes
                uint8_t data[] = {1, 2, 3, 4, 5};
                std::span<const uint8_t> span(data, 5);
                stream.write(span);

                stream.add_padding();

                CHECK_EQ(stream.size(), 8);  // Should be padded to next multiple of 8
                CHECK_EQ(buffer.size(), 8);

                // Check padding bytes are zero
                CHECK_EQ(buffer[5], 0);
                CHECK_EQ(buffer[6], 0);
                CHECK_EQ(buffer[7], 0);
            }

            SUBCASE("Padding for different sizes")
            {
                // Test various sizes and their expected padding
                std::vector<std::pair<size_t, size_t>> test_cases = {
                    {0, 0},  // 0 -> 0 (no padding needed)
                    {1, 7},  // 1 -> 8 (7 padding bytes)
                    {2, 6},  // 2 -> 8 (6 padding bytes)
                    {3, 5},  // 3 -> 8 (5 padding bytes)
                    {4, 4},  // 4 -> 8 (4 padding bytes)
                    {5, 3},  // 5 -> 8 (3 padding bytes)
                    {6, 2},  // 6 -> 8 (2 padding bytes)
                    {7, 1},  // 7 -> 8 (1 padding byte)
                    {8, 0},  // 8 -> 8 (no padding needed)
                    {9, 7},  // 9 -> 16 (7 padding bytes)
                };

                for (const auto& [initial_size, expected_padding] : test_cases)
                {
                    std::vector<uint8_t> test_buffer;
                    memory_output_stream test_stream(test_buffer);

                    // Write initial_size bytes
                    if (initial_size > 0)
                    {
                        std::vector<uint8_t> data(initial_size, 42);
                        std::span<const uint8_t> span(data);
                        test_stream.write(span);
                    }

                    auto size_before = test_stream.size();
                    test_stream.add_padding();
                    auto size_after = test_stream.size();

                    CHECK_EQ(size_before, initial_size);
                    CHECK_EQ(size_after - size_before, expected_padding);
                    CHECK_EQ(size_after % 8, 0);  // Should always be multiple of 8
                }
            }
        }

        TEST_CASE("stream lifecycle")
        {
            std::vector<uint8_t> buffer;
            memory_output_stream stream(buffer);

            SUBCASE("Stream is initially open")
            {
                CHECK(stream.is_open());
            }

            SUBCASE("Flush operation")
            {
                uint8_t data[] = {1, 2, 3};
                std::span<const uint8_t> span(data, 3);
                stream.write(span);

                // Flush should not throw or change state for memory stream
                CHECK_NOTHROW(stream.flush());
                CHECK(stream.is_open());
                CHECK_EQ(stream.size(), 3);
            }

            SUBCASE("Close operation")
            {
                uint8_t data[] = {1, 2, 3};
                std::span<const uint8_t> span(data, 3);
                stream.write(span);

                // Close should not throw for memory stream
                CHECK_NOTHROW(stream.close());
                CHECK(stream.is_open());  // Memory stream should remain open
                CHECK_EQ(stream.size(), 3);
            }
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
            auto written = stream.write(span);

            CHECK_EQ(written, large_size);
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

                auto written = stream.write(std::numeric_limits<uint8_t>::max(), 255);

                CHECK_EQ(written, 255);
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

                auto written = stream.write(static_cast<uint8_t>(0), 100);

                CHECK_EQ(written, 100);
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