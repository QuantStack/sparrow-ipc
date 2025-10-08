#include <array>
#include <limits>
#include <numeric>
#include <span>
#include <string>
#include <vector>

#include <sparrow_ipc/chunk_memory_output_stream.hpp>

#include "doctest/doctest.h"

namespace sparrow_ipc
{
    TEST_SUITE("chunked_memory_output_stream")
    {
        TEST_CASE("basic construction")
        {
            SUBCASE("Construction with empty vector of vectors")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                CHECK(stream.is_open());
                CHECK_EQ(stream.size(), 0);
                CHECK_EQ(chunks.size(), 0);
            }

            SUBCASE("Construction with existing chunks")
            {
                std::vector<std::vector<uint8_t>> chunks = {
                    {1, 2, 3},
                    {4, 5, 6, 7},
                    {8, 9}
                };
                chunked_memory_output_stream stream(chunks);

                CHECK(stream.is_open());
                CHECK_EQ(stream.size(), 9);
                CHECK_EQ(chunks.size(), 3);
            }
        }

        TEST_CASE("write operations with span")
        {
            SUBCASE("Write single byte span")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                uint8_t data[] = {42};
                std::span<const uint8_t> span(data, 1);

                auto written = stream.write(span);

                CHECK_EQ(written, 1);
                CHECK_EQ(stream.size(), 1);
                CHECK_EQ(chunks.size(), 1);
                CHECK_EQ(chunks[0].size(), 1);
                CHECK_EQ(chunks[0][0], 42);
            }

            SUBCASE("Write multiple bytes span")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                uint8_t data[] = {1, 2, 3, 4, 5};
                std::span<const uint8_t> span(data, 5);

                auto written = stream.write(span);

                CHECK_EQ(written, 5);
                CHECK_EQ(stream.size(), 5);
                CHECK_EQ(chunks.size(), 1);
                CHECK_EQ(chunks[0].size(), 5);
                for (size_t i = 0; i < 5; ++i)
                {
                    CHECK_EQ(chunks[0][i], i + 1);
                }
            }

            SUBCASE("Write empty span")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                std::span<const uint8_t> empty_span;

                auto written = stream.write(empty_span);

                CHECK_EQ(written, 0);
                CHECK_EQ(stream.size(), 0);
                CHECK_EQ(chunks.size(), 1);
                CHECK_EQ(chunks[0].size(), 0);
            }

            SUBCASE("Multiple span writes create multiple chunks")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                uint8_t data1[] = {10, 20};
                uint8_t data2[] = {30, 40, 50};
                uint8_t data3[] = {60};

                stream.write(std::span<const uint8_t>(data1, 2));
                stream.write(std::span<const uint8_t>(data2, 3));
                stream.write(std::span<const uint8_t>(data3, 1));

                CHECK_EQ(stream.size(), 6);
                CHECK_EQ(chunks.size(), 3);

                CHECK_EQ(chunks[0].size(), 2);
                CHECK_EQ(chunks[0][0], 10);
                CHECK_EQ(chunks[0][1], 20);

                CHECK_EQ(chunks[1].size(), 3);
                CHECK_EQ(chunks[1][0], 30);
                CHECK_EQ(chunks[1][1], 40);
                CHECK_EQ(chunks[1][2], 50);

                CHECK_EQ(chunks[2].size(), 1);
                CHECK_EQ(chunks[2][0], 60);
            }
        }

        TEST_CASE("write operations with move")
        {
            SUBCASE("Write moved vector")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                std::vector<uint8_t> buffer = {1, 2, 3, 4, 5};
                auto written = stream.write(std::move(buffer));

                CHECK_EQ(written, 5);
                CHECK_EQ(stream.size(), 5);
                CHECK_EQ(chunks.size(), 1);
                CHECK_EQ(chunks[0].size(), 5);
                for (size_t i = 0; i < 5; ++i)
                {
                    CHECK_EQ(chunks[0][i], i + 1);
                }
            }

            SUBCASE("Write multiple moved vectors")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                std::vector<uint8_t> buffer1 = {10, 20, 30};
                std::vector<uint8_t> buffer2 = {40, 50};
                std::vector<uint8_t> buffer3 = {60, 70, 80, 90};

                stream.write(std::move(buffer1));
                stream.write(std::move(buffer2));
                stream.write(std::move(buffer3));

                CHECK_EQ(stream.size(), 9);
                CHECK_EQ(chunks.size(), 3);

                CHECK_EQ(chunks[0].size(), 3);
                CHECK_EQ(chunks[1].size(), 2);
                CHECK_EQ(chunks[2].size(), 4);
            }

            SUBCASE("Write empty moved vector")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                std::vector<uint8_t> empty_buffer;
                auto written = stream.write(std::move(empty_buffer));

                CHECK_EQ(written, 0);
                CHECK_EQ(stream.size(), 0);
                CHECK_EQ(chunks.size(), 1);
                CHECK_EQ(chunks[0].size(), 0);
            }
        }

        TEST_CASE("write operations with repeated value")
        {
            SUBCASE("Write value multiple times")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                auto written = stream.write(static_cast<uint8_t>(255), 5);

                CHECK_EQ(written, 5);
                CHECK_EQ(stream.size(), 5);
                CHECK_EQ(chunks.size(), 1);
                CHECK_EQ(chunks[0].size(), 5);
                for (size_t i = 0; i < 5; ++i)
                {
                    CHECK_EQ(chunks[0][i], 255);
                }
            }

            SUBCASE("Write value zero times")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                auto written = stream.write(static_cast<uint8_t>(42), 0);

                CHECK_EQ(written, 0);
                CHECK_EQ(stream.size(), 0);
                CHECK_EQ(chunks.size(), 1);
                CHECK_EQ(chunks[0].size(), 0);
            }

            SUBCASE("Multiple repeated value writes")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                stream.write(static_cast<uint8_t>(100), 3);
                stream.write(static_cast<uint8_t>(200), 2);
                stream.write(static_cast<uint8_t>(50), 4);

                CHECK_EQ(stream.size(), 9);
                CHECK_EQ(chunks.size(), 3);

                CHECK_EQ(chunks[0].size(), 3);
                for (size_t i = 0; i < 3; ++i)
                {
                    CHECK_EQ(chunks[0][i], 100);
                }

                CHECK_EQ(chunks[1].size(), 2);
                for (size_t i = 0; i < 2; ++i)
                {
                    CHECK_EQ(chunks[1][i], 200);
                }

                CHECK_EQ(chunks[2].size(), 4);
                for (size_t i = 0; i < 4; ++i)
                {
                    CHECK_EQ(chunks[2][i], 50);
                }
            }
        }

        TEST_CASE("mixed write operations")
        {
            std::vector<std::vector<uint8_t>> chunks;
            chunked_memory_output_stream stream(chunks);

            // Write span
            uint8_t data[] = {1, 2, 3};
            stream.write(std::span<const uint8_t>(data, 3));

            // Write repeated value
            stream.write(static_cast<uint8_t>(42), 2);

            // Write moved vector
            std::vector<uint8_t> buffer = {10, 20, 30, 40};
            stream.write(std::move(buffer));

            CHECK_EQ(stream.size(), 9);
            CHECK_EQ(chunks.size(), 3);

            CHECK_EQ(chunks[0].size(), 3);
            CHECK_EQ(chunks[0][0], 1);
            CHECK_EQ(chunks[0][1], 2);
            CHECK_EQ(chunks[0][2], 3);

            CHECK_EQ(chunks[1].size(), 2);
            CHECK_EQ(chunks[1][0], 42);
            CHECK_EQ(chunks[1][1], 42);

            CHECK_EQ(chunks[2].size(), 4);
            CHECK_EQ(chunks[2][0], 10);
            CHECK_EQ(chunks[2][1], 20);
            CHECK_EQ(chunks[2][2], 30);
            CHECK_EQ(chunks[2][3], 40);
        }

        TEST_CASE("reserve functionality")
        {
            std::vector<std::vector<uint8_t>> chunks;
            chunked_memory_output_stream stream(chunks);

            // Reserve space
            stream.reserve(100);

            // Chunks vector should have reserved capacity but size should remain 0
            CHECK_GE(chunks.capacity(), 100);
            CHECK_EQ(stream.size(), 0);
            CHECK_EQ(chunks.size(), 0);

            // Writing should work normally after reserve
            uint8_t data[] = {1, 2, 3};
            std::span<const uint8_t> span(data, 3);
            stream.write(span);

            CHECK_EQ(stream.size(), 3);
            CHECK_EQ(chunks.size(), 1);
        }

        TEST_CASE("size calculation")
        {
            SUBCASE("Size with empty chunks")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                CHECK_EQ(stream.size(), 0);
            }

            SUBCASE("Size with pre-existing chunks")
            {
                std::vector<std::vector<uint8_t>> chunks = {
                    {1, 2, 3},
                    {4, 5},
                    {6, 7, 8, 9}
                };
                chunked_memory_output_stream stream(chunks);

                CHECK_EQ(stream.size(), 9);
            }

            SUBCASE("Size updates after writes")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                CHECK_EQ(stream.size(), 0);

                uint8_t data[] = {1, 2, 3};
                stream.write(std::span<const uint8_t>(data, 3));
                CHECK_EQ(stream.size(), 3);

                stream.write(static_cast<uint8_t>(42), 5);
                CHECK_EQ(stream.size(), 8);

                std::vector<uint8_t> buffer = {10, 20};
                stream.write(std::move(buffer));
                CHECK_EQ(stream.size(), 10);
            }

            SUBCASE("Size with chunks of varying sizes")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                stream.write(static_cast<uint8_t>(1), 1);
                stream.write(static_cast<uint8_t>(2), 10);
                stream.write(static_cast<uint8_t>(3), 100);
                stream.write(static_cast<uint8_t>(4), 1000);

                CHECK_EQ(stream.size(), 1111);
                CHECK_EQ(chunks.size(), 4);
            }
        }

        TEST_CASE("stream lifecycle")
        {
            std::vector<std::vector<uint8_t>> chunks;
            chunked_memory_output_stream stream(chunks);

            SUBCASE("Stream is always open")
            {
                CHECK(stream.is_open());

                uint8_t data[] = {1, 2, 3};
                stream.write(std::span<const uint8_t>(data, 3));
                CHECK(stream.is_open());

                stream.flush();
                CHECK(stream.is_open());

                stream.close();
                CHECK(stream.is_open());
            }

            SUBCASE("Flush operation")
            {
                uint8_t data[] = {1, 2, 3};
                stream.write(std::span<const uint8_t>(data, 3));

                // Flush should not throw or change state
                CHECK_NOTHROW(stream.flush());
                CHECK(stream.is_open());
                CHECK_EQ(stream.size(), 3);
                CHECK_EQ(chunks.size(), 1);
            }

            SUBCASE("Close operation")
            {
                uint8_t data[] = {1, 2, 3};
                stream.write(std::span<const uint8_t>(data, 3));

                // Close should not throw
                CHECK_NOTHROW(stream.close());
                CHECK(stream.is_open());
                CHECK_EQ(stream.size(), 3);
                CHECK_EQ(chunks.size(), 1);
            }
        }

        TEST_CASE("large data handling")
        {
            std::vector<std::vector<uint8_t>> chunks;
            chunked_memory_output_stream stream(chunks);

            SUBCASE("Single large chunk")
            {
                const size_t large_size = 10000;
                std::vector<uint8_t> large_data(large_size);
                std::iota(large_data.begin(), large_data.end(), 0);

                auto written = stream.write(std::move(large_data));

                CHECK_EQ(written, large_size);
                CHECK_EQ(stream.size(), large_size);
                CHECK_EQ(chunks.size(), 1);
                CHECK_EQ(chunks[0].size(), large_size);

                // Verify data integrity
                for (size_t i = 0; i < large_size; ++i)
                {
                    CHECK_EQ(chunks[0][i], static_cast<uint8_t>(i));
                }
            }

            SUBCASE("Many small chunks")
            {
                const size_t num_chunks = 1000;
                const size_t chunk_size = 10;

                for (size_t i = 0; i < num_chunks; ++i)
                {
                    uint8_t value = static_cast<uint8_t>(i);
                    stream.write(value, chunk_size);
                }

                CHECK_EQ(stream.size(), num_chunks * chunk_size);
                CHECK_EQ(chunks.size(), num_chunks);

                for (size_t i = 0; i < num_chunks; ++i)
                {
                    CHECK_EQ(chunks[i].size(), chunk_size);
                    for (size_t j = 0; j < chunk_size; ++j)
                    {
                        CHECK_EQ(chunks[i][j], static_cast<uint8_t>(i));
                    }
                }
            }

            SUBCASE("Large repeated value write")
            {
                const size_t count = 50000;
                auto written = stream.write(static_cast<uint8_t>(123), count);

                CHECK_EQ(written, count);
                CHECK_EQ(stream.size(), count);
                CHECK_EQ(chunks.size(), 1);
                CHECK_EQ(chunks[0].size(), count);

                for (size_t i = 0; i < count; ++i)
                {
                    CHECK_EQ(chunks[0][i], 123);
                }
            }
        }

        TEST_CASE("edge cases")
        {
            SUBCASE("Maximum value writes")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                auto written = stream.write(std::numeric_limits<uint8_t>::max(), 255);

                CHECK_EQ(written, 255);
                CHECK_EQ(stream.size(), 255);
                CHECK_EQ(chunks.size(), 1);
                for (size_t i = 0; i < 255; ++i)
                {
                    CHECK_EQ(chunks[0][i], std::numeric_limits<uint8_t>::max());
                }
            }

            SUBCASE("Zero byte writes")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                auto written = stream.write(static_cast<uint8_t>(0), 100);

                CHECK_EQ(written, 100);
                CHECK_EQ(stream.size(), 100);
                CHECK_EQ(chunks.size(), 1);
                for (size_t i = 0; i < 100; ++i)
                {
                    CHECK_EQ(chunks[0][i], 0);
                }
            }

            SUBCASE("Interleaved empty and non-empty writes")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                stream.write(static_cast<uint8_t>(1), 5);
                stream.write(static_cast<uint8_t>(2), 0);
                stream.write(static_cast<uint8_t>(3), 3);
                std::vector<uint8_t> empty;
                stream.write(std::move(empty));
                stream.write(static_cast<uint8_t>(4), 2);

                CHECK_EQ(stream.size(), 10);
                CHECK_EQ(chunks.size(), 5);

                CHECK_EQ(chunks[0].size(), 5);
                CHECK_EQ(chunks[1].size(), 0);
                CHECK_EQ(chunks[2].size(), 3);
                CHECK_EQ(chunks[3].size(), 0);
                CHECK_EQ(chunks[4].size(), 2);
            }
        }

        TEST_CASE("reference semantics")
        {
            SUBCASE("Stream modifies original chunks vector")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                uint8_t data[] = {1, 2, 3};
                stream.write(std::span<const uint8_t>(data, 3));

                // Verify that the original vector is modified
                CHECK_EQ(chunks.size(), 1);
                CHECK_EQ(chunks[0].size(), 3);
                CHECK_EQ(chunks[0][0], 1);
                CHECK_EQ(chunks[0][1], 2);
                CHECK_EQ(chunks[0][2], 3);
            }

            SUBCASE("Multiple streams to same chunks vector")
            {
                std::vector<std::vector<uint8_t>> chunks;

                {
                    chunked_memory_output_stream stream1(chunks);
                    uint8_t data1[] = {10, 20};
                    stream1.write(std::span<const uint8_t>(data1, 2));
                }

                {
                    chunked_memory_output_stream stream2(chunks);
                    uint8_t data2[] = {30, 40};
                    stream2.write(std::span<const uint8_t>(data2, 2));
                }

                CHECK_EQ(chunks.size(), 2);
                CHECK_EQ(chunks[0][0], 10);
                CHECK_EQ(chunks[0][1], 20);
                CHECK_EQ(chunks[1][0], 30);
                CHECK_EQ(chunks[1][1], 40);
            }
        }
    }
}
