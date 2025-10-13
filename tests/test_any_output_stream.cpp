#include <algorithm>
#include <fstream>
#include <sstream>
#include <vector>

#include "doctest/doctest.h"

#include "sparrow_ipc/any_output_stream.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"

TEST_SUITE("any_output_stream")
{
    TEST_CASE("Construction and basic write")
    {
        SUBCASE("With memory_output_stream")
        {
            std::vector<uint8_t> buffer;
            sparrow_ipc::memory_output_stream<std::vector<uint8_t>> mem_stream(buffer);
            sparrow_ipc::any_output_stream stream(mem_stream);

            const std::vector<uint8_t> data = {1, 2, 3, 4, 5};
            stream.write(std::span<const uint8_t>(data));

            CHECK_EQ(buffer.size(), 5);
            CHECK_EQ(buffer, data);
        }

        SUBCASE("With custom stream (vector wrapper)")
        {
            // Custom stream that just wraps a vector
            struct custom_stream
            {
                std::vector<uint8_t>& buffer;

                custom_stream& write(const char* s, std::streamsize count)
                {
                    buffer.insert(buffer.end(), s, s + count);
                    return *this;
                }

                custom_stream& write(std::span<const uint8_t> data)
                {
                    buffer.insert(buffer.end(), data.begin(), data.end());
                    return *this;
                }

                custom_stream& put(uint8_t value)
                {
                    buffer.push_back(value);
                    return *this;
                }
                
                std::size_t size() const { return buffer.size(); }
            };
            
            std::vector<uint8_t> buffer;
            custom_stream custom{buffer};
            sparrow_ipc::any_output_stream stream(custom);

            const std::vector<uint8_t> data = {10, 20, 30};
            stream.write(std::span<const uint8_t>(data));

            CHECK_EQ(buffer.size(), 3);
            CHECK_EQ(buffer[0], 10);
            CHECK_EQ(buffer[1], 20);
            CHECK_EQ(buffer[2], 30);
        }
    }

    TEST_CASE("Write single byte")
    {
        std::vector<uint8_t> buffer;
        sparrow_ipc::memory_output_stream mem_stream(buffer);
        sparrow_ipc::any_output_stream stream(mem_stream);

        stream.write(uint8_t{42});

        CHECK_EQ(buffer.size(), 1);
        CHECK_EQ(buffer[0], 42);
    }

    TEST_CASE("Write repeated bytes")
    {
        std::vector<uint8_t> buffer;
        sparrow_ipc::memory_output_stream mem_stream(buffer);
        sparrow_ipc::any_output_stream stream(mem_stream);

        stream.write(uint8_t{0}, 5);

        CHECK_EQ(buffer.size(), 5);
        CHECK(std::all_of(buffer.begin(), buffer.end(), [](uint8_t b) { return b == 0; }));
    }

    TEST_CASE("Add padding")
    {
        std::vector<uint8_t> buffer;
        sparrow_ipc::memory_output_stream mem_stream(buffer);
        sparrow_ipc::any_output_stream stream(mem_stream);

        // Write 5 bytes
        stream.write(std::vector<uint8_t>{1, 2, 3, 4, 5});

        // Add padding to align to 8-byte boundary
        stream.add_padding();

        // Should pad to 8 bytes (5 data + 3 padding)
        CHECK_EQ(buffer.size(), 8);
        CHECK_EQ(buffer[5], 0);
        CHECK_EQ(buffer[6], 0);
        CHECK_EQ(buffer[7], 0);
    }

    TEST_CASE("Reserve")
    {
        SUBCASE("Direct reserve")
        {
            std::vector<uint8_t> buffer;
            sparrow_ipc::memory_output_stream mem_stream(buffer);
            sparrow_ipc::any_output_stream stream(mem_stream);

            stream.reserve(100);

            CHECK_GE(buffer.capacity(), 100);
        }

        SUBCASE("Lazy reserve with function")
        {
            std::vector<uint8_t> buffer;
            sparrow_ipc::memory_output_stream mem_stream(buffer);
            sparrow_ipc::any_output_stream stream(mem_stream);

            stream.reserve([]() { return 200; });

            CHECK_GE(buffer.capacity(), 200);
        }
    }

    TEST_CASE("Size tracking")
    {
        std::vector<uint8_t> buffer;
        sparrow_ipc::memory_output_stream mem_stream(buffer);
        sparrow_ipc::any_output_stream stream(mem_stream);

        CHECK_EQ(stream.size(), 0);

        stream.write(std::vector<uint8_t>{1, 2, 3});
        CHECK_EQ(stream.size(), 3);

        stream.write(uint8_t{4});
        CHECK_EQ(stream.size(), 4);
    }

    TEST_CASE("Type recovery with get()")
    {
        std::vector<uint8_t> buffer;
        sparrow_ipc::memory_output_stream mem_stream(buffer);
        sparrow_ipc::any_output_stream stream(mem_stream);

        SUBCASE("Correct type")
        {
            auto& recovered = stream.get<sparrow_ipc::memory_output_stream<std::vector<uint8_t>>>();
            recovered.write(std::span<const uint8_t>{std::vector<uint8_t>{1, 2, 3}});
            CHECK_EQ(buffer.size(), 3);
        }

        SUBCASE("Wrong type throws")
        {
            CHECK_THROWS_AS(
                stream.get<std::ostringstream>(),
                std::bad_cast
            );
        }
    }

    TEST_CASE("Move semantics")
    {
        std::vector<uint8_t> buffer;
        sparrow_ipc::memory_output_stream mem_stream(buffer);
        sparrow_ipc::any_output_stream stream1(mem_stream);

        stream1.write(std::vector<uint8_t>{1, 2, 3});

        // Move construction
        sparrow_ipc::any_output_stream stream2(std::move(stream1));
        CHECK_EQ(stream2.size(), 3);

        // Move assignment
        sparrow_ipc::any_output_stream stream3(mem_stream);
        stream3 = std::move(stream2);
        CHECK_EQ(stream3.size(), 3);
    }

    TEST_CASE("Polymorphic usage")
    {
        auto write_data = [](sparrow_ipc::any_output_stream& stream)
        {
            const std::vector<uint8_t> data = {10, 20, 30, 40, 50};
            stream.write(std::span<const uint8_t>(data));
            stream.add_padding();  // Pad to 8 bytes
        };

        SUBCASE("With memory stream")
        {
            std::vector<uint8_t> buffer;
            sparrow_ipc::memory_output_stream mem_stream(buffer);
            sparrow_ipc::any_output_stream stream(mem_stream);

            write_data(stream);

            CHECK_EQ(buffer.size(), 8);  // 5 data + 3 padding
        }

        SUBCASE("With ostringstream")
        {
            std::ostringstream oss;
            sparrow_ipc::any_output_stream stream(oss);

            write_data(stream);

            CHECK_GE(oss.str().size(), 5);
        }
    }

    TEST_CASE("Edge cases")
    {
        SUBCASE("Empty write")
        {
            std::vector<uint8_t> buffer;
            sparrow_ipc::memory_output_stream mem_stream(buffer);
            sparrow_ipc::any_output_stream stream(mem_stream);

            std::vector<uint8_t> empty;
            stream.write(std::span<const uint8_t>(empty));

            CHECK_EQ(buffer.size(), 0);
        }

        SUBCASE("Already aligned padding")
        {
            std::vector<uint8_t> buffer;
            sparrow_ipc::memory_output_stream mem_stream(buffer);
            sparrow_ipc::any_output_stream stream(mem_stream);

            // Write exactly 8 bytes
            stream.write(std::vector<uint8_t>{1, 2, 3, 4, 5, 6, 7, 8});
            stream.add_padding();

            // Should not add any padding
            CHECK_EQ(buffer.size(), 8);
        }

        SUBCASE("Zero byte write repeated")
        {
            std::vector<uint8_t> buffer;
            sparrow_ipc::memory_output_stream mem_stream(buffer);
            sparrow_ipc::any_output_stream stream(mem_stream);

            stream.write(uint8_t{0}, 0);

            CHECK_EQ(buffer.size(), 0);
        }
    }
}
