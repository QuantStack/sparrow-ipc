#include <array>
#include <filesystem>
#include <fstream>
#include <limits>
#include <numeric>
#include <span>
#include <string>
#include <vector>

#include <sparrow_ipc/file_output_stream.hpp>

#include "doctest/doctest.h"

namespace sparrow_ipc
{
    // Helper class to manage temporary files for testing
    class temporary_file
    {
    private:

        std::filesystem::path m_path;

    public:

        temporary_file(const std::string& prefix = "test_file_output_stream")
        {
            m_path = std::filesystem::temp_directory_path()
                     / (prefix + "_" + std::to_string(std::rand()) + ".tmp");
        }

        ~temporary_file()
        {
            if (std::filesystem::exists(m_path))
            {
                std::filesystem::remove(m_path);
            }
        }

        const std::filesystem::path& path() const
        {
            return m_path;
        }

        std::string path_string() const
        {
            return m_path.string();
        }

        // Read the entire file content as bytes
        std::vector<uint8_t> read_content() const
        {
            std::ifstream file(m_path, std::ios::binary);
            if (!file)
            {
                return {};
            }

            file.seekg(0, std::ios::end);
            size_t size = file.tellg();
            file.seekg(0, std::ios::beg);

            std::vector<uint8_t> content(size);
            file.read(reinterpret_cast<char*>(content.data()), size);
            return content;
        }

        // Get file size
        size_t file_size() const
        {
            if (!std::filesystem::exists(m_path))
            {
                return 0;
            }
            return std::filesystem::file_size(m_path);
        }
    };

    TEST_SUITE("file_output_stream")
    {
        TEST_CASE("construction and basic state")
        {
            SUBCASE("Construction with valid file")
            {
                temporary_file temp_file;
                std::ofstream file(temp_file.path(), std::ios::binary);

                file_output_stream stream(file);

                CHECK(stream.is_open());
                CHECK_EQ(stream.size(), 0);
            }

            SUBCASE("Construction with closed file throws exception")
            {
                temporary_file temp_file;
                std::ofstream file;  // Not opened

                CHECK_THROWS_AS(file_output_stream{file}, std::runtime_error);
            }

            SUBCASE("Construction with file that fails to open throws exception")
            {
                std::ofstream file("/invalid/path/file.tmp");  // Invalid path

                CHECK_THROWS_AS(file_output_stream{file}, std::runtime_error);
            }
        }

        TEST_CASE("write operations")
        {
            SUBCASE("Write single byte span")
            {
                temporary_file temp_file;
                std::ofstream file(temp_file.path(), std::ios::binary);
                file_output_stream stream(file);

                uint8_t data[] = {42};
                std::span<const uint8_t> span(data, 1);

                auto written = stream.write(span);

                CHECK_EQ(written, 1);
                CHECK_EQ(stream.size(), 1);

                stream.flush();
                file.close();

                auto content = temp_file.read_content();
                REQUIRE_EQ(content.size(), 1);
                CHECK_EQ(content[0], 42);
            }

            SUBCASE("Write multiple bytes span")
            {
                temporary_file temp_file;
                std::ofstream file(temp_file.path(), std::ios::binary);
                file_output_stream stream(file);

                uint8_t data[] = {1, 2, 3, 4, 5};
                std::span<const uint8_t> span(data, 5);

                auto written = stream.write(span);

                CHECK_EQ(written, 5);
                CHECK_EQ(stream.size(), 5);

                stream.flush();
                file.close();

                auto content = temp_file.read_content();
                REQUIRE_EQ(content.size(), 5);
                for (size_t i = 0; i < 5; ++i)
                {
                    CHECK_EQ(content[i], i + 1);
                }
            }

            SUBCASE("Write empty span")
            {
                temporary_file temp_file;
                std::ofstream file(temp_file.path(), std::ios::binary);
                file_output_stream stream(file);

                std::span<const uint8_t> empty_span;

                auto written = stream.write(empty_span);

                CHECK_EQ(written, 0);
                CHECK_EQ(stream.size(), 0);

                stream.flush();
                file.close();

                auto content = temp_file.read_content();
                CHECK_EQ(content.size(), 0);
            }

            SUBCASE("Write single byte using write(uint8_t)")
            {
                temporary_file temp_file;
                std::ofstream file(temp_file.path(), std::ios::binary);
                file_output_stream stream(file);

                auto written = stream.write(static_cast<uint8_t>(123), 1);

                CHECK_EQ(written, 1);
                CHECK_EQ(stream.size(), 1);

                stream.flush();
                file.close();

                auto content = temp_file.read_content();
                REQUIRE_EQ(content.size(), 1);
                CHECK_EQ(content[0], 123);
            }

            SUBCASE("Write value multiple times")
            {
                temporary_file temp_file;
                std::ofstream file(temp_file.path(), std::ios::binary);
                file_output_stream stream(file);

                auto written = stream.write(static_cast<uint8_t>(255), 3);

                CHECK_EQ(written, 3);
                CHECK_EQ(stream.size(), 3);

                stream.flush();
                file.close();

                auto content = temp_file.read_content();
                REQUIRE_EQ(content.size(), 3);
                CHECK_EQ(content[0], 255);
                CHECK_EQ(content[1], 255);
                CHECK_EQ(content[2], 255);
            }

            SUBCASE("Write value zero times")
            {
                temporary_file temp_file;
                std::ofstream file(temp_file.path(), std::ios::binary);
                file_output_stream stream(file);

                auto written = stream.write(static_cast<uint8_t>(42), 0);

                CHECK_EQ(written, 0);
                CHECK_EQ(stream.size(), 0);

                stream.flush();
                file.close();

                auto content = temp_file.read_content();
                CHECK_EQ(content.size(), 0);
            }
        }

        TEST_CASE("sequential writes")
        {
            temporary_file temp_file;
            std::ofstream file(temp_file.path(), std::ios::binary);
            file_output_stream stream(file);

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

            stream.flush();
            file.close();

            // Verify final file content
            auto content = temp_file.read_content();
            std::vector<uint8_t> expected = {10, 20, 30, 40, 50, 60, 60};
            CHECK_EQ(content, expected);
        }

        TEST_CASE("reserve functionality")
        {
            temporary_file temp_file;
            std::ofstream file(temp_file.path(), std::ios::binary);
            file_output_stream stream(file);

            // Reserve space (should be no-op for file streams)
            CHECK_NOTHROW(stream.reserve(100));

            // Size should remain 0 after reserve
            CHECK_EQ(stream.size(), 0);

            // Writing should work normally after reserve
            uint8_t data[] = {1, 2, 3};
            std::span<const uint8_t> span(data, 3);
            stream.write(span);

            CHECK_EQ(stream.size(), 3);

            stream.flush();
            file.close();

            auto content = temp_file.read_content();
            CHECK_EQ(content.size(), 3);
        }

        TEST_CASE("add_padding functionality")
        {
            temporary_file temp_file;
            std::ofstream file(temp_file.path(), std::ios::binary);
            file_output_stream stream(file);

            SUBCASE("No padding needed when size is multiple of 8")
            {
                // Write 8 bytes
                uint8_t data[] = {1, 2, 3, 4, 5, 6, 7, 8};
                std::span<const uint8_t> span(data, 8);
                stream.write(span);

                auto size_before = stream.size();
                stream.add_padding();

                CHECK_EQ(stream.size(), size_before);
                CHECK_EQ(stream.size(), 8);

                stream.flush();
                file.close();

                auto content = temp_file.read_content();
                CHECK_EQ(content.size(), 8);
            }

            SUBCASE("Padding needed when size is not multiple of 8")
            {
                // Write 5 bytes
                uint8_t data[] = {1, 2, 3, 4, 5};
                std::span<const uint8_t> span(data, 5);
                stream.write(span);

                stream.add_padding();

                CHECK_EQ(stream.size(), 8);  // Should be padded to next multiple of 8

                stream.flush();
                file.close();

                auto content = temp_file.read_content();
                CHECK_EQ(content.size(), 8);

                // Check original data is preserved
                for (size_t i = 0; i < 5; ++i)
                {
                    CHECK_EQ(content[i], i + 1);
                }
                // Check padding bytes are zero
                CHECK_EQ(content[5], 0);
                CHECK_EQ(content[6], 0);
                CHECK_EQ(content[7], 0);
            }
        }

        TEST_CASE("stream lifecycle")
        {
            temporary_file temp_file;
            std::ofstream file(temp_file.path(), std::ios::binary);
            file_output_stream stream(file);

            SUBCASE("Stream is initially open")
            {
                CHECK(stream.is_open());
            }

            SUBCASE("Flush operation")
            {
                uint8_t data[] = {1, 2, 3};
                std::span<const uint8_t> span(data, 3);
                stream.write(span);

                CHECK_NOTHROW(stream.flush());
                CHECK(stream.is_open());
                CHECK_EQ(stream.size(), 3);

                // Verify data is written to file after flush
                CHECK_EQ(temp_file.file_size(), 3);
            }

            SUBCASE("Close operation")
            {
                uint8_t data[] = {1, 2, 3};
                std::span<const uint8_t> span(data, 3);
                stream.write(span);

                CHECK_NOTHROW(stream.close());
                CHECK_FALSE(stream.is_open());  // File stream should be closed
                CHECK_EQ(stream.size(), 3);     // Size should remain the same

                // Verify data is written to file after close
                CHECK_EQ(temp_file.file_size(), 3);
            }

            SUBCASE("Multiple close calls are safe")
            {
                stream.close();
                CHECK_FALSE(stream.is_open());

                CHECK_NOTHROW(stream.close());  // Second close should not throw
                CHECK_FALSE(stream.is_open());
            }
        }

        TEST_CASE("large data handling")
        {
            temporary_file temp_file;
            std::ofstream file(temp_file.path(), std::ios::binary);
            file_output_stream stream(file);

            // Write a large amount of data
            const size_t large_size = 10000;
            std::vector<uint8_t> large_data(large_size);
            std::iota(large_data.begin(), large_data.end(), 0);  // Fill with 0, 1, 2, ...

            std::span<const uint8_t> span(large_data);
            auto written = stream.write(span);

            CHECK_EQ(written, large_size);
            CHECK_EQ(stream.size(), large_size);

            stream.flush();
            file.close();

            auto content = temp_file.read_content();
            CHECK_EQ(content.size(), large_size);

            // Verify data integrity
            for (size_t i = 0; i < large_size; ++i)
            {
                CHECK_EQ(content[i], static_cast<uint8_t>(i));
            }
        }

        TEST_CASE("edge cases")
        {
            SUBCASE("Maximum value repeated writes")
            {
                temporary_file temp_file;
                std::ofstream file(temp_file.path(), std::ios::binary);
                file_output_stream stream(file);

                auto written = stream.write(std::numeric_limits<uint8_t>::max(), 255);

                CHECK_EQ(written, 255);
                CHECK_EQ(stream.size(), 255);

                stream.flush();
                file.close();

                auto content = temp_file.read_content();
                REQUIRE_EQ(content.size(), 255);
                for (size_t i = 0; i < 255; ++i)
                {
                    CHECK_EQ(content[i], std::numeric_limits<uint8_t>::max());
                }
            }

            SUBCASE("Zero byte repeated writes")
            {
                temporary_file temp_file;
                std::ofstream file(temp_file.path(), std::ios::binary);
                file_output_stream stream(file);

                auto written = stream.write(static_cast<uint8_t>(0), 100);

                CHECK_EQ(written, 100);
                CHECK_EQ(stream.size(), 100);

                stream.flush();
                file.close();

                auto content = temp_file.read_content();
                REQUIRE_EQ(content.size(), 100);
                for (size_t i = 0; i < 100; ++i)
                {
                    CHECK_EQ(content[i], 0);
                }
            }
        }

        TEST_CASE("different write patterns")
        {
            SUBCASE("Alternating single and bulk writes")
            {
                temporary_file temp_file;
                std::ofstream file(temp_file.path(), std::ios::binary);
                file_output_stream stream(file);

                // Single byte write
                stream.write(static_cast<uint8_t>(100));

                // Bulk write
                uint8_t data[] = {200, 201, 202};
                std::span<const uint8_t> span(data, 3);
                stream.write(span);

                // Repeated value write
                stream.write(static_cast<uint8_t>(150), 2);

                // Single byte write again
                stream.write(static_cast<uint8_t>(250));

                CHECK_EQ(stream.size(), 7);

                stream.flush();
                file.close();

                auto content = temp_file.read_content();
                std::vector<uint8_t> expected = {100, 200, 201, 202, 150, 150, 250};
                CHECK_EQ(content, expected);
            }

            SUBCASE("Binary data with null bytes")
            {
                temporary_file temp_file;
                std::ofstream file(temp_file.path(), std::ios::binary);
                file_output_stream stream(file);

                uint8_t data[] = {0x00, 0xFF, 0x00, 0xAA, 0x00, 0x55};
                std::span<const uint8_t> span(data, 6);
                stream.write(span);

                CHECK_EQ(stream.size(), 6);

                stream.flush();
                file.close();

                auto content = temp_file.read_content();
                REQUIRE_EQ(content.size(), 6);
                CHECK_EQ(content[0], 0x00);
                CHECK_EQ(content[1], 0xFF);
                CHECK_EQ(content[2], 0x00);
                CHECK_EQ(content[3], 0xAA);
                CHECK_EQ(content[4], 0x00);
                CHECK_EQ(content[5], 0x55);
            }
        }

        TEST_CASE("error handling")
        {
            SUBCASE("Writing to closed stream")
            {
                temporary_file temp_file;
                std::ofstream file(temp_file.path(), std::ios::binary);
                file_output_stream stream(file);

                // Close the underlying file
                file.close();

                // Stream should report as closed
                CHECK_FALSE(stream.is_open());

                // Writing should still work but may not persist (depends on implementation)
                uint8_t data[] = {1, 2, 3};
                std::span<const uint8_t> span(data, 3);
                auto written = stream.write(span);

                // The write operation itself shouldn't throw, but data may not be written
                CHECK_EQ(written, 3);
                CHECK_EQ(stream.size(), 3);
            }
        }

        TEST_CASE("size tracking accuracy")
        {
            temporary_file temp_file;
            std::ofstream file(temp_file.path(), std::ios::binary);
            file_output_stream stream(file);

            SUBCASE("Size tracking with various write operations")
            {
                CHECK_EQ(stream.size(), 0);

                // Write span
                uint8_t data1[] = {1, 2};
                stream.write(std::span<const uint8_t>(data1, 2));
                CHECK_EQ(stream.size(), 2);

                // Write single byte
                stream.write(static_cast<uint8_t>(3));
                CHECK_EQ(stream.size(), 3);

                // Write repeated value
                stream.write(static_cast<uint8_t>(4), 3);
                CHECK_EQ(stream.size(), 6);

                // Write empty span (should not change size)
                std::span<const uint8_t> empty;
                stream.write(empty);
                CHECK_EQ(stream.size(), 6);

                // Write zero count (should not change size)
                stream.write(static_cast<uint8_t>(5), 0);
                CHECK_EQ(stream.size(), 6);

                stream.flush();
                file.close();

                // Verify file size matches stream size
                CHECK_EQ(temp_file.file_size(), 6);
            }
        }
    }
}
