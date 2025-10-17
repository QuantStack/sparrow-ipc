#include <stdexcept>
#include <string>
#include <vector>

#include <doctest/doctest.h>

#include <sparrow_ipc/compression.hpp>

namespace sparrow_ipc
{
    TEST_SUITE("De/Compression")
    {
        TEST_CASE("Unsupported ZSTD de/compression")
        {
            std::string original_string = "some data to compress";
            std::vector<uint8_t> original_data(original_string.begin(), original_string.end());
            const auto compression_type = org::apache::arrow::flatbuf::CompressionType::ZSTD;

            // Test compression with ZSTD
            CHECK_THROWS_WITH_AS(compress(compression_type, original_data), "Compression using zstd is not supported yet.", std::runtime_error);

            // Test decompression with ZSTD
            CHECK_THROWS_WITH_AS(decompress(compression_type, original_data), "Decompression using zstd is not supported yet.", std::runtime_error);
        }

        TEST_CASE("Empty data")
        {
            const std::vector<uint8_t> empty_data;
            const auto compression_type = org::apache::arrow::flatbuf::CompressionType::LZ4_FRAME;

            // Test compression of empty data
            auto compressed = compress(compression_type, empty_data);
            CHECK_EQ(compressed.size(), CompressionHeaderSize);
            const std::int64_t header = *reinterpret_cast<const std::int64_t*>(compressed.data());
            CHECK_EQ(header, -1);

            // Test decompression of empty data
            auto decompressed = decompress(compression_type, compressed);
            std::visit([](const auto& value) { CHECK(value.empty()); }, decompressed);
        }

        TEST_CASE("Data compression and decompression round-trip")
        {
            std::string original_string = "Hello world, this is a test of compression and decompression. But we need more words to make this compression worth it!";
            std::vector<uint8_t> original_data(original_string.begin(), original_string.end());

            // Compress data
            auto compression_type = org::apache::arrow::flatbuf::CompressionType::LZ4_FRAME;
            std::vector<uint8_t> compressed_data = compress(compression_type, original_data);

            // Decompress
            auto decompressed_result = decompress(compression_type, compressed_data);
            std::visit(
                [&original_data](const auto& decompressed_data)
                {
                    CHECK_EQ(decompressed_data.size(), original_data.size());
                    const std::vector<uint8_t> vec(decompressed_data.begin(), decompressed_data.end());
                    CHECK_EQ(vec, original_data);
                },
                decompressed_result
            );
        }

        TEST_CASE("Data compression with incompressible data")
        {
            std::string original_string = "abc";
            std::vector<uint8_t> original_data(original_string.begin(), original_string.end());

            // Compress data
            auto compression_type = org::apache::arrow::flatbuf::CompressionType::LZ4_FRAME;
            std::vector<uint8_t> compressed_data = compress(compression_type, original_data);

            // Decompress
            auto decompressed_result = decompress(compression_type, compressed_data);
            std::visit(
                [&original_data](const auto& decompressed_data)
                {
                    CHECK_EQ(decompressed_data.size(), original_data.size());
                    const std::vector<uint8_t> vec(decompressed_data.begin(), decompressed_data.end());
                    CHECK_EQ(vec, original_data);
                },
                decompressed_result
            );

            // Check that the compressed data is just the original data with a -1 header
            const std::int64_t header = *reinterpret_cast<const std::int64_t*>(compressed_data.data());
            CHECK_EQ(header, -1);
            std::vector<uint8_t> body(compressed_data.begin() + sizeof(header), compressed_data.end());
            CHECK_EQ(body, original_data);
        }
    }
}
