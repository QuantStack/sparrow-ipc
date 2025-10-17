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
            CHECK_THROWS_WITH_AS(sparrow_ipc::compress(compression_type, original_data), "Compression using zstd is not supported yet.", std::runtime_error);

            // Test decompression with ZSTD
            CHECK_THROWS_WITH_AS(sparrow_ipc::decompress(compression_type, original_data), "Decompression using zstd is not supported yet.", std::runtime_error);
        }

        TEST_CASE("Empty data")
        {
            const std::vector<uint8_t> empty_data;
            const auto compression_type = org::apache::arrow::flatbuf::CompressionType::LZ4_FRAME;

            // Test compression of empty data
            auto compressed = compress(compression_type, empty_data);
            CHECK(compressed.empty());

            // Test decompression of empty data
            auto decompressed = decompress(compression_type, empty_data);
            CHECK(decompressed.empty());
        }

        TEST_CASE("Data compression and decompression round-trip")
        {
            std::string original_string = "hello world, this is a test of compression and decompression!!";
            std::vector<uint8_t> original_data(original_string.begin(), original_string.end());
            const int64_t original_size = original_data.size();

            // Compress data
            auto compression_type = org::apache::arrow::flatbuf::CompressionType::LZ4_FRAME;
            std::vector<uint8_t> compressed_frame = compress(compression_type, original_data);

            CHECK_GT(compressed_frame.size(), 0);
            CHECK_NE(compressed_frame, original_data);

            // Manually create the IPC-formatted compressed buffer by adding the 8-byte prefix
            std::vector<uint8_t> ipc_formatted_buffer;
            ipc_formatted_buffer.reserve(sizeof(int64_t) + compressed_frame.size());
            ipc_formatted_buffer.insert(ipc_formatted_buffer.end(), reinterpret_cast<const uint8_t*>(&original_size), reinterpret_cast<const uint8_t*>(&original_size) + sizeof(int64_t));
            ipc_formatted_buffer.insert(ipc_formatted_buffer.end(), compressed_frame.begin(), compressed_frame.end());

            // Decompress
            std::vector<uint8_t> decompressed_data = decompress(compression_type, ipc_formatted_buffer);

            CHECK_EQ(decompressed_data.size(), original_data.size());
            CHECK_EQ(decompressed_data, original_data);
        }
    }
}
