#include <string>
#include <vector>

#include <doctest/doctest.h>

#include <sparrow_ipc/compression.hpp>

namespace sparrow_ipc
{
    TEST_CASE("Compression and Decompression Round-trip")
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
