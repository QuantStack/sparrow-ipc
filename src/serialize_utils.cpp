#include "sparrow_ipc/compression.hpp"
#include "sparrow_ipc/flatbuffer_utils.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/serialize_utils.hpp"


namespace sparrow_ipc
{
    void fill_body(const sparrow::arrow_proxy& arrow_proxy, any_output_stream& stream)
    {
        for (const auto& buffer : arrow_proxy.buffers())
        {
            stream.write(buffer);
            stream.add_padding();
        }
        for (const auto& child : arrow_proxy.children())
        {
            fill_body(child, stream);
        }
    }

    void generate_body(const sparrow::record_batch& record_batch, any_output_stream& stream)
    {
        for (const auto& column : record_batch.columns())
        {
            const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(column);
            fill_body(arrow_proxy, stream);
        }
    }

    int64_t calculate_body_size(const sparrow::arrow_proxy& arrow_proxy)
    {
        int64_t total_size = 0;
        for (const auto& buffer : arrow_proxy.buffers())
        {
            total_size += utils::align_to_8(buffer.size());
        }
        for (const auto& child : arrow_proxy.children())
        {
            total_size += calculate_body_size(child);
        }
        return total_size;
    }

    int64_t calculate_body_size(const sparrow::record_batch& record_batch)
    {
        return std::accumulate(
            record_batch.columns().begin(),
            record_batch.columns().end(),
            int64_t{0},
            [](int64_t acc, const sparrow::array& arr)
            {
                const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(arr);
                return acc + calculate_body_size(arrow_proxy);
            }
        );
    }

    std::size_t calculate_schema_message_size(const sparrow::record_batch& record_batch)
    {
        // Build the schema message to get its exact size
        flatbuffers::FlatBufferBuilder schema_builder = get_schema_message_builder(record_batch);
        const flatbuffers::uoffset_t schema_len = schema_builder.GetSize();

        // Calculate total size:
        // - Continuation bytes (4)
        // - Message length prefix (4)
        // - FlatBuffer schema message data
        // - Padding to 8-byte alignment
        std::size_t total_size = continuation.size() + sizeof(uint32_t) + schema_len;
        return utils::align_to_8(total_size);
    }

    std::size_t calculate_record_batch_message_size(const sparrow::record_batch& record_batch, std::optional<org::apache::arrow::flatbuf::CompressionType> compression)
    {
        // Build the record batch message to get its exact metadata size
        flatbuffers::FlatBufferBuilder record_batch_builder = get_record_batch_message_builder(record_batch, compression);
        const flatbuffers::uoffset_t record_batch_len = record_batch_builder.GetSize();

        std::size_t actual_body_size = 0;
        if (compression.has_value())
        {
            // If compressed, the body size is the sum of compressed buffer sizes + original size prefixes + padding
            auto [compressed_body, compressed_buffers] = generate_compressed_body_and_buffers(record_batch, compression.value());
            actual_body_size = compressed_body.size();
        }
        else
        {
            // If not compressed, the body size is the sum of uncompressed buffer sizes with padding
            actual_body_size = static_cast<std::size_t>(calculate_body_size(record_batch));
        }

        // Calculate total size:
        // - Continuation bytes (4)
        // - Message length prefix (4)
        // - FlatBuffer record batch metadata
        // - Padding after metadata to 8-byte alignment
        // - Body data (already aligned)
        std::size_t metadata_size = continuation.size() + sizeof(uint32_t) + record_batch_len;
        metadata_size = utils::align_to_8(metadata_size);

        return metadata_size + actual_body_size;
    }

    [[nodiscard]] SPARROW_IPC_API std::pair<std::vector<uint8_t>, std::vector<org::apache::arrow::flatbuf::Buffer>>
    generate_compressed_body_and_buffers(const sparrow::record_batch& record_batch, const org::apache::arrow::flatbuf::CompressionType compression_type)
    {
        std::vector<uint8_t> compressed_body;
        std::vector<org::apache::arrow::flatbuf::Buffer> compressed_buffers;
        int64_t current_offset = 0;

        for (const auto& column : record_batch.columns())
        {
            const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(column);
            for (const auto& buffer : arrow_proxy.buffers())
            {
                // Original buffer size
                const int64_t original_size = static_cast<int64_t>(buffer.size());

                // Compress the buffer
                std::vector<uint8_t> compressed_buffer_data = compress(compression_type, std::span<const uint8_t>(buffer.data(), original_size));
                const int64_t compressed_data_size = static_cast<int64_t>(compressed_buffer_data.size());

                // Calculate total size of this compressed chunk (original size prefix + compressed data + padding)
                const int64_t total_chunk_size = sizeof(int64_t) + compressed_data_size;
                const size_t aligned_chunk_size = utils::align_to_8(total_chunk_size);
                const size_t padding_needed = aligned_chunk_size - total_chunk_size;

                // Write original size (8 bytes) followed by compressed data
                compressed_body.insert(compressed_body.end(), reinterpret_cast<const uint8_t*>(&original_size), reinterpret_cast<const uint8_t*>(&original_size) + sizeof(int64_t));
                compressed_body.insert(compressed_body.end(), compressed_buffer_data.begin(), compressed_buffer_data.end());

                // Add padding to the compressed data
                compressed_body.insert(compressed_body.end(), padding_needed, 0);

                // Update compressed buffer metadata
                compressed_buffers.emplace_back(current_offset, aligned_chunk_size);
                current_offset += aligned_chunk_size;
            }
        }
        return {compressed_body, compressed_buffers};
    }

    std::vector<sparrow::data_type> get_column_dtypes(const sparrow::record_batch& rb)
    {
        std::vector<sparrow::data_type> dtypes;
        dtypes.reserve(rb.nb_columns());
        std::ranges::transform(
            rb.columns(),
            std::back_inserter(dtypes),
            [](const auto& col)
            {
                return col.data_type();
            }
        );
        return dtypes;
    }
}
