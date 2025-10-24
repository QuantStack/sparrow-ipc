#include "sparrow_ipc/flatbuffer_utils.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/serialize_utils.hpp"


namespace sparrow_ipc
{
    void fill_body(const sparrow::arrow_proxy& arrow_proxy, any_output_stream& stream, std::optional<CompressionType> compression)
    {
        std::for_each(arrow_proxy.buffers().begin(), arrow_proxy.buffers().end(), [&](const auto& buffer) {
            if (compression.has_value())
            {
                auto compressed_buffer_with_header = compress(compression.value(), std::span<const uint8_t>(buffer.data(), buffer.size()));
                stream.write(std::span(compressed_buffer_with_header.data(), compressed_buffer_with_header.size()));
            }
            else
            {
                stream.write(buffer);
            }
            stream.add_padding();
        });

        std::for_each(arrow_proxy.children().begin(), arrow_proxy.children().end(), [&](const auto& child) {
            fill_body(child, stream, compression);
        });
    }

    void generate_body(const sparrow::record_batch& record_batch, any_output_stream& stream, std::optional<CompressionType> compression)
    {
        std::for_each(record_batch.columns().begin(), record_batch.columns().end(), [&](const auto& column) {
            const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(column);
            fill_body(arrow_proxy, stream, compression);
        });
    }

    int64_t calculate_body_size(const sparrow::arrow_proxy& arrow_proxy, std::optional<CompressionType> compression)
    {
        int64_t total_size = 0;
        if (compression.has_value())
        {
            for (const auto& buffer : arrow_proxy.buffers())
            {
                total_size += utils::align_to_8(compress(compression.value(), std::span<const uint8_t>(buffer.data(), buffer.size())).size());
            }
        }
        else
        {
            for (const auto& buffer : arrow_proxy.buffers())
            {
                total_size += utils::align_to_8(buffer.size());
            }
        }

        for (const auto& child : arrow_proxy.children())
        {
            total_size += calculate_body_size(child, compression);
        }
        return total_size;
    }

    int64_t calculate_body_size(const sparrow::record_batch& record_batch, std::optional<CompressionType> compression)
    {
        return std::accumulate(
            record_batch.columns().begin(),
            record_batch.columns().end(),
            int64_t{0},
            [&](int64_t acc, const sparrow::array& arr)
            {
                const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(arr);
                return acc + calculate_body_size(arrow_proxy, compression);
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

    std::size_t calculate_record_batch_message_size(const sparrow::record_batch& record_batch, std::optional<CompressionType> compression)
    {
        // Build the record batch message to get its exact metadata size
        flatbuffers::FlatBufferBuilder record_batch_builder = get_record_batch_message_builder(record_batch, compression);
        const flatbuffers::uoffset_t record_batch_len = record_batch_builder.GetSize();

        const std::size_t actual_body_size = static_cast<std::size_t>(calculate_body_size(record_batch, compression));

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

    std::vector<org::apache::arrow::flatbuf::Buffer>
    generate_compressed_buffers(const sparrow::record_batch& record_batch, const CompressionType compression_type)
    {
        std::vector<org::apache::arrow::flatbuf::Buffer> compressed_buffers;
        int64_t current_offset = 0;

        for (const auto& column : record_batch.columns())
        {
            const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(column);
            for (const auto& buffer : arrow_proxy.buffers())
            {
                std::vector<uint8_t> compressed_buffer_with_header = compress(compression_type, std::span<const uint8_t>(buffer.data(), buffer.size()));
                const size_t aligned_chunk_size = utils::align_to_8(compressed_buffer_with_header.size());
                compressed_buffers.emplace_back(current_offset, aligned_chunk_size);
                current_offset += aligned_chunk_size;
            }
        }
        return compressed_buffers;
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
