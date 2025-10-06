#include <lz4frame.h>

#include "sparrow_ipc/flatbuffer_utils.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/utils.hpp"

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

    std::size_t calculate_record_batch_message_size(const sparrow::record_batch& record_batch)
    {
        // Build the record batch message to get its exact metadata size
        flatbuffers::FlatBufferBuilder record_batch_builder = get_record_batch_message_builder(record_batch);
        const flatbuffers::uoffset_t record_batch_len = record_batch_builder.GetSize();

        // Calculate body size (already includes 8-byte alignment for each buffer)
        const int64_t body_size = calculate_body_size(record_batch);

        // Calculate total size:
        // - Continuation bytes (4)
        // - Message length prefix (4)
        // - FlatBuffer record batch metadata
        // - Padding after metadata to 8-byte alignment
        // - Body data (already aligned)
        std::size_t metadata_size = continuation.size() + sizeof(uint32_t) + record_batch_len;
        metadata_size = utils::align_to_8(metadata_size);

        return metadata_size + static_cast<std::size_t>(body_size);
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
