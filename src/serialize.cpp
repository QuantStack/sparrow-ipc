#include "sparrow_ipc/serialize.hpp"

#include <iterator>

#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>>
    create_metadata(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema)
    {
        if (arrow_schema.metadata == nullptr)
        {
            return 0;
        }

        const auto metadata_view = sparrow::key_value_view(arrow_schema.metadata);
        std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>> kv_offsets;
        kv_offsets.reserve(metadata_view.size());
        for (const auto& [key, value] : metadata_view)
        {
            const auto key_offset = builder.CreateString(std::string(key));
            const auto value_offset = builder.CreateString(std::string(value));
            kv_offsets.push_back(org::apache::arrow::flatbuf::CreateKeyValue(builder, key_offset, value_offset));
        }
        return builder.CreateVector(kv_offsets);
    }

    ::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>
    create_field(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema)
    {
        flatbuffers::Offset<flatbuffers::String> fb_name_offset = (arrow_schema.name == nullptr)
                                                                      ? 0
                                                                      : builder.CreateString(arrow_schema.name);
        const auto [type_enum, type_offset] = utils::get_flatbuffer_type(builder, arrow_schema.format);
        auto fb_metadata_offset = create_metadata(builder, arrow_schema);
        const auto children = create_children(builder, arrow_schema);
        const auto fb_field = org::apache::arrow::flatbuf::CreateField(
            builder,
            fb_name_offset,
            (arrow_schema.flags & static_cast<int64_t>(sparrow::ArrowFlag::NULLABLE)) != 0,
            type_enum,
            type_offset,
            0,  // TODO: support dictionary
            children,
            fb_metadata_offset
        );
        return fb_field;
    }

    ::flatbuffers::Offset<::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>>>
    create_children(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema)
    {
        std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::Field>> children_vec;
        children_vec.reserve(arrow_schema.n_children);
        for (size_t i = 0; i < arrow_schema.n_children; ++i)
        {
            if (arrow_schema.children[i] == nullptr)
            {
                throw std::invalid_argument("ArrowSchema has null child pointer");
            }
            const auto& child = *arrow_schema.children[i];
            flatbuffers::Offset<org::apache::arrow::flatbuf::Field> field = create_field(builder, child);
            children_vec.emplace_back(field);
        }
        return children_vec.empty() ? 0 : builder.CreateVector(children_vec);
    }

    ::flatbuffers::Offset<::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>>>
    create_children(flatbuffers::FlatBufferBuilder& builder, sparrow::record_batch::column_range columns)
    {
        std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::Field>> children_vec;
        children_vec.reserve(columns.size());
        for (const auto& column : columns)
        {
            const auto& arrow_schema = sparrow::detail::array_access::get_arrow_proxy(column).schema();
            flatbuffers::Offset<org::apache::arrow::flatbuf::Field> field = create_field(builder, arrow_schema);
            children_vec.emplace_back(field);
        }
        return children_vec.empty() ? 0 : builder.CreateVector(children_vec);
    }

    flatbuffers::FlatBufferBuilder get_schema_message_builder(const sparrow::record_batch& record_batch)
    {
        flatbuffers::FlatBufferBuilder schema_builder;
        record_batch.columns();
        const auto fields_vec = create_children(schema_builder, record_batch.columns());
        const auto schema_offset = org::apache::arrow::flatbuf::CreateSchema(
            schema_builder,
            org::apache::arrow::flatbuf::Endianness::Little,  // TODO: make configurable
            fields_vec
        );
        const auto schema_message_offset = org::apache::arrow::flatbuf::CreateMessage(
            schema_builder,
            org::apache::arrow::flatbuf::MetadataVersion::V5,
            org::apache::arrow::flatbuf::MessageHeader::Schema,
            schema_offset.Union(),
            0,  // body length IS 0 for schema messages
            0   // custom metadata
        );
        schema_builder.Finish(schema_message_offset);
        return schema_builder;
    }

    std::vector<uint8_t> serialize_schema_message(const sparrow::record_batch& record_batch)
    {
        std::vector<uint8_t> schema_buffer;
        schema_buffer.insert(schema_buffer.end(), continuation.begin(), continuation.end());
        flatbuffers::FlatBufferBuilder schema_builder = get_schema_message_builder(record_batch);
        const flatbuffers::uoffset_t schema_len = schema_builder.GetSize();
        schema_buffer.reserve(schema_buffer.size() + sizeof(uint32_t) + schema_len);
        // Write the 4-byte length prefix after the continuation bytes
        schema_buffer.insert(
            schema_buffer.end(),
            reinterpret_cast<const uint8_t*>(&schema_len),
            reinterpret_cast<const uint8_t*>(&schema_len) + sizeof(uint32_t)
        );
        // Append the actual message bytes
        schema_buffer.insert(
            schema_buffer.end(),
            schema_builder.GetBufferPointer(),
            schema_builder.GetBufferPointer() + schema_len
        );
        // padding to 8 bytes
        schema_buffer.insert(
            schema_buffer.end(),
            utils::align_to_8(static_cast<int64_t>(schema_buffer.size()))
                - static_cast<int64_t>(schema_buffer.size()),
            0
        );
        return schema_buffer;
    }

    void fill_fieldnodes(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::FieldNode>& nodes
    )
    {
        nodes.emplace_back(arrow_proxy.length(), arrow_proxy.null_count());
        nodes.reserve(nodes.size() + arrow_proxy.n_children());
        for (const auto& child : arrow_proxy.children())
        {
            fill_fieldnodes(child, nodes);
        }
    }

    std::vector<org::apache::arrow::flatbuf::FieldNode>
    create_fieldnodes(const sparrow::record_batch& record_batch)
    {
        std::vector<org::apache::arrow::flatbuf::FieldNode> nodes;
        nodes.reserve(record_batch.columns().size());
        for (const auto& column : record_batch.columns())
        {
            fill_fieldnodes(sparrow::detail::array_access::get_arrow_proxy(column), nodes);
        }
        return nodes;
    }

    void fill_buffers(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::Buffer>& flatbuf_buffers,
        int64_t& offset
    )
    {
        const auto& buffers = arrow_proxy.buffers();
        for (const auto& buffer : buffers)
        {
            int64_t size = static_cast<int64_t>(buffer.size());
            flatbuf_buffers.emplace_back(offset, size);
            offset += utils::align_to_8(size);
        }
        for (const auto& child : arrow_proxy.children())
        {
            fill_buffers(child, flatbuf_buffers, offset);
        }
    }

    std::vector<org::apache::arrow::flatbuf::Buffer> get_buffers(const sparrow::record_batch& record_batch)
    {
        std::vector<org::apache::arrow::flatbuf::Buffer> buffers;
        std::int64_t offset = 0;
        for (const auto& column : record_batch.columns())
        {
            const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(column);
            fill_buffers(arrow_proxy, buffers, offset);
        }
        return buffers;
    }

    void fill_body(const sparrow::arrow_proxy& arrow_proxy, std::vector<uint8_t>& body)
    {
        for (const auto& buffer : arrow_proxy.buffers())
        {
            body.insert(body.end(), buffer.begin(), buffer.end());
            const int64_t padding_size = utils::align_to_8(static_cast<int64_t>(buffer.size()))
                                         - static_cast<int64_t>(buffer.size());
            body.insert(body.end(), padding_size, 0);
        }
        for (const auto& child : arrow_proxy.children())
        {
            fill_body(child, body);
        }
    }

    std::vector<uint8_t> generate_body(const sparrow::record_batch& record_batch)
    {
        std::vector<uint8_t> body;
        for (const auto& column : record_batch.columns())
        {
            const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(column);
            fill_body(arrow_proxy, body);
        }
        return body;
    }

    int64_t calculate_body_size(const sparrow::arrow_proxy& arrow_proxy)
    {
        int64_t total_size = 0;
        for (const auto& buffer : arrow_proxy.buffers())
        {
            total_size += utils::align_to_8(static_cast<int64_t>(buffer.size()));
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
            0,
            [](int64_t acc, const sparrow::array& arr)
            {
                const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(arr);
                return acc + calculate_body_size(arrow_proxy);
            }
        );
    }

    flatbuffers::FlatBufferBuilder get_record_batch_message_builder(
        const sparrow::record_batch& record_batch,
        const std::vector<org::apache::arrow::flatbuf::FieldNode>& nodes,
        const std::vector<org::apache::arrow::flatbuf::Buffer>& buffers
    )
    {
        flatbuffers::FlatBufferBuilder record_batch_builder;

        auto nodes_offset = record_batch_builder.CreateVectorOfStructs(nodes);
        auto buffers_offset = record_batch_builder.CreateVectorOfStructs(buffers);
        const auto record_batch_offset = org::apache::arrow::flatbuf::CreateRecordBatch(
            record_batch_builder,
            static_cast<int64_t>(record_batch.nb_rows()),
            nodes_offset,
            buffers_offset,
            0,  // TODO: Compression
            0   // TODO :variadic buffer Counts
        );

        const int64_t body_size = calculate_body_size(record_batch);
        const auto record_batch_message_offset = org::apache::arrow::flatbuf::CreateMessage(
            record_batch_builder,
            org::apache::arrow::flatbuf::MetadataVersion::V5,
            org::apache::arrow::flatbuf::MessageHeader::RecordBatch,
            record_batch_offset.Union(),
            body_size,  // body length
            0           // custom metadata
        );
        record_batch_builder.Finish(record_batch_message_offset);
        return record_batch_builder;
    }

    std::vector<uint8_t> serialize_record_batch(const sparrow::record_batch& record_batch)
    {
        std::vector<org::apache::arrow::flatbuf::FieldNode> nodes = create_fieldnodes(record_batch);
        std::vector<org::apache::arrow::flatbuf::Buffer> flatbuf_buffers = get_buffers(record_batch);
        flatbuffers::FlatBufferBuilder record_batch_builder = get_record_batch_message_builder(
            record_batch,
            nodes,
            flatbuf_buffers
        );
        std::vector<uint8_t> output;
        output.insert(output.end(), continuation.begin(), continuation.end());
        const flatbuffers::uoffset_t record_batch_len = record_batch_builder.GetSize();
        output.insert(
            output.end(),
            reinterpret_cast<const uint8_t*>(&record_batch_len),
            reinterpret_cast<const uint8_t*>(&record_batch_len) + sizeof(record_batch_len)
        );
        output.insert(
            output.end(),
            record_batch_builder.GetBufferPointer(),
            record_batch_builder.GetBufferPointer() + record_batch_len
        );
        // padding to 8 bytes
        output.insert(
            output.end(),
            utils::align_to_8(static_cast<int64_t>(output.size())) - static_cast<int64_t>(output.size()),
            0
        );
        std::vector<uint8_t> body = generate_body(record_batch);
        output.insert(output.end(), std::make_move_iterator(body.begin()), std::make_move_iterator(body.end()));
        return output;
    }

}