#pragma once

#include <ostream>
#include <ranges>
#include <vector>

#include <sparrow/record_batch.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/config/config.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    [[nodiscard]] std::vector<uint8_t> serialize(const R& record_batches);

    [[nodiscard]] SPARROW_IPC_API std::vector<uint8_t>
    serialize_schema_message(const sparrow::record_batch& record_batch);

    [[nodiscard]] SPARROW_IPC_API std::vector<uint8_t>
    serialize_record_batch(const sparrow::record_batch& record_batch);

    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    [[nodiscard]] std::vector<uint8_t> serialize_record_batches(const R& record_batches)
    {
        std::vector<uint8_t> output;
        for (const auto& record_batch : record_batches)
        {
            const auto rb_serialized = serialize_record_batch(record_batch);
            output.insert(
                output.end(),
                std::make_move_iterator(rb_serialized.begin()),
                std::make_move_iterator(rb_serialized.end())
            );
        }
        return output;
    }

    [[nodiscard]] SPARROW_IPC_API
        flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>>
        create_metadata(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema);

    [[nodiscard]] SPARROW_IPC_API ::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>
    create_field(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema);

    [[nodiscard]] SPARROW_IPC_API ::flatbuffers::Offset<
        ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>>>
    create_children(flatbuffers::FlatBufferBuilder& builder, sparrow::record_batch::column_range columns);

    [[nodiscard]] SPARROW_IPC_API ::flatbuffers::Offset<
        ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>>>
    create_children(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema);

    [[nodiscard]] SPARROW_IPC_API flatbuffers::FlatBufferBuilder
    get_schema_message_builder(const sparrow::record_batch& record_batch);

    [[nodiscard]] SPARROW_IPC_API std::vector<uint8_t>
    serialize_schema_message(const sparrow::record_batch& record_batch);

    SPARROW_IPC_API void fill_fieldnodes(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::FieldNode>& nodes
    );

    [[nodiscard]] SPARROW_IPC_API std::vector<org::apache::arrow::flatbuf::FieldNode>
    create_fieldnodes(const sparrow::record_batch& record_batch);

    SPARROW_IPC_API void fill_buffers(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::Buffer>& flatbuf_buffers,
        int64_t& offset
    );

    [[nodiscard]] SPARROW_IPC_API std::vector<org::apache::arrow::flatbuf::Buffer>
    get_buffers(const sparrow::record_batch& record_batch);

    SPARROW_IPC_API void fill_body(const sparrow::arrow_proxy& arrow_proxy, std::vector<uint8_t>& body);
    [[nodiscard]] SPARROW_IPC_API std::vector<uint8_t> generate_body(const sparrow::record_batch& record_batch);
    [[nodiscard]] SPARROW_IPC_API int64_t calculate_body_size(const sparrow::arrow_proxy& arrow_proxy);
    [[nodiscard]] SPARROW_IPC_API int64_t calculate_body_size(const sparrow::record_batch& record_batch);

    [[nodiscard]] SPARROW_IPC_API flatbuffers::FlatBufferBuilder get_record_batch_message_builder(
        const sparrow::record_batch& record_batch,
        const std::vector<org::apache::arrow::flatbuf::FieldNode>& nodes,
        const std::vector<org::apache::arrow::flatbuf::Buffer>& buffers
    );

    [[nodiscard]] SPARROW_IPC_API std::vector<uint8_t>
    serialize_record_batch(const sparrow::record_batch& record_batch);

    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    std::vector<uint8_t> serialize(const R& record_batches)
    {
        if (record_batches.empty())
        {
            return {};
        }
        if (!utils::check_record_batches_consistency(record_batches))
        {
            throw std::invalid_argument(
                "All record batches must have the same schema to be serialized together."
            );
        }
        std::vector<uint8_t> serialized_schema = serialize_schema_message(record_batches[0]);
        std::vector<uint8_t> serialized_record_batches = serialize_record_batches(record_batches);
        serialized_schema.insert(
            serialized_schema.end(),
            std::make_move_iterator(serialized_record_batches.begin()),
            std::make_move_iterator(serialized_record_batches.end())
        );
        // End of stream message
        serialized_schema.insert(serialized_schema.end(), end_of_stream.begin(), end_of_stream.end());
        return serialized_schema;
    }
}
