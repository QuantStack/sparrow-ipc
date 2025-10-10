#pragma once

#include <span>

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/variable_size_binary_array.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/compression.hpp"
#include "sparrow_ipc/deserialize_utils.hpp"

namespace sparrow_ipc
{
    // TODO after handling deserialize_primitive_array, do the same here and then in other data types
    namespace
    {
        struct DecompressedBinaryBuffers
        {
            std::vector<uint8_t> validity_buffer;
            std::vector<uint8_t> offset_buffer;
            std::vector<uint8_t> data_buffer;
        };

        void release_decompressed_binary_buffers(ArrowArray* array)
        {
            if (array->private_data)
            {
                delete static_cast<DecompressedBinaryBuffers*>(array->private_data);
                array->private_data = nullptr;
            }
            array->release = nullptr;
        }
    }

    template <typename T>
    [[nodiscard]] T deserialize_non_owning_variable_size_binary(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        size_t& buffer_index
    )
    {
        const auto compression = record_batch.compression();
        DecompressedBinaryBuffers* decompressed_buffers_owner = nullptr;

        // Validity buffer
        const auto validity_buffer_metadata = record_batch.buffers()->Get(buffer_index++);
        auto validity_buffer_span = body.subspan(validity_buffer_metadata->offset(), validity_buffer_metadata->length());
        if (compression && validity_buffer_metadata->length() > 0)
        {
            if (!decompressed_buffers_owner)
            {
                decompressed_buffers_owner = new DecompressedBinaryBuffers();
            }
            decompressed_buffers_owner->validity_buffer = decompress(compression->codec(), validity_buffer_span);
            validity_buffer_span = decompressed_buffers_owner->validity_buffer;
        }

        uint8_t* bitmap_ptr = nullptr;
        int64_t null_count = 0;
        if (validity_buffer_metadata->length() > 0)
        {
            bitmap_ptr = const_cast<uint8_t*>(validity_buffer_span.data());
            const sparrow::dynamic_bitset_view<const std::uint8_t> bitmap_view{
                bitmap_ptr,
                static_cast<size_t>(record_batch.length())};
            null_count = bitmap_view.null_count();
        }

        // Offset buffer
        const auto offset_metadata = record_batch.buffers()->Get(buffer_index++);
        auto offset_buffer_span = body.subspan(offset_metadata->offset(), offset_metadata->length());
        if (compression)
        {
            if (!decompressed_buffers_owner)
            {
                decompressed_buffers_owner = new DecompressedBinaryBuffers();
            }
            decompressed_buffers_owner->offset_buffer = decompress(compression->codec(), offset_buffer_span);
            offset_buffer_span = decompressed_buffers_owner->offset_buffer;
        }
        auto offset_ptr = const_cast<uint8_t*>(offset_buffer_span.data());

        // Data buffer
        const auto buffer_metadata = record_batch.buffers()->Get(buffer_index++);
        auto data_buffer_span = body.subspan(buffer_metadata->offset(), buffer_metadata->length());
        if (compression)
        {
            if (!decompressed_buffers_owner)
            {
                decompressed_buffers_owner = new DecompressedBinaryBuffers();
            }
            decompressed_buffers_owner->data_buffer = decompress(compression->codec(), data_buffer_span);
            data_buffer_span = decompressed_buffers_owner->data_buffer;
        }
        auto buffer_ptr = const_cast<uint8_t*>(data_buffer_span.data());

        const std::string_view format = data_type_to_format(sparrow::detail::get_data_type_from_array<T>::get());
        ArrowSchema schema = make_non_owning_arrow_schema(
            format,
            name.data(),
            metadata,
            std::nullopt,
            0,
            nullptr,
            nullptr
        );

        std::vector<std::uint8_t*> buffers = {bitmap_ptr, offset_ptr, buffer_ptr};
        ArrowArray array = make_non_owning_arrow_array(
            record_batch.length(),
            null_count,
            0,
            std::move(buffers),
            0,
            nullptr,
            nullptr
        );

        if (decompressed_buffers_owner)
        {
            array.private_data = decompressed_buffers_owner;
            array.release = release_decompressed_binary_buffers;
        }

        sparrow::arrow_proxy ap{std::move(array), std::move(schema)};
        return T{std::move(ap)};
    }
}
