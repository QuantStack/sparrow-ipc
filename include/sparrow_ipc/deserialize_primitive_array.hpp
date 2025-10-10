#pragma once

#include <optional>
#include <vector>

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/primitive_array.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/compression.hpp"
#include "sparrow_ipc/deserialize_utils.hpp"

namespace sparrow_ipc
{
    namespace
    {
        struct DecompressedBuffers
        {
            std::vector<uint8_t> validity_buffer;
            std::vector<uint8_t> data_buffer;
        };

        void release_decompressed_buffers(ArrowArray* array)
        {
            if (array->private_data)
            {
                delete static_cast<DecompressedBuffers*>(array->private_data);
                array->private_data = nullptr;
            }
            array->release = nullptr;
        }
    }

    template <typename T>
    [[nodiscard]] sparrow::primitive_array<T> deserialize_non_owning_primitive_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        size_t& buffer_index
    )
    {
        const auto compression = record_batch.compression();
        DecompressedBuffers* decompressed_buffers_owner = nullptr;

        // Validity buffer
        const auto validity_buffer_metadata = record_batch.buffers()->Get(buffer_index++);
        auto validity_buffer_span = body.subspan(validity_buffer_metadata->offset(), validity_buffer_metadata->length());
        if (compression)
        {
            if (!decompressed_buffers_owner)
            {
                decompressed_buffers_owner = new DecompressedBuffers();
            }
            decompressed_buffers_owner->validity_buffer = decompress(compression->codec(), validity_buffer_span);
            validity_buffer_span = decompressed_buffers_owner->validity_buffer;
        }
        auto bitmap_ptr = const_cast<uint8_t*>(validity_buffer_span.data());
        const sparrow::dynamic_bitset_view<const std::uint8_t> bitmap_view{
            bitmap_ptr,
            static_cast<size_t>(record_batch.length())};
        auto null_count = bitmap_view.null_count();
        if (validity_buffer_metadata->length() == 0)
        {
            bitmap_ptr = nullptr;
            null_count = 0;
        }

        // Data buffer
        const auto primitive_buffer_metadata = record_batch.buffers()->Get(buffer_index++);
        auto data_buffer_span = body.subspan(primitive_buffer_metadata->offset(), primitive_buffer_metadata->length());
        if (compression)
        {
            if (!decompressed_buffers_owner)
            {
                decompressed_buffers_owner = new DecompressedBuffers();
            }
            decompressed_buffers_owner->data_buffer = decompress(compression->codec(), data_buffer_span);
            data_buffer_span = decompressed_buffers_owner->data_buffer;
        }
        auto primitives_ptr = const_cast<uint8_t*>(data_buffer_span.data());

        const std::string_view format = data_type_to_format(
            sparrow::detail::get_data_type_from_array<sparrow::primitive_array<T>>::get()
        );
        ArrowSchema schema = make_non_owning_arrow_schema(
            format,
            name.data(),
            metadata,
            std::nullopt,
            0,
            nullptr,
            nullptr
        );

        std::vector<std::uint8_t*> buffers = {bitmap_ptr, primitives_ptr};
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
            array.release = release_decompressed_buffers;
        }

        sparrow::arrow_proxy ap{std::move(array), std::move(schema)};
        return sparrow::primitive_array<T>{std::move(ap)};
    }
}
