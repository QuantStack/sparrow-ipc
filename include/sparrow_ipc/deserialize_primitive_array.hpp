#pragma once

#include <optional>
#include <vector>

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/primitive_array.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/deserialize_utils.hpp"

namespace sparrow_ipc
{
    template <typename T>
    [[nodiscard]] sparrow::primitive_array<T> deserialize_non_owning_primitive_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        size_t& buffer_index
    )
    {
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

        const auto compression = record_batch.compression();
        std::vector<std::vector<std::uint8_t>> decompressed_buffers;

        // TODO do not decompress validity buffers?
        auto validity_buffer_span = utils::get_and_decompress_buffer(record_batch, body, buffer_index, compression, decompressed_buffers);

        const auto [bitmap_ptr, null_count] = utils::get_bitmap_pointer_and_null_count(validity_buffer_span, record_batch.length());

        auto data_buffer_span = utils::get_and_decompress_buffer(record_batch, body, buffer_index, compression, decompressed_buffers);

        ArrowArray array;
        if (compression)
        {
            array = make_arrow_array<owning_arrow_array_private_data>(
                record_batch.length(),
                null_count,
                0,
                0,
                nullptr,
                nullptr,
                std::move(decompressed_buffers)
            );
        }
        else
        {
            auto primitives_ptr = const_cast<uint8_t*>(data_buffer_span.data());
            std::vector<std::uint8_t*> buffers = {bitmap_ptr, primitives_ptr};
            array = make_arrow_array<non_owning_arrow_array_private_data>(
                record_batch.length(),
                null_count,
                0,
                0,
                nullptr,
                nullptr,
                std::move(buffers)
            );
        }

        sparrow::arrow_proxy ap{std::move(array), std::move(schema)};
        return sparrow::primitive_array<T>{std::move(ap)};
    }
}
