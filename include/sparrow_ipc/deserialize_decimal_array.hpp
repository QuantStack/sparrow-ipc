#pragma once

#include <span>

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/decimal_array.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/deserialize_utils.hpp"

namespace sparrow_ipc
{
    template <sparrow::decimal_type T>
    [[nodiscard]] sparrow::decimal_array<T> deserialize_non_owning_decimal(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index,
        int32_t scale,
        int32_t precision
    )
    {
        constexpr std::size_t sizeof_decimal = sizeof(typename T::integer_type);
        std::string format_str = "d:" + std::to_string(precision) + "," + std::to_string(scale);
        if constexpr (sizeof_decimal != 16)  // We don't need to specify the size for 128-bit
                                             // decimals
        {
            format_str += "," + std::to_string(sizeof_decimal * 8);
        }

        // Set up flags based on nullable
        std::optional<std::unordered_set<sparrow::ArrowFlag>> flags;
        if (nullable)
        {
            flags = std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::NULLABLE};
        }

        ArrowSchema schema = make_non_owning_arrow_schema(
            format_str,
            name.data(),
            metadata,
            flags,
            0,
            nullptr,
            nullptr
        );

        const auto compression = record_batch.compression();
        std::vector<arrow_array_private_data::optionally_owned_buffer> buffers;

        auto validity_buffer_span = utils::get_buffer(record_batch, body, buffer_index);
        auto data_buffer_span = utils::get_buffer(record_batch, body, buffer_index);

        if (compression)
        {
            buffers.push_back(utils::get_decompressed_buffer(validity_buffer_span, compression));
            buffers.push_back(utils::get_decompressed_buffer(data_buffer_span, compression));
        }
        else
        {
            buffers.emplace_back(validity_buffer_span);
            sparrow::buffer<std::uint8_t> data_buffer_copy(data_buffer_span.size(), sparrow::buffer<std::uint8_t>::default_allocator());
            std::memcpy(data_buffer_copy.data(), data_buffer_span.data(), data_buffer_span.size());
            buffers.emplace_back(std::move(data_buffer_copy));
        }

        const auto [bitmap_ptr, null_count] = utils::get_bitmap_pointer_and_null_count(
            validity_buffer_span,
            record_batch.length()
        );

        ArrowArray array = make_arrow_array<arrow_array_private_data>(
            record_batch.length(),
            null_count,
            0,
            0,
            nullptr,
            nullptr,
            std::move(buffers)
        );
        sparrow::arrow_proxy ap{std::move(array), std::move(schema)};
        return sparrow::decimal_array<T>(std::move(ap));
    }
}