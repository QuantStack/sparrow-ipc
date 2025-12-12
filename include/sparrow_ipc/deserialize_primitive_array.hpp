#pragma once

#include <optional>
#include <vector>

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/date_array.hpp>
#include <sparrow/primitive_array.hpp>
#include <sparrow/time_array.hpp>
#include <sparrow/timestamp_array.hpp>
#include <sparrow/timestamp_without_timezone_array.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/deserialize_array_impl.hpp"

namespace sparrow_ipc
{
    template <typename T>
    [[nodiscard]] sparrow::date_array<T> deserialize_non_owning_date_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index
    )
    {
        const std::string_view format = data_type_to_format(
            sparrow::detail::get_data_type_from_array<sparrow::date_array<T>>::get()
        );

        // Set up flags based on nullable
        std::optional<std::unordered_set<sparrow::ArrowFlag>> flags;
        if (nullable)
        {
            flags = std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::NULLABLE};
        }

        ArrowSchema schema = make_non_owning_arrow_schema(
            format,
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
            buffers.push_back(validity_buffer_span);
            buffers.push_back(data_buffer_span);
        }

        // TODO bitmap_ptr is not used anymore... Leave it for now, and remove later if no need confirmed
        const auto [bitmap_ptr, null_count] = utils::get_bitmap_pointer_and_null_count(validity_buffer_span, record_batch.length());

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
        return sparrow::date_array<T>{std::move(ap)};
    }

    template <typename T>
    [[nodiscard]] sparrow::timestamp_array<T> deserialize_non_owning_timestamp_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index,
        const std::string& timezone
    )
    {
        std::string format = std::string(data_type_to_format(
            sparrow::detail::get_data_type_from_array<sparrow::timestamp_array<T>>::get()
        )) + timezone;

        // Set up flags based on nullable
        std::optional<std::unordered_set<sparrow::ArrowFlag>> flags;
        if (nullable)
        {
            flags = std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::NULLABLE};
        }

        ArrowSchema schema = make_non_owning_arrow_schema(
            format.c_str(),
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
            buffers.push_back(validity_buffer_span);
            buffers.push_back(data_buffer_span);
        }

        // TODO bitmap_ptr is not used anymore... Leave it for now, and remove later if no need confirmed
        const auto [bitmap_ptr, null_count] = utils::get_bitmap_pointer_and_null_count(validity_buffer_span, record_batch.length());

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
        return sparrow::timestamp_array<T>{std::move(ap)};
    }

    template <typename T>
    [[nodiscard]] sparrow::timestamp_without_timezone_array<T> deserialize_non_owning_timestamp_without_timezone_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index
    )
    {
        const std::string_view format = data_type_to_format(
            sparrow::detail::get_data_type_from_array<sparrow::timestamp_without_timezone_array<T>>::get()
        );

        // Set up flags based on nullable
        std::optional<std::unordered_set<sparrow::ArrowFlag>> flags;
        if (nullable)
        {
            flags = std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::NULLABLE};
        }

        ArrowSchema schema = make_non_owning_arrow_schema(
            format,
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
            buffers.push_back(validity_buffer_span);
            buffers.push_back(data_buffer_span);
        }

        // TODO bitmap_ptr is not used anymore... Leave it for now, and remove later if no need confirmed
        const auto [bitmap_ptr, null_count] = utils::get_bitmap_pointer_and_null_count(validity_buffer_span, record_batch.length());

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
        return sparrow::timestamp_without_timezone_array<T>{std::move(ap)};
    }

    template <typename T>
    [[nodiscard]] sparrow::time_array<T> deserialize_non_owning_time_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index
    )
    {
        const std::string_view format = data_type_to_format(
            sparrow::detail::get_data_type_from_array<sparrow::time_array<T>>::get()
        );

        // Set up flags based on nullable
        std::optional<std::unordered_set<sparrow::ArrowFlag>> flags;
        if (nullable)
        {
            flags = std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::NULLABLE};
        }

        ArrowSchema schema = make_non_owning_arrow_schema(
            format,
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
            buffers.push_back(validity_buffer_span);
            buffers.push_back(data_buffer_span);
        }

        // TODO bitmap_ptr is not used anymore... Leave it for now, and remove later if no need confirmed
        const auto [bitmap_ptr, null_count] = utils::get_bitmap_pointer_and_null_count(validity_buffer_span, record_batch.length());

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
        return sparrow::time_array<T>{std::move(ap)};
    }

    template <typename T>
    [[nodiscard]] sparrow::primitive_array<T> deserialize_non_owning_primitive_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index
    )
    {
        return detail::deserialize_non_owning_simple_array<sparrow::primitive_array, T>(
            record_batch,
            body,
            name,
            metadata,
            nullable,
            buffer_index
        );
    }
}
