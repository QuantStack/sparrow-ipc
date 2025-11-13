#pragma once

#include <flatbuffers/flatbuffers.h>

#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    SPARROW_IPC_API size_t write_footer(
        const sparrow::record_batch& record_batch,
        any_output_stream& stream
    );

    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    void
    serialize_to_file(const R& record_batches, any_output_stream& stream, std::optional<CompressionType> compression)
    {
        if (record_batches.empty())
        {
            return;
        }

        if (!utils::check_record_batches_consistency(record_batches))
        {
            throw std::invalid_argument(
                "All record batches must have the same schema to be serialized together."
            );
        }

        // Write magic bytes at the beginning of the file
        stream.write(arrow_file_header_magic);
        stream.add_padding();

        // Write stream format (schema + record batches) using existing function
        serialize_record_batches_to_ipc_stream(record_batches, stream, compression);

        // Write footer
        const size_t footer_size = write_footer(record_batches.front(), stream);

        // Write footer size (int32, little-endian)
        const int32_t footer_size_i32 = static_cast<int32_t>(footer_size);
        stream.write(
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(&footer_size_i32), sizeof(int32_t))
        );

        // Write magic bytes at the end
        stream.write(arrow_file_magic);
    }
}
