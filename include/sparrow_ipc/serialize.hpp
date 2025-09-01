#pragma once


#include <cstdint>
#include <vector>

#include <sparrow/array_api.hpp>
#include <sparrow/buffer/dynamic_bitset/dynamic_bitset_view.hpp>
#include <sparrow/record_batch.hpp>

#include "config/config.hpp"


namespace sparrow_ipc
{
    namespace details
    {
        SPARROW_IPC_API std::vector<uint8_t> serialize_schema_message(const ArrowSchema& arrow_schema);
        SPARROW_IPC_API void serialize_record_batch_message(
            const ArrowArray& arrow_arr,
            const std::vector<int64_t>& buffers_sizes,
            std::vector<uint8_t>& final_buffer
        );
    }
}
