#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "sparrow.hpp"

#include "Message_generated.h"
#include "Schema_generated.h"

#include "config/config.hpp"

namespace sparrow_ipc
{
    namespace details
    {
        SPARROW_IPC_API std::vector<uint8_t> serialize_schema_message(const ArrowSchema& arrow_schema);
        SPARROW_IPC_API void serialize_record_batch_message(const ArrowArray& arrow_arr, const std::vector<int64_t>& buffers_sizes, std::vector<uint8_t>& final_buffer);

        SPARROW_IPC_API void deserialize_schema_message(const uint8_t* buf_ptr, size_t& current_offset, std::optional<std::string>& name, std::optional<std::vector<sparrow::metadata_pair>>& metadata);
        SPARROW_IPC_API const org::apache::arrow::flatbuf::RecordBatch* deserialize_record_batch_message(const uint8_t* buf_ptr, size_t& current_offset);
    }
}
