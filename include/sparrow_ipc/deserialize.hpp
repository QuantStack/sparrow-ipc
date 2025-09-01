#pragma once

#include <optional>
#include <string>

#include <sparrow/record_batch.hpp>

#include "config/config.hpp"
#include "Message_generated.h"
#include "sparrow_ipc/encapsulated_message.hpp"
#include "SparseTensor_generated.h"


namespace sparrow_ipc
{
    SPARROW_IPC_API void deserialize_schema_message(
        const uint8_t* buf_ptr,
        size_t& current_offset,
        std::optional<std::string>& name,
        std::optional<std::vector<sparrow::metadata_pair>>& metadata
    );
    SPARROW_IPC_API [[nodiscard]] const org::apache::arrow::flatbuf::RecordBatch*
    deserialize_record_batch_message(const uint8_t* buf_ptr, size_t& current_offset);

    SPARROW_IPC_API [[nodiscard]] std::vector<sparrow::record_batch> deserialize_stream(const uint8_t* buf_ptr);
}