#pragma once

#include <optional>
#include <vector>

#include "sparrow.hpp"

#include "config/config.hpp"

namespace sparrow_ipc
{
	namespace details
	{
	    SPARROW_IPC_API void serialize_schema_message(const ArrowSchema& arrow_schema, const std::optional<sparrow::key_value_view>& metadata, std::vector<uint8_t>& final_buffer);
	    SPARROW_IPC_API void serialize_record_batch_message(const ArrowArray& arrow_arr, std::vector<uint8_t>& final_buffer);
	}
}
