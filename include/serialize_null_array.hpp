#pragma once

#include "config/config.hpp"
#include "serialize.hpp"

namespace sparrow_ipc
{
    // TODO Use `arr` as const after fixing the issue upstream in sparrow::get_arrow_structures
    SPARROW_IPC_API std::vector<uint8_t> serialize_null_array(sparrow::null_array& arr);
    SPARROW_IPC_API sparrow::null_array deserialize_null_array(const std::vector<uint8_t>& buffer);
}
