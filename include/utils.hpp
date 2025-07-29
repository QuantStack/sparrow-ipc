#pragma once

#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>

#include "Schema_generated.h"

#include "config/config.hpp"

// TODO add tests
// TODO in that case define #ifdef TESTING for the fcts here to be testable and available in anonymous namespaces
namespace sparrow_ipc
{
    namespace utils
    {
        // Aligns a value to the next multiple of 8, as required by the Arrow IPC format for message bodies
        SPARROW_IPC_API int64_t align_to_8(int64_t n);

        // Creates a Flatbuffers type from a format string
        // This function maps a sparrow data type to the corresponding Flatbuffers type
        SPARROW_IPC_API std::pair<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>>
        get_flatbuffer_type(flatbuffers::FlatBufferBuilder& builder, std::string_view format_str);
    }
}
