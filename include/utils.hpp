#pragma once

#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>

// TODO what to do with namespace?
// TODO add tests for these?
// TODO add namespace ? sparrow-ipc / detail or utils?
#include "config/config.hpp"

// Aligns a value to the next multiple of 8, as required by the Arrow IPC format for message bodies.
SPARROW_IPC_API int64_t align_to_8(int64_t n);

// Parse the format string
SPARROW_IPC_API std::optional<int32_t> parse_format(std::string_view format_str, std::string_view sep);

SPARROW_IPC_API std::pair<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>>
get_flatbuffer_decimal_type(flatbuffers::FlatBufferBuilder& builder, std::string_view format_str, int32_t bitWidth);

SPARROW_IPC_API std::pair<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>>
get_flatbuffer_type(flatbuffers::FlatBufferBuilder& builder, std::string_view format_str);
