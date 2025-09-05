#pragma once

#include <vector>

#include <flatbuffers/flatbuffers.h>

#include <sparrow/utils/metadata.hpp>

#include "Schema_generated.h"

namespace sparrow_ipc
{
    std::vector<sparrow::metadata_pair> to_sparrow_metadata(
        const ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>& metadata
    );
}