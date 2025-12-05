#include "sparrow_ipc/metadata.hpp"

#include <algorithm>

namespace sparrow_ipc
{
    std::vector<sparrow::metadata_pair> to_sparrow_metadata(
        const ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>& metadata
    )
    {
        std::vector<sparrow::metadata_pair> sparrow_metadata;
        sparrow_metadata.reserve(metadata.size());
        for (const auto& kv : metadata)
        {
            sparrow_metadata.emplace_back(kv->key()->str(), kv->value()->str());
        }
        return sparrow_metadata;
    }
}