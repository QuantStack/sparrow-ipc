# Serialization and Deserialization                             {#serialization}

This page describes how to serialize and deserialize record batches using `sparrow-ipc`.

## Overview

`sparrow-ipc` provides two main approaches for both serialization and deserialization:

- **Function API**: Simple one-shot operations for serializing/deserializing complete data
- **Class API**: Streaming-oriented classes (`serializer` and `deserializer`) for incremental operations

## Serialization

### Serialize record batches to a memory stream

The simplest way to serialize record batches is to use the `serializer` class with a `memory_output_stream`:

```cpp
#include <vector>
#include <sparrow_ipc/memory_output_stream.hpp>
#include <sparrow_ipc/serializer.hpp>
#include <sparrow/record_batch.hpp>

namespace sp = sparrow;
namespace sp_ipc = sparrow_ipc;

std::vector<uint8_t> serialize_batches_to_stream(const std::vector<sp::record_batch>& batches)
{
    std::vector<uint8_t> stream_data;
    sp_ipc::memory_output_stream stream(stream_data);
    sp_ipc::serializer serializer(stream);

    // Serialize all batches using the streaming operator
    serializer << batches << sp_ipc::end_stream;

    return stream_data;
}
```

### Serialize individual record batches

You can also serialize record batches one at a time:

```cpp
#include <vector>
#include <sparrow_ipc/memory_output_stream.hpp>
#include <sparrow_ipc/serializer.hpp>
#include <sparrow/record_batch.hpp>

namespace sp = sparrow;
namespace sp_ipc = sparrow_ipc;

std::vector<uint8_t> serialize_batches_individually(const std::vector<sp::record_batch>& batches)
{
    std::vector<uint8_t> stream_data;
    sp_ipc::memory_output_stream stream(stream_data);
    sp_ipc::serializer serializer(stream);

    // Serialize batches one by one
    for (const auto& batch : batches)
    {
        serializer << batch;
    }

    // Don't forget to end the stream
    serializer << sp_ipc::end_stream;

    return stream_data;
}
```

### Pipe a source of record batches to a stream

For streaming scenarios where batches are generated on-the-fly:

```cpp
#include <optional>
#include <vector>
#include <sparrow_ipc/memory_output_stream.hpp>
#include <sparrow_ipc/serializer.hpp>
#include <sparrow/record_batch.hpp>

namespace sp = sparrow;
namespace sp_ipc = sparrow_ipc;

class record_batch_source
{
public:
    std::optional<sp::record_batch> next();
};

std::vector<uint8_t> stream_from_source(record_batch_source& source)
{
    std::vector<uint8_t> stream_data;
    sp_ipc::memory_output_stream stream(stream_data);
    sp_ipc::serializer serializer(stream);

    std::optional<sp::record_batch> batch;
    while ((batch = source.next()))
    {
        serializer << *batch;
    }
    serializer << sp_ipc::end_stream;

    return stream_data;
}
```

## Deserialization

### Using the function API

The simplest way to deserialize a complete Arrow IPC stream is using `deserialize_stream`:

```cpp
#include <vector>
#include <sparrow_ipc/deserialize.hpp>
#include <sparrow/record_batch.hpp>

namespace sp = sparrow;
namespace sp_ipc = sparrow_ipc;

std::vector<sp::record_batch> deserialize_stream_example(const std::vector<uint8_t>& stream_data)
{
    // Deserialize the entire stream at once
    auto batches = sp_ipc::deserialize_stream(stream_data);
    return batches;
}
```

### Using the deserializer class

The `deserializer` class provides more control over deserialization and is useful when you want to:
- Accumulate batches into an existing container
- Deserialize data incrementally as it arrives
- Process multiple streams into a single container

#### Basic usage

```cpp
#include <iostream>
#include <span>
#include <vector>
#include <sparrow_ipc/deserializer.hpp>
#include <sparrow/record_batch.hpp>

namespace sp = sparrow;
namespace sp_ipc = sparrow_ipc;

void deserializer_basic_example(const std::vector<uint8_t>& stream_data)
{
    // Create a container to hold the deserialized batches
    std::vector<sp::record_batch> batches;

    // Create a deserializer that will append to our container
    sp_ipc::deserializer deser(batches);

    // Deserialize the stream data
    deser.deserialize(std::span<const uint8_t>(stream_data));

    // Process the accumulated batches
    for (const auto& batch : batches)
    {
        std::cout << "Batch with " << batch.nb_rows() << " rows and " 
                  << batch.nb_columns() << " columns\n";
    }
}
```

#### Incremental deserialization

The `deserializer` class is particularly useful for streaming scenarios where data arrives in chunks:

```cpp
#include <iostream>
#include <span>
#include <vector>
#include <sparrow_ipc/deserializer.hpp>
#include <sparrow/record_batch.hpp>

namespace sp = sparrow;
namespace sp_ipc = sparrow_ipc;

void deserializer_incremental_example(const std::vector<std::vector<uint8_t>>& stream_chunks)
{
    // Container to accumulate all deserialized batches
    std::vector<sp::record_batch> batches;

    // Create a deserializer
    sp_ipc::deserializer deser(batches);

    // Deserialize chunks as they arrive using the streaming operator
    for (const auto& chunk : stream_chunks)
    {
        deser << std::span<const uint8_t>(chunk);
        std::cout << "After chunk: " << batches.size() << " batches accumulated\n";
    }

    // All batches are now available in the container
    std::cout << "Total batches deserialized: " << batches.size() << "\n";
}
```

#### Chaining deserializations

The streaming operator can be chained for fluent API usage:

```cpp
#include <span>
#include <vector>
#include <sparrow_ipc/deserializer.hpp>
#include <sparrow/record_batch.hpp>

namespace sp = sparrow;
namespace sp_ipc = sparrow_ipc;

void deserializer_chaining_example(
    const std::vector<uint8_t>& chunk1,
    const std::vector<uint8_t>& chunk2,
    const std::vector<uint8_t>& chunk3)
{
    std::vector<sp::record_batch> batches;
    sp_ipc::deserializer deser(batches);

    // Chain multiple deserializations in a single expression
    deser << std::span<const uint8_t>(chunk1)
          << std::span<const uint8_t>(chunk2)
          << std::span<const uint8_t>(chunk3);
}
```

### Using different container types

The `deserializer` class works with any container that satisfies `std::ranges::input_range` and supports `insert` at the end:

```cpp
#include <deque>
#include <list>
#include <span>
#include <vector>
#include <sparrow_ipc/deserializer.hpp>
#include <sparrow/record_batch.hpp>

namespace sp = sparrow;
namespace sp_ipc = sparrow_ipc;

void different_containers_example(const std::vector<uint8_t>& stream_data)
{
    // Using std::deque
    std::deque<sp::record_batch> deque_batches;
    sp_ipc::deserializer deser_deque(deque_batches);
    deser_deque.deserialize(std::span<const uint8_t>(stream_data));

    // Using std::list
    std::list<sp::record_batch> list_batches;
    sp_ipc::deserializer deser_list(list_batches);
    deser_list.deserialize(std::span<const uint8_t>(stream_data));
}
```

## Round-trip example

Here's a complete example showing serialization and deserialization:

```cpp
#include <cassert>
#include <span>
#include <vector>
#include <sparrow_ipc/deserialize.hpp>
#include <sparrow_ipc/deserializer.hpp>
#include <sparrow_ipc/memory_output_stream.hpp>
#include <sparrow_ipc/serializer.hpp>
#include <sparrow/record_batch.hpp>

namespace sp = sparrow;
namespace sp_ipc = sparrow_ipc;

void round_trip_example()
{
    // Create sample data
    auto int_array = sp::primitive_array<int32_t>({1, 2, 3, 4, 5});
    auto string_array = sp::string_array(
        std::vector<std::string>{"hello", "world", "test", "data", "example"}
    );
    
    sp::record_batch original_batch(
        {{"id", sp::array(std::move(int_array))},
         {"name", sp::array(std::move(string_array))}}
    );

    // Serialize
    std::vector<uint8_t> stream_data;
    sp_ipc::memory_output_stream stream(stream_data);
    sp_ipc::serializer serializer(stream);
    serializer << original_batch << sp_ipc::end_stream;

    // Deserialize using function API
    auto deserialized_batches = sp_ipc::deserialize_stream(stream_data);
    
    assert(deserialized_batches.size() == 1);
    assert(deserialized_batches[0].nb_rows() == original_batch.nb_rows());
    assert(deserialized_batches[0].nb_columns() == original_batch.nb_columns());

    // Or using deserializer class
    std::vector<sp::record_batch> batches;
    sp_ipc::deserializer deser(batches);
    deser << std::span<const uint8_t>(stream_data);

    assert(batches.size() == 1);
}
```
