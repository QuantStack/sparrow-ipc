# Serialization and Deserialization                             {#serialization}

This page describes how to serialize and deserialize record batches using `sparrow-ipc`.

## Overview

`sparrow-ipc` provides two main approaches for both serialization and deserialization:

- **Function API**: Simple one-shot operations for serializing/deserializing complete data
- **Class API**: Streaming-oriented classes (`serializer` and `deserializer`) for incremental operations

## Serialization

### Serialize record batches to a memory stream

The simplest way to serialize record batches is to use the `serializer` class with a `memory_output_stream`:

\snippet write_and_read_streams.cpp example_serialize_to_stream

### Serialize individual record batches

You can also serialize record batches one at a time:

\snippet write_and_read_streams.cpp example_serialize_individual

## Deserialization

### Using the function API

The simplest way to deserialize a complete Arrow IPC stream is using `deserialize_stream`:

\snippet deserializer_example.cpp example_deserialize_stream

### Using the deserializer class

The `deserializer` class provides more control over deserialization and is useful when you want to:
- Accumulate batches into an existing container
- Deserialize data incrementally as it arrives
- Process multiple streams into a single container

#### Basic usage

\snippet deserializer_example.cpp example_deserializer_basic

#### Incremental deserialization

The `deserializer` class is particularly useful for streaming scenarios where data arrives in chunks:

\snippet deserializer_example.cpp example_deserializer_incremental

#### Chaining deserializations

The streaming operator can be chained for fluent API usage:

\snippet deserializer_example.cpp example_deserializer_chaining
