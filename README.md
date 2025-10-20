# sparrow-ipc

[![GHA Linux](https://github.com/quantstack/sparrow-ipc/actions/workflows/linux.yml/badge.svg)](https://github.com/quantstack/sparrow-ipc/actions/workflows/linux.yml)
[![GHA OSX](https://github.com/quantstack/sparrow-ipc/actions/workflows/osx.yml/badge.svg)](https://github.com/quantstack/sparrow-ipc/actions/workflows/osx.yml)
[![GHA Windows](https://github.com/quantstack/sparrow-ipc/actions/workflows/windows.yml/badge.svg)](https://github.com/quantstack/sparrow-ipc/actions/workflows/windows.yml)

**!!!Sparrow-IPC is still under development and is not ready for production use!!!**

**!!!The documentation is still under development and may be incomplete or contain errors!!!**

## Introduction

`sparrow-ipc` provides high-performance, **zero-copy** serialization and deserialization of record batches, adhering to both [sparrow](https://github.com/man-group/sparrow) and [Apache Arrow IPC specifications](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc).

`sparrow-ipc` requires a modern C++ compiler supporting C++20.

## Installation


### Install from sources

`sparrow-ipc` has a few dependencies that you can install in a mamba environment:

```bash
mamba env create -f environment-dev.yml
mamba activate sparrow-ipc
```

You can then create a build directory, and build the project and install it with cmake:

```bash
mkdir build
de build
cmake .. \
    -DCMAKE_BUILD_TYPE=Debug \
          -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX \
          -DCMAKE_PREFIX_PATH=$CONDA_PREFIX \
          -DSPARROW_IPC_BUILD_TESTS=ON \
          -DSPARROW_IPC_BUILD_EXAMPLES=ON

make install
```

## Usage

### Requirements

Compilers:
- Clang 18 or higher
- GCC 11.2 or higher
- Apple Clang 16 or higher
- MSVC 19.41 or higher

### Serialize record batches to a memory stream

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

### Pipe a source of record batches to a stream

```cpp
#include <optional>
#include <ostream>
#include <vector>
#include <sparrow_ipc/memory_output_stream.hpp>
#include <sparrow_ipc/serializer.hpp>
#include <sparrow/record_batch.hpp>

namespace sp = sparrow;

class record_batch_source
{
public:
    std::optional<sp::record_batch> next();
};

void stream_record_batches(std::ostream& os, record_batch_source& source)
{
    sp::serializer serial(os);
    std::optional<sp::record_batch> batch = std::nullopt;
    while (batch = source.next())
    {
        serial << batch;
    }
    serial << sp_ipc::end_stream;
}
```

### Deserialize a stream into record batches

```cpp
#include <vector>
#include <sparrow_ipc/deserializer.hpp>
#include <sparrow/record_batch.hpp>

namespace sp = sparrow;
namespace sp_ipc = sparrow_ipc;

std::vector<sp::record_batch> deserialize_stream_to_batches(const std::vector<uint8_t>& stream_data)
{
    auto batches = sp_ipc::deserialize_stream(stream_data);
    return batches;
}
```

## Documentation

The documentation (currently being written) can be found at https://quantstack.github.io/sparrow-ipc/index.html

## Acknowledgements

This project is developed by [QuantStack](quantstack.net), building on the foundations laid by the sparrow library and the Apache Arrow project.

## License

This software is licensed under the BSD-3-Clause license. See the [LICENSE](LICENSE) file for details.
