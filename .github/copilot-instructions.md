# Sparrow-IPC AI Agent Instructions

C++20 library for Arrow IPC serialization/deserialization using FlatBuffers. See [../examples/write_and_read_streams.cpp](../examples/write_and_read_streams.cpp) for usage patterns.

## Architecture

- **Serialization**: `record_batch` → `serializer` → FlatBuffer metadata + body → stream (continuation bytes + length + message + padding + data)
- **Deserialization**: Binary stream → `extract_encapsulated_message()` → parse FlatBuffer → reconstruct `record_batch`
- **Critical**: All record batches in a stream must have identical schemas (validated in `serialize_record_batches_to_ipc_stream`)
- **Memory model**: Deserialized arrays use `std::span<const uint8_t>` - source buffer must outlive arrays

## Build System

**Dependency fetching** (unique pattern in `cmake/external_dependencies.cmake`):
- `FETCH_DEPENDENCIES_WITH_CMAKE=OFF` - require via `find_package()` (CI default)
- `FETCH_DEPENDENCIES_WITH_CMAKE=MISSING` - auto-fetch missing (local dev)
- All binaries/libs → `${CMAKE_BINARY_DIR}/bin/${CMAKE_BUILD_TYPE}` (not standard locations)

**FlatBuffer schemas**: Auto-downloaded from Apache Arrow during configure → `${CMAKE_BINARY_DIR}/generated/*_generated.h`. Never edit generated headers.

**Build**:
```bash
mamba env create -f environment-dev.yml && mamba activate sparrow-ipc
cmake -B build -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX -DCMAKE_PREFIX_PATH=$CONDA_PREFIX -DSPARROW_IPC_BUILD_TESTS=ON
cmake --build build -j12
cmake --build build --target run_tests
```

## Platform-Specific Patterns

**Linux executables linking sparrow-ipc**: Must set RPATH (libs in same dir):
```cmake
set_target_properties(my_exe PROPERTIES
    BUILD_RPATH_USE_ORIGIN ON
    BUILD_RPATH "$ORIGIN"
    INSTALL_RPATH "$ORIGIN")
```
See `integration_tests/CMakeLists.txt` for examples. Missing this causes "cannot open shared object file" errors.

**Windows**: Explicit DLL copying in CMakeLists (see `tests/CMakeLists.txt:32-47`).

## Testing

- Arrow test data: Auto-fetched from `apache/arrow-testing`, `.json.gz` files extracted during configure
- Unit tests: `cmake --build build --target run_tests`
- Integration tests: `integration_tests/` tools integrate with Apache Arrow's Archery framework via Docker

## Naming & Style

- `snake_case` for everything (types, functions)
- `m_` prefix for members
- Namespace: `sparrow_ipc`
- Format: `cmake --build build --target clang-format` (requires `ACTIVATE_LINTER=ON`)

## Common Issues

1. Schema mismatches in stream → `std::invalid_argument`
2. Deallocating source buffer while arrays in use → undefined behavior
3. Missing RPATH on Linux → runtime linking errors
4. Only LZ4 compression supported (not ZSTD yet)
