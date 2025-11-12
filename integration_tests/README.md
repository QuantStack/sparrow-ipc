# Integration Tests

This directory contains integration test tools for `sparrow-ipc`.

## Tools

### `arrow_file_to_stream`

Reads a JSON file containing Arrow record batches and outputs the serialized Arrow IPC stream to stdout.

**Usage:**
```bash
./arrow_file_to_stream <json_file_path> > output.stream
```

**Example:**
```bash
# Convert a JSON file to an Arrow IPC stream file
./arrow_file_to_stream input.json > output.stream
```

### `arrow_stream_to_file`

Reads an Arrow IPC stream from a file and writes it to another file.

**Usage:**
```bash
./arrow_stream_to_file <input_file_path> <output_file_path>
```

**Example:**
```bash
# Read stream from file and write to another file
./arrow_stream_to_file input.stream output.stream
```

## Round-trip Example

You can chain these tools to perform a round-trip conversion:

```bash
# JSON -> Stream file -> Stream file (verify)
./arrow_file_to_stream input.json > intermediate.stream
./arrow_stream_to_file intermediate.stream final.stream
```

Or test a complete pipeline:

```bash
# Convert JSON to stream and process it
./arrow_file_to_stream test.json > test.stream
./arrow_stream_to_file test.stream test_verified.stream
```

## Building

To build these integration tests, enable the `SPARROW_IPC_BUILD_INTEGRATION_TESTS` CMake option:

```bash
cmake -DSPARROW_IPC_BUILD_INTEGRATION_TESTS=ON ..
cmake --build .
```

The executables will be built in your build directory under `integration_tests/`.

## Testing

The integration tests include automated tests that verify the functionality of both tools:

### Running Tests

```bash
# Run the test suite
./test_integration_tools

# Or using CTest
ctest -R integration_tools_test
```

### Test Coverage

The test suite includes:
- **Argument validation**: Tests for missing or incorrect arguments
- **File not found**: Tests error handling for non-existent files
- **Valid conversions**: Tests successful JSON to stream conversions
- **Stream processing**: Tests valid stream file processing
- **Round-trip testing**: Tests JSON → stream → file → deserialize pipeline
- **Paths with spaces**: Tests handling of file paths containing spaces
- **Multiple file types**: Tests various Arrow data types (primitive, binary, zero-length, etc.)

All tests automatically use the Arrow testing data from the `ARROW_TESTING_DATA_DIR` if available.
