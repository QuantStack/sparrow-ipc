#pragma once

#include <ostream>
#include <ranges>
#include <vector>

#include <sparrow/record_batch.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/config/config.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Serializes a record batch schema into a binary message format.
     *
     * This function creates a serialized schema message by combining continuation bytes,
     * a length prefix, the flatbuffer schema data, and padding to ensure 8-byte alignment.
     * The resulting format follows the Arrow IPC specification for schema messages.
     *
     * @param record_batch The record batch containing the schema to be serialized
     * @return std::vector<uint8_t> A byte vector containing the complete serialized schema message
     *         with continuation bytes, 4-byte length prefix, schema data, and 8-byte alignment padding
     */
    [[nodiscard]] SPARROW_IPC_API std::vector<uint8_t>
    serialize_schema_message(const sparrow::record_batch& record_batch);

    /**
     * @brief Serializes a record batch into a binary format following the Arrow IPC specification.
     *
     * This function converts a sparrow record batch into a serialized byte vector that includes:
     * - A continuation marker at the beginning
     * - The record batch metadata length (4 bytes)
     * - The FlatBuffer-encoded record batch metadata containing field nodes and buffer information
     * - Padding to ensure 8-byte alignment
     * - The actual data body containing the record batch buffers
     *
     * The serialization follows the Arrow IPC stream format where each record batch message
     * consists of a metadata section followed by a body section containing the actual data.
     *
     * @param record_batch The sparrow record batch to be serialized
     * @return std::vector<uint8_t> A byte vector containing the complete serialized record batch
     *         in Arrow IPC format, ready for transmission or storage
     */
    [[nodiscard]] SPARROW_IPC_API std::vector<uint8_t>
    serialize_record_batch(const sparrow::record_batch& record_batch);

    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    /**
     * @brief Serializes a collection of record batches into a single byte vector.
     *
     * This function takes a range or container of record batches and serializes each one
     * individually, then concatenates all the serialized data into a single output vector.
     * The serialization is performed by calling serialize_record_batch() for each record batch
     * in the input collection.
     *
     * @tparam R The type of the record batch container/range (must be iterable)
     * @param record_batches A collection of record batches to be serialized
     * @return std::vector<uint8_t> A byte vector containing the serialized data of all record batches
     *
     * @note The function uses move iterators to efficiently transfer the serialized data
     *       from individual record batches to the output vector.
     */
    [[nodiscard]] std::vector<uint8_t> serialize_record_batches(const R& record_batches)
    {
        std::vector<uint8_t> output;
        for (const auto& record_batch : record_batches)
        {
            const auto rb_serialized = serialize_record_batch(record_batch);
            output.insert(
                output.end(),
                std::make_move_iterator(rb_serialized.begin()),
                std::make_move_iterator(rb_serialized.end())
            );
        }
        return output;
    }

    /**
     * @brief Creates a FlatBuffers vector of KeyValue pairs from ArrowSchema metadata.
     *
     * This function converts metadata from an ArrowSchema into a FlatBuffers representation
     * suitable for serialization. It processes key-value pairs from the schema's metadata
     * and creates corresponding FlatBuffers KeyValue objects.
     *
     * @param builder Reference to the FlatBufferBuilder used for creating FlatBuffers objects
     * @param arrow_schema The ArrowSchema containing metadata to be serialized
     *
     * @return A FlatBuffers offset to a vector of KeyValue pairs. Returns 0 if the schema
     *         has no metadata (metadata is nullptr).
     *
     * @note The function reserves memory for the vector based on the metadata size for
     *       optimal performance.
     */
    [[nodiscard]] SPARROW_IPC_API
        flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>>
        create_metadata(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema);

    /**
     * @brief Creates a FlatBuffer Field object from an ArrowSchema.
     *
     * This function converts an ArrowSchema structure into a FlatBuffer Field representation
     * suitable for Apache Arrow IPC serialization. It handles the creation of all necessary
     * components including field name, type information, metadata, children, and nullable flag.
     *
     * @param builder Reference to the FlatBufferBuilder used for creating FlatBuffer objects
     * @param arrow_schema The ArrowSchema structure containing the field definition to convert
     *
     * @return A FlatBuffer offset to the created Field object that can be used in further
     *         FlatBuffer construction operations
     *
     * @note Dictionary encoding is not currently supported (TODO item)
     * @note The function checks the NULLABLE flag from the ArrowSchema flags to determine nullability
     */
    [[nodiscard]] SPARROW_IPC_API ::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>
    create_field(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema);

    /**
     * @brief Creates a FlatBuffers vector of Field objects from an ArrowSchema's children.
     *
     * This function iterates through all children of the given ArrowSchema and converts
     * each child to a FlatBuffers Field object. The resulting fields are collected into
     * a FlatBuffers vector.
     *
     * @param builder Reference to the FlatBufferBuilder used for creating FlatBuffers objects
     * @param arrow_schema The ArrowSchema containing the children to convert
     *
     * @return A FlatBuffers offset to a vector of Field objects, or 0 if no children exist
     *
     * @throws std::invalid_argument If any child pointer in the ArrowSchema is null
     *
     * @note The function reserves space for all children upfront for performance optimization
     * @note Returns 0 (null offset) when the schema has no children, otherwise returns a valid vector offset
     */
    [[nodiscard]] SPARROW_IPC_API ::flatbuffers::Offset<
        ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>>>
    create_children(flatbuffers::FlatBufferBuilder& builder, sparrow::record_batch::column_range columns);

    /**
     * @brief Creates a FlatBuffers vector of Field objects from a range of columns.
     *
     * This function iterates through the provided column range, extracts the Arrow schema
     * from each column's proxy, and creates corresponding FlatBuffers Field objects.
     * The resulting fields are collected into a vector and converted to a FlatBuffers
     * vector offset.
     *
     * @param builder Reference to the FlatBuffers builder used for creating the vector
     * @param columns Range of columns to process, each containing an Arrow schema proxy
     *
     * @return FlatBuffers offset to a vector of Field objects, or 0 if the input range is empty
     *
     * @note The function reserves space in the children vector based on the column count
     *       for performance optimization
     */
    [[nodiscard]] SPARROW_IPC_API ::flatbuffers::Offset<
        ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>>>
    create_children(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema);

    /**
     * @brief Creates a FlatBuffer builder containing a serialized Arrow schema message.
     *
     * This function constructs an Arrow IPC schema message from a record batch by:
     * 1. Creating field definitions from the record batch columns
     * 2. Building a Schema flatbuffer with little-endian byte order
     * 3. Wrapping the schema in a Message with metadata version V5
     * 4. Finalizing the buffer for serialization
     *
     * @param record_batch The source record batch containing column definitions
     * @return flatbuffers::FlatBufferBuilder A completed FlatBuffer containing the schema message,
     *         ready for Arrow IPC serialization
     *
     * @note The schema message has zero body length as it contains only metadata
     * @note Currently uses little-endian byte order (marked as TODO for configurability)
     */
    [[nodiscard]] SPARROW_IPC_API flatbuffers::FlatBufferBuilder
    get_schema_message_builder(const sparrow::record_batch& record_batch);

    /**
     * @brief Serializes a schema message for a record batch into a byte buffer.
     *
     * This function creates a serialized schema message following the Arrow IPC format.
     * The resulting buffer contains:
     * 1. Continuation bytes at the beginning
     * 2. A 4-byte length prefix indicating the size of the schema message
     * 3. The actual FlatBuffer schema message bytes
     * 4. Padding bytes to align the total size to 8-byte boundaries
     *
     * @param record_batch The record batch containing the schema to serialize
     * @return std::vector<uint8_t> A byte buffer containing the complete serialized schema message
     */
    [[nodiscard]] SPARROW_IPC_API std::vector<uint8_t>
    serialize_schema_message(const sparrow::record_batch& record_batch);

    /**
     * @brief Recursively fills a vector of FieldNode objects from an arrow_proxy and its children.
     *
     * This function creates FieldNode objects containing length and null count information
     * from the given arrow_proxy and recursively processes all its children, appending
     * them to the provided nodes vector in depth-first order.
     *
     * @param arrow_proxy The arrow proxy object containing array metadata (length, null_count)
     *                    and potential child arrays
     * @param nodes Reference to a vector that will be populated with FieldNode objects.
     *              Each FieldNode contains the length and null count of the corresponding array.
     *
     * @note The function reserves space in the nodes vector to optimize memory allocation
     *       when processing children arrays.
     * @note The traversal order is depth-first, with parent nodes added before their children.
     */
    SPARROW_IPC_API void fill_fieldnodes(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::FieldNode>& nodes
    );

    /**
     * @brief Creates a vector of Apache Arrow FieldNode objects from a record batch.
     *
     * This function iterates through all columns in the provided record batch and
     * generates corresponding FieldNode flatbuffer objects. Each column's arrow proxy
     * is used to populate the field nodes vector through the fill_fieldnodes function.
     *
     * @param record_batch The sparrow record batch containing columns to process
     * @return std::vector<org::apache::arrow::flatbuf::FieldNode> Vector of FieldNode
     *         objects representing the structure and metadata of each column
     */
    [[nodiscard]] SPARROW_IPC_API std::vector<org::apache::arrow::flatbuf::FieldNode>
    create_fieldnodes(const sparrow::record_batch& record_batch);

    /**
     * @brief Recursively fills a vector of FlatBuffer Buffer objects with buffer information from an Arrow
     * proxy.
     *
     * This function traverses an Arrow proxy structure and creates FlatBuffer Buffer entries for each buffer
     * found in the proxy and its children. The buffers are processed in a depth-first manner, first handling
     * the buffers of the current proxy, then recursively processing all child proxies.
     *
     * @param arrow_proxy The Arrow proxy object containing buffers and potential child proxies to process
     * @param flatbuf_buffers Vector of FlatBuffer Buffer objects to be populated with buffer information
     * @param offset Reference to the current byte offset, updated as buffers are processed and aligned to
     * 8-byte boundaries
     *
     * @note The offset is automatically aligned to 8-byte boundaries using utils::align_to_8() for each
     * buffer
     * @note This function modifies both the flatbuf_buffers vector and the offset parameter
     */
    SPARROW_IPC_API void fill_buffers(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::Buffer>& flatbuf_buffers,
        int64_t& offset
    );

    /**
     * @brief Extracts buffer information from a record batch for serialization.
     *
     * This function iterates through all columns in the provided record batch and
     * collects their buffer information into a vector of Arrow FlatBuffer Buffer objects.
     * The buffers are processed sequentially with cumulative offset tracking.
     *
     * @param record_batch The sparrow record batch containing columns to extract buffers from
     * @return std::vector<org::apache::arrow::flatbuf::Buffer> A vector containing all buffer
     *         descriptors from the record batch columns, with properly calculated offsets
     *
     * @note This function relies on the fill_buffers helper function to process individual
     *       column buffers and maintain offset consistency across all buffers.
     */
    [[nodiscard]] SPARROW_IPC_API std::vector<org::apache::arrow::flatbuf::Buffer>
    get_buffers(const sparrow::record_batch& record_batch);

    /**
     * @brief Fills the body vector with buffer data from an arrow proxy and its children.
     *
     * This function recursively processes an arrow proxy by:
     * 1. Iterating through all buffers in the proxy and appending their data to the body vector
     * 2. Adding padding bytes (zeros) after each buffer to align data to 8-byte boundaries
     * 3. Recursively processing all child proxies in the same manner
     *
     * The function ensures proper memory alignment by padding each buffer's data to the next
     * 8-byte boundary, which is typically required for efficient memory access and Arrow
     * format compliance.
     *
     * @param arrow_proxy The arrow proxy containing buffers and potential child proxies to serialize
     * @param body Reference to the vector where the serialized buffer data will be appended
     */
    SPARROW_IPC_API void fill_body(const sparrow::arrow_proxy& arrow_proxy, std::vector<uint8_t>& body);

    /**
     * @brief Generates a serialized body from a record batch.
     *
     * This function iterates through all columns in the provided record batch,
     * extracts their Arrow proxy representations, and serializes them into a
     * single byte vector that forms the body of the serialized data.
     *
     * @param record_batch The record batch containing columns to be serialized
     * @return std::vector<uint8_t> A byte vector containing the serialized body data
     */
    [[nodiscard]] SPARROW_IPC_API std::vector<uint8_t> generate_body(const sparrow::record_batch& record_batch);

    /**
     * @brief Calculates the total size of the body section for an Arrow array.
     *
     * This function recursively computes the total size needed for all buffers
     * in an Arrow array structure, including buffers from child arrays. Each
     * buffer size is aligned to 8-byte boundaries as required by the Arrow format.
     *
     * @param arrow_proxy The Arrow array proxy containing buffers and child arrays
     * @return int64_t The total aligned size in bytes of all buffers in the array hierarchy
     */
    [[nodiscard]] SPARROW_IPC_API int64_t calculate_body_size(const sparrow::arrow_proxy& arrow_proxy);

    /**
     * @brief Calculates the total body size of a record batch by summing the body sizes of all its columns.
     *
     * This function iterates through all columns in the given record batch and accumulates
     * the body size of each column's underlying Arrow array proxy. The body size represents
     * the total memory required for the serialized data content of the record batch.
     *
     * @param record_batch The sparrow record batch containing columns to calculate size for
     * @return int64_t The total body size in bytes of all columns in the record batch
     */
    [[nodiscard]] SPARROW_IPC_API int64_t calculate_body_size(const sparrow::record_batch& record_batch);

    /**
     * @brief Creates a FlatBuffer message containing a serialized Apache Arrow RecordBatch.
     *
     * This function builds a complete Arrow IPC message by serializing a record batch
     * along with its metadata (field nodes and buffer information) into a FlatBuffer
     * format that conforms to the Arrow IPC specification.
     *
     * @param record_batch The source record batch containing the data to be serialized
     * @param nodes Vector of field nodes describing the structure and null counts of columns
     * @param buffers Vector of buffer descriptors containing offset and length information
     *                for the data buffers
     *
     * @return A FlatBufferBuilder containing the complete serialized message ready for
     *         transmission or storage. The builder is finished and ready to be accessed
     *         via GetBufferPointer() and GetSize().
     *
     * @note The returned message uses Arrow IPC format version V5
     * @note Compression and variadic buffer counts are not currently implemented (set to 0)
     * @note The body size is automatically calculated based on the record batch contents
     */
    [[nodiscard]] SPARROW_IPC_API flatbuffers::FlatBufferBuilder get_record_batch_message_builder(
        const sparrow::record_batch& record_batch,
        const std::vector<org::apache::arrow::flatbuf::FieldNode>& nodes,
        const std::vector<org::apache::arrow::flatbuf::Buffer>& buffers
    );

    /**
     * @brief Serializes a record batch into a binary format following the Arrow IPC specification.
     *
     * This function converts a sparrow record batch into a serialized byte vector that includes:
     * - A continuation marker
     * - The record batch message length (4 bytes)
     * - The flatbuffer-encoded record batch metadata
     * - Padding to align to 8-byte boundaries
     * - The record batch body containing the actual data buffers
     *
     * @param record_batch The sparrow record batch to serialize
     * @return std::vector<uint8_t> A byte vector containing the serialized record batch
     *         in Arrow IPC format, ready for transmission or storage
     *
     * @note The output follows Arrow IPC message format with proper alignment and
     *       includes both metadata and data portions of the record batch
     */
    [[nodiscard]] SPARROW_IPC_API std::vector<uint8_t>
    serialize_record_batch(const sparrow::record_batch& record_batch);

    /**
     * @brief Adds padding bytes to a buffer to ensure 8-byte alignment.
     *
     * This function appends zero bytes to the end of the provided buffer until
     * its size is a multiple of 8. This is often required for proper memory
     * alignment in binary formats such as Apache Arrow IPC.
     *
     * @param buffer The byte vector to which padding will be added
     */
    void add_padding(std::vector<uint8_t>& buffer);
}
