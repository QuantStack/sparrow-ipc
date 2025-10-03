#pragma once
#include <flatbuffers/flatbuffers.h>
#include <Message_generated.h>

#include <sparrow/c_interface.hpp>
#include <sparrow/record_batch.hpp>

namespace sparrow_ipc
{
    // Creates a Flatbuffers Decimal type from a format string
    // The format string is expected to be in the format "d:precision,scale"
    [[nodiscard]] std::pair<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>>
    get_flatbuffer_decimal_type(
        flatbuffers::FlatBufferBuilder& builder,
        std::string_view format_str,
        const int32_t bitWidth
    );

    // Creates a Flatbuffers type from a format string
    // This function maps a sparrow data type to the corresponding Flatbuffers type
    [[nodiscard]] std::pair<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>>
    get_flatbuffer_type(flatbuffers::FlatBufferBuilder& builder, std::string_view format_str);

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
    [[nodiscard]] flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>>
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
    [[nodiscard]] ::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>
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
    [[nodiscard]] ::flatbuffers::Offset<
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
    [[nodiscard]] ::flatbuffers::Offset<
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
    [[nodiscard]] flatbuffers::FlatBufferBuilder
    get_schema_message_builder(const sparrow::record_batch& record_batch);

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
    void fill_fieldnodes(
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
    [[nodiscard]] std::vector<org::apache::arrow::flatbuf::FieldNode>
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
    void fill_buffers(
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
    [[nodiscard]] std::vector<org::apache::arrow::flatbuf::Buffer>
    get_buffers(const sparrow::record_batch& record_batch);

    /**
     * @brief Creates a FlatBuffer message containing a serialized Apache Arrow RecordBatch.
     *
     * This function builds a complete Arrow IPC message by serializing a record batch
     * along with its metadata (field nodes and buffer information) into a FlatBuffer
     * format that conforms to the Arrow IPC specification.
     *
     * @param record_batch The source record batch containing the data to be serialized
     *
     * @return A FlatBufferBuilder containing the complete serialized message ready for
     *         transmission or storage. The builder is finished and ready to be accessed
     *         via GetBufferPointer() and GetSize().
     *
     * @note The returned message uses Arrow IPC format version V5
     * @note Compression and variadic buffer counts are not currently implemented (set to 0)
     * @note The body size is automatically calculated based on the record batch contents
     */
    [[nodiscard]] flatbuffers::FlatBufferBuilder
    get_record_batch_message_builder(const sparrow::record_batch& record_batch);
}