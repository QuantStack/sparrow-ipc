#include "sparrow_ipc/encapsulated_message.hpp"

#include <stdexcept>

#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    EncapsulatedMessage::EncapsulatedMessage(const uint8_t* buf_ptr)
        : m_buf_ptr(buf_ptr)
    {
    }

    const org::apache::arrow::flatbuf::Message* EncapsulatedMessage::flat_buffer_message() const
    {
        const uint8_t* message_ptr = m_buf_ptr + (sizeof(uint32_t) * 2);  // 4 bytes continuation + 4 bytes
                                                                          // metadata size
        return org::apache::arrow::flatbuf::GetMessage(message_ptr);
    }

    size_t EncapsulatedMessage::metadata_length() const
    {
        return *(reinterpret_cast<const uint32_t*>(m_buf_ptr + sizeof(uint32_t)));
    }

    [[nodiscard]] std::variant<
        const org::apache::arrow::flatbuf::Schema*,
        const org::apache::arrow::flatbuf::RecordBatch*,
        const org::apache::arrow::flatbuf::Tensor*,
        const org::apache::arrow::flatbuf::DictionaryBatch*,
        const org::apache::arrow::flatbuf::SparseTensor*>
    EncapsulatedMessage::metadata() const
    {
        const auto schema_message = flat_buffer_message();
        switch (schema_message->header_type())
        {
            case org::apache::arrow::flatbuf::MessageHeader::Schema:
            {
                return schema_message->header_as_Schema();
            }
            case org::apache::arrow::flatbuf::MessageHeader::RecordBatch:
            {
                return schema_message->header_as_RecordBatch();
            }
            case org::apache::arrow::flatbuf::MessageHeader::Tensor:
            {
                return schema_message->header_as_Tensor();
            }
            case org::apache::arrow::flatbuf::MessageHeader::DictionaryBatch:
            {
                return schema_message->header_as_DictionaryBatch();
            }
            case org::apache::arrow::flatbuf::MessageHeader::SparseTensor:
            {
                return schema_message->header_as_SparseTensor();
            }
            default:
                throw std::runtime_error("Unknown message header type.");
        }
    }

    const ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>*
    EncapsulatedMessage::custom_metadata() const
    {
        return flat_buffer_message()->custom_metadata();
    }

    size_t EncapsulatedMessage::body_length() const
    {
        return static_cast<size_t>(flat_buffer_message()->bodyLength());
    }

    std::span<const uint8_t> EncapsulatedMessage::body() const
    {
        const size_t offset = sizeof(uint32_t) * 2  // 4 bytes continuation + 4 bytes metadata size
                              + metadata_length();
        const size_t padded_offset = utils::align_to_8(offset);  // Round up to 8-byte boundary
        const uint8_t* body_ptr = m_buf_ptr + padded_offset;
        return {body_ptr, body_length()};
    }

    size_t EncapsulatedMessage::total_length() const
    {
        const size_t offset = sizeof(uint32_t) * 2  // 4 bytes continuation + 4 bytes metadata size
                              + metadata_length();
        const size_t padded_offset = utils::align_to_8(offset);  // Round up to 8-byte boundary
        return padded_offset + body_length();
    }

    std::span<const uint8_t> EncapsulatedMessage::as_span() const
    {
        return {m_buf_ptr, total_length()};
    }

    EncapsulatedMessage create_encapsulated_message(const uint8_t* buf_ptr)
    {
        if (!buf_ptr)
        {
            throw std::invalid_argument("Buffer pointer cannot be null.");
        }
        const std::span<const uint8_t> continuation_span(buf_ptr, 4);
        if (!is_continuation(continuation_span))
        {
            throw std::runtime_error("Buffer starts with continuation bytes, expected a valid message.");
        }
        return {buf_ptr};
    }
}