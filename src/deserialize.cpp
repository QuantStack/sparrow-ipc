#include "sparrow_ipc/deserialize.hpp"

#include <sparrow/types/data_type.hpp>

#include "sparrow_ipc/deserialize_fixedsizebinary_array.hpp"
#include "sparrow_ipc/deserialize_primitive_array.hpp"
#include "sparrow_ipc/deserialize_variable_size_binary_array.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/metadata.hpp"

namespace sparrow_ipc
{
    void deserialize_schema_message(
        std::span<const uint8_t> data,
        size_t& current_offset,
        std::optional<std::string>& name,
        std::optional<std::vector<sparrow::metadata_pair>>& metadata
    )
    {
        const uint32_t schema_meta_len = *(reinterpret_cast<const uint32_t*>(data.data() + current_offset));
        current_offset += sizeof(uint32_t);
        const auto schema_message = org::apache::arrow::flatbuf::GetMessage(data.data() + current_offset);
        if (schema_message->header_type() != org::apache::arrow::flatbuf::MessageHeader::Schema)
        {
            throw std::runtime_error("Expected Schema message at the start of the buffer.");
        }
        const auto flatbuffer_schema = static_cast<const org::apache::arrow::flatbuf::Schema*>(
            schema_message->header()
        );
        const auto fields = flatbuffer_schema->fields();
        if (fields->size() != 1)
        {
            throw std::runtime_error("Expected schema with exactly one field.");
        }

        const auto field = fields->Get(0);

        // Get name
        if (const auto fb_name = field->name())
        {
            name = fb_name->str();
        }

        // Handle metadata
        const auto fb_metadata = field->custom_metadata();
        if (fb_metadata && !fb_metadata->empty())
        {
            metadata = std::vector<sparrow::metadata_pair>();
            metadata->reserve(fb_metadata->size());
            for (const auto& kv : *fb_metadata)
            {
                metadata->emplace_back(kv->key()->str(), kv->value()->str());
            }
        }
        current_offset += schema_meta_len;
    }

    const org::apache::arrow::flatbuf::RecordBatch*
    deserialize_record_batch_message(std::span<const uint8_t> data, size_t& current_offset)
    {
        current_offset += sizeof(uint32_t);
        const auto batch_message = org::apache::arrow::flatbuf::GetMessage(data.data() + current_offset);
        if (batch_message->header_type() != org::apache::arrow::flatbuf::MessageHeader::RecordBatch)
        {
            throw std::runtime_error("Expected RecordBatch message, but got a different type.");
        }
        return static_cast<const org::apache::arrow::flatbuf::RecordBatch*>(batch_message->header());
    }

    std::vector<sparrow::array> get_arrays_from_record_batch(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        const org::apache::arrow::flatbuf::Schema& schema,
        const EncapsulatedMessage& encapsulated_message
    )
    {
        const size_t length = static_cast<size_t>(record_batch.length());
        size_t buffer_index = 0;

        std::vector<sparrow::array> arrays;
        arrays.reserve(schema.fields()->size());

        for (const auto field : *(schema.fields()))
        {
            const ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>*
                fb_custom_metadata = field->custom_metadata();
            const std::optional<std::vector<sparrow::metadata_pair>>
                metadata = fb_custom_metadata == nullptr
                               ? std::nullopt
                               : std::make_optional(to_sparrow_metadata(*fb_custom_metadata));
            const auto name = field->name()->string_view();
            const auto field_type = field->type_type();
            const auto deserialize_non_owning_primitive_array_lambda = [&]<typename T>()
            {
                return deserialize_non_owning_primitive_array<T>(
                    record_batch,
                    encapsulated_message.body(),
                    name,
                    metadata,
                    buffer_index
                );
            };
            switch (field_type)
            {
                case org::apache::arrow::flatbuf::Type::Bool:
                    arrays.emplace_back(
                        deserialize_non_owning_primitive_array_lambda.template operator()<bool>()
                    );
                    break;
                case org::apache::arrow::flatbuf::Type::Int:
                {
                    const auto int_type = field->type_as_Int();
                    const auto bit_width = int_type->bitWidth();
                    const bool is_signed = int_type->is_signed();

                    if (is_signed)
                    {
                        switch (bit_width)
                        {
                                // clang-format off
                                        case 8:  arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<int8_t>()); break;
                                        case 16: arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<int16_t>()); break;
                                        case 32: arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<int32_t>()); break;
                                        case 64: arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<int64_t>()); break;
                                        default: throw std::runtime_error("Unsupported integer bit width.");
                                // clang-format on
                        }
                    }
                    else
                    {
                        switch (bit_width)
                        {
                                // clang-format off
                                        case 8:  arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<uint8_t>()); break;
                                        case 16: arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<uint16_t>()); break;
                                        case 32: arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<uint32_t>()); break;
                                        case 64: arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<uint64_t>()); break;
                                        default: throw std::runtime_error("Unsupported integer bit width.");
                                // clang-format on
                        }
                    }
                }
                break;
                case org::apache::arrow::flatbuf::Type::FloatingPoint:
                {
                    const auto float_type = field->type_as_FloatingPoint();
                    switch (float_type->precision())
                    {
                            // clang-format off
                                    case org::apache::arrow::flatbuf::Precision::HALF:
                                        arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<sparrow::float16_t>());
                                        break;
                                    case org::apache::arrow::flatbuf::Precision::SINGLE:
                                        arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<float>());
                                        break;
                                    case org::apache::arrow::flatbuf::Precision::DOUBLE:
                                        arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<double>());
                                        break;
                                    default:
                                        throw std::runtime_error("Unsupported floating point precision.");
                            // clang-format on
                    }
                    break;
                }
                case org::apache::arrow::flatbuf::Type::FixedSizeBinary:
                {
                    const auto fixed_size_binary_field = field->type_as_FixedSizeBinary();
                    arrays.emplace_back(deserialize_non_owning_fixedwidthbinary(
                        record_batch,
                        encapsulated_message.body(),
                        name,
                        metadata,
                        buffer_index,
                        fixed_size_binary_field->byteWidth()
                    ));
                    break;
                }
                case org::apache::arrow::flatbuf::Type::Binary:
                    arrays.emplace_back(
                        deserialize_non_owning_variable_size_binary<sparrow::binary_array>(
                            record_batch,
                            encapsulated_message.body(),
                            name,
                            metadata,
                            buffer_index
                        )
                    );
                    break;
                case org::apache::arrow::flatbuf::Type::LargeBinary:
                    arrays.emplace_back(
                        deserialize_non_owning_variable_size_binary<sparrow::big_binary_array>(
                            record_batch,
                            encapsulated_message.body(),
                            name,
                            metadata,
                            buffer_index
                        )
                    );
                    break;
                case org::apache::arrow::flatbuf::Type::Utf8:
                    arrays.emplace_back(
                        deserialize_non_owning_variable_size_binary<sparrow::string_array>(
                            record_batch,
                            encapsulated_message.body(),
                            name,
                            metadata,
                            buffer_index
                        )
                    );
                    break;
                case org::apache::arrow::flatbuf::Type::LargeUtf8:
                    arrays.emplace_back(
                        deserialize_non_owning_variable_size_binary<sparrow::big_string_array>(
                            record_batch,
                            encapsulated_message.body(),
                            name,
                            metadata,
                            buffer_index
                        )
                    );
                    break;
                default:
                    throw std::runtime_error("Unsupported type.");
            }
        }
        return arrays;
    }

    std::vector<sparrow::record_batch> deserialize_stream(std::span<const uint8_t> data)
    {
        const org::apache::arrow::flatbuf::Schema* schema = nullptr;
        std::vector<sparrow::record_batch> record_batches;
        std::vector<std::string_view> field_names;
        std::vector<bool> fields_nullable;
        std::vector<sparrow::data_type> field_types;
        do
        {
            const auto [encapsulated_message, rest] = extract_encapsulated_message(data);
            const org::apache::arrow::flatbuf::Message* message = encapsulated_message.flat_buffer_message();
            switch (message->header_type())
            {
                case org::apache::arrow::flatbuf::MessageHeader::Schema:
                {
                    schema = message->header_as_Schema();
                    const size_t size = static_cast<size_t>(schema->fields()->size());
                    field_names.reserve(size);
                    fields_nullable.reserve(size);

                    for (const auto field : *(schema->fields()))
                    {
                        field_names.emplace_back(field->name()->string_view());
                        fields_nullable.push_back(field->nullable());
                    }
                }
                break;
                case org::apache::arrow::flatbuf::MessageHeader::RecordBatch:
                {
                    if (schema == nullptr)
                    {
                        throw std::runtime_error("Schema message is missing.");
                    }
                    const auto record_batch = message->header_as_RecordBatch();
                    if (record_batch == nullptr)
                    {
                        throw std::runtime_error("RecordBatch message is missing.");
                    }
                    std::vector<sparrow::array> arrays = get_arrays_from_record_batch(
                        *record_batch,
                        *schema,
                        encapsulated_message
                    );
                    std::vector<std::string> field_names_str(field_names.cbegin(), field_names.cend());
                    record_batches.emplace_back(std::move(field_names_str), std::move(arrays), "test");
                }
                break;
                case org::apache::arrow::flatbuf::MessageHeader::Tensor:
                case org::apache::arrow::flatbuf::MessageHeader::DictionaryBatch:
                case org::apache::arrow::flatbuf::MessageHeader::SparseTensor:
                    throw std::runtime_error("Not supported");
                default:
                    throw std::runtime_error("Unknown message header type.");
            }
            data = rest;
            if (is_end_of_stream(data.subspan(0, 8)))
            {
                break;
            }
        } while (true);
        return record_batches;
    }
}