#include "sparrow_ipc/deserialize.hpp"

#include <sparrow/types/data_type.hpp>

#include "sparrow_ipc/deserialize_fixedsizebinary_array.hpp"
#include "sparrow_ipc/deserialize_primitive_array.hpp"
#include "sparrow_ipc/deserialize_variable_size_binary_array.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/metadata.hpp"

namespace sparrow_ipc
{
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
        const encapsulated_message& encapsulated_message,
        const std::vector<std::optional<std::vector<sparrow::metadata_pair>>>& field_metadata
    )
    {
        size_t buffer_index = 0;

        std::vector<sparrow::array> arrays;
        arrays.reserve(schema.fields()->size());
        size_t field_idx = 0;
        for (const auto field : *(schema.fields()))
        {
            const ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>*
                fb_custom_metadata = field->custom_metadata();
            const std::optional<std::vector<sparrow::metadata_pair>>& metadata = field_metadata[field_idx++];
            const std::string name = field->name() == nullptr ? "" : field->name()->str();
            const auto field_type = field->type_type();
            // TODO rename all the deserialize_non_owning... fcts since this is not correct anymore
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
        std::vector<std::string> field_names;
        std::vector<bool> fields_nullable;
        std::vector<sparrow::data_type> field_types;
        std::vector<std::optional<std::vector<sparrow::metadata_pair>>> fields_metadata;
        do
        {
            // Check for end-of-stream marker here as data could contain only that (if no record batches present/written)
            if (data.size() >= 8 && is_end_of_stream(data.subspan(0, 8)))
            {
                break;
            }

            const auto [encapsulated_message, rest] = extract_encapsulated_message(data);
            const org::apache::arrow::flatbuf::Message* message = encapsulated_message.flat_buffer_message();

            if (message == nullptr)
            {
                throw std::invalid_argument("Extracted flatbuffers message is null.");
            }

            switch (message->header_type())
            {
                case org::apache::arrow::flatbuf::MessageHeader::Schema:
                {
                    schema = message->header_as_Schema();
                    const size_t size = static_cast<size_t>(schema->fields()->size());
                    field_names.reserve(size);
                    fields_nullable.reserve(size);
                    fields_metadata.reserve(size);

                    for (const auto field : *(schema->fields()))
                    {
                        if(field != nullptr && field->name() != nullptr)
                        {
                           field_names.emplace_back(field->name()->str());
                        }
                        else {
                            field_names.emplace_back("_unnamed_");
                        }
                        fields_nullable.push_back(field->nullable());
                        const ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>*
                            fb_custom_metadata = field->custom_metadata();
                        std::optional<std::vector<sparrow::metadata_pair>>
                            metadata = fb_custom_metadata == nullptr
                                           ? std::nullopt
                                           : std::make_optional(to_sparrow_metadata(*fb_custom_metadata));
                        fields_metadata.push_back(std::move(metadata));
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
                        encapsulated_message,
                        fields_metadata
                    );
                    auto names_copy = field_names; // TODO: Remove when issue with the to_vector of record_batch is fixed
                    sparrow::record_batch sp_record_batch(std::move(names_copy), std::move(arrays));
                    record_batches.emplace_back(std::move(sp_record_batch));
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
        } while (!data.empty());
        return record_batches;
    }
}
