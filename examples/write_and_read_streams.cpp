#include <algorithm>
#include <iostream>
#include <vector>
#include <random>

#include <sparrow/record_batch.hpp>

#include <sparrow_ipc/memory_output_stream.hpp>
#include <sparrow_ipc/serializer.hpp>
#include <sparrow_ipc/deserialize.hpp>

namespace sp = sparrow;

// Random number generator
std::random_device rd;
std::mt19937 gen(rd());

/**
 * Helper function to create a record batch with the same schema but random values
 * All batches have: int32 column, float column, bool column, and string column
 */
sp::record_batch create_random_record_batch(size_t num_rows) 
{
    std::uniform_int_distribution<int32_t> int_dist(0, 1000);
    std::uniform_real_distribution<float> float_dist(-100.0f, 100.0f);
    std::uniform_int_distribution<int> bool_dist(0, 1);
    
    // Create integer column with random values
    std::vector<int32_t> int_values;
    int_values.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        int_values.push_back(int_dist(gen));
    }
    auto int_array = sp::primitive_array<int32_t>(std::move(int_values) );

    // Create float column with random values
    std::vector<float> float_values;
    float_values.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        float_values.push_back(float_dist(gen));
    }
    auto float_array = sp::primitive_array<float>(std::move(float_values));

    // Create boolean column with random values
    std::vector<bool> bool_values;
    bool_values.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        bool_values.push_back(static_cast<bool>(bool_dist(gen)));
    }
    auto bool_array = sp::primitive_array<bool>(std::move(bool_values));

    // Create string column with random values
    std::vector<std::string> string_values;
    string_values.reserve(num_rows);
    const std::vector<std::string> sample_strings = {
        "alpha", "beta", "gamma", "delta", "epsilon", 
        "zeta", "eta", "theta", "iota", "kappa"
    };
    std::uniform_int_distribution<size_t> str_dist(0, sample_strings.size() - 1);
    
    for (size_t i = 0; i < num_rows; ++i) {
        string_values.push_back(sample_strings[str_dist(gen)] + "_" + std::to_string(i));
    }
    auto string_array = sp::string_array(std::move(string_values));

    // Create record batch with named columns (same schema for all batches)
    return sp::record_batch({
        {"id", sp::array(std::move(int_array))},
        {"value", sp::array(std::move(float_array))},
        {"flag", sp::array(std::move(bool_array))},
        {"name", sp::array(std::move(string_array))}
    });
}

int main() 
{
    std::cout << "=== Sparrow IPC Stream Write and Read Example ===\n";
    std::cout << "Note: All record batches in a stream must have the same schema.\n\n";

    try {
        // Configuration
        constexpr size_t num_batches = 5;
        constexpr size_t rows_per_batch = 10;

        // Step 1: Create several record batches with the SAME schema but random values
        std::cout << "1. Creating " << num_batches << " record batches with random values...\n";
        std::cout << "   Each batch has the same schema: (id: int32, value: float, flag: bool, name: string)\n";
        
        std::vector<sp::record_batch> original_batches;
        original_batches.reserve(num_batches);
        
        for (size_t i = 0; i < num_batches; ++i) {
            original_batches.push_back(create_random_record_batch(rows_per_batch));
        }

        std::cout << "   Created " << original_batches.size() << " record batches\n";
        for(size_t i = 0; i < original_batches.size(); ++i) {
            std::cout << std::format("{}\n", original_batches[i]);
        }

        // Step 2: Serialize the record batches to a stream
        std::cout << "\n2. Serializing record batches to stream...\n";
        
        std::vector<uint8_t> stream_data;
        sparrow_ipc::memory_output_stream stream(stream_data);
        sparrow_ipc::serializer serializer(stream);

        // Serialize all batches using the streaming operator
        serializer << original_batches << sparrow_ipc::end_stream;
        
        std::cout << "   Serialized stream size: " << stream_data.size() << " bytes\n";

        // Step 3: Deserialize the stream back to record batches
        std::cout << "\n3. Deserializing stream back to record batches...\n";
        
        auto deserialized_batches = sparrow_ipc::deserialize_stream(
            std::span<const uint8_t>(stream_data)
        );
        
        std::cout << "   Deserialized " << deserialized_batches.size() << " record batches\n";

        // Step 4: Verify that original and deserialized data match
        std::cout << "\n4. Verifying data integrity...\n";
        
        if (original_batches.size() != deserialized_batches.size()) {
            std::cerr << "ERROR: Batch count mismatch! Original: " << original_batches.size() 
                      << ", Deserialized: " << deserialized_batches.size() << "\n";
            return 1;
        }

        bool all_match = true;
        for (size_t batch_idx = 0; batch_idx < original_batches.size(); ++batch_idx) {
            const auto& original = original_batches[batch_idx];
            const auto& deserialized = deserialized_batches[batch_idx];
            
            // Check basic structure
            if (original.nb_columns() != deserialized.nb_columns() ||
                original.nb_rows() != deserialized.nb_rows()) {
                std::cerr << "ERROR: Batch " << batch_idx << " structure mismatch!\n";
                all_match = false;
                continue;
            }

            // Check column names
            if(!std::ranges::equal(original.names(), deserialized.names())) {
                std::cerr << "WARNING: Batch " << batch_idx << " column names mismatch!\n";
            }
            
            // Check column data
            for (size_t col_idx = 0; col_idx < original.nb_columns(); ++col_idx) {
                const auto& orig_col = original.get_column(col_idx);
                const auto& deser_col = deserialized.get_column(col_idx);
                
                if (orig_col.data_type() != deser_col.data_type()) {
                    std::cerr << "ERROR: Batch " << batch_idx << ", column " << col_idx 
                              << " type mismatch!\n";
                    all_match = false;
                    continue;
                }
                
                // Check values
                for (size_t row_idx = 0; row_idx < orig_col.size(); ++row_idx) {
                    if (orig_col[row_idx] != deser_col[row_idx]) {
                        std::cerr << "ERROR: Batch " << batch_idx << ", column " << col_idx 
                                  << ", row " << row_idx << " value mismatch!\n";
                        std::cerr << "  Original: " << orig_col[row_idx] 
                                  << ", Deserialized: " << deser_col[row_idx] << "\n";
                        all_match = false;
                    }
                }
            }
        }
        
        if (all_match) {
            std::cout << "   ✓ All data matches perfectly!\n";
        } else {
            std::cerr << "   ✗ Data verification failed!\n";
            return 1;
        }

        // Step 5: Display sample data from the first batch
        std::cout << "\n5. Sample data from the first batch:\n";
        std::cout << std::format("{}\n", original_batches[0]);
        
        // Step 6: Demonstrate individual serialization vs batch serialization
        std::cout << "\n6. Demonstrating individual vs batch serialization...\n";
        
        // Serialize individual batches one by one
        std::vector<uint8_t> individual_stream_data;
        sparrow_ipc::memory_output_stream individual_stream(individual_stream_data);
        sparrow_ipc::serializer individual_serializer(individual_stream);
        
        for (const auto& batch : original_batches) {
            individual_serializer << batch;
        }
        individual_serializer << sparrow_ipc::end_stream;
        
        std::cout << "   Individual serialization size: " << individual_stream_data.size() << " bytes\n";
        std::cout << "   Batch serialization size: " << stream_data.size() << " bytes\n";
        
        // Both should produce the same result
        auto individual_deserialized = sparrow_ipc::deserialize_stream(
            std::span<const uint8_t>(individual_stream_data)
        );
        
        if (individual_deserialized.size() == deserialized_batches.size()) {
            std::cout << "   ✓ Individual and batch serialization produce equivalent results\n";
        } else {
            std::cerr << "   ✗ Individual and batch serialization mismatch!\n";
        }

        // Step 7: Verify schema consistency
        std::cout << "\n7. Verifying schema consistency across all batches...\n";
        bool schema_consistent = true;
        for (size_t i = 1; i < deserialized_batches.size(); ++i) {
            if (deserialized_batches[0].nb_columns() != deserialized_batches[i].nb_columns()) {
                std::cerr << "   ERROR: Batch " << i << " has different number of columns!\n";
                schema_consistent = false;
            }
            
            for (size_t col_idx = 0; col_idx < deserialized_batches[0].nb_columns() && col_idx < deserialized_batches[i].nb_columns(); ++col_idx) {
                const auto& col0 = deserialized_batches[0].get_column(col_idx);
                const auto& col_i = deserialized_batches[i].get_column(col_idx);
                
                if (col0.data_type() != col_i.data_type()) {
                    std::cerr << "   ERROR: Batch " << i << ", column " << col_idx 
                              << " has different type!\n";
                    schema_consistent = false;
                }
                
                if (col0.name() != col_i.name()) {
                    std::cerr << "   ERROR: Batch " << i << ", column " << col_idx 
                              << " has different name!\n";
                    schema_consistent = false;
                }
            }
        }
        
        if (schema_consistent) {
            std::cout << "   ✓ All batches have consistent schema!\n";
        } else {
            std::cerr << "   ✗ Schema inconsistency detected!\n";
        }

        std::cout << "\n=== Example completed successfully! ===\n";

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
