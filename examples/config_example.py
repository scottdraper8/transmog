"""
Example demonstrating Transmog configuration functionality.

This example shows how to use the TransmogConfig system with its fluent API
to configure Transmog processing.
"""

import os
import sys

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from src package
from transmog import Processor, TransmogConfig, ProcessingMode, ConversionMode


def main():
    """Run the example."""
    # Sample nested JSON data
    data = {
        "id": 123,
        "name": "Example Company",
        "address": {
            "street": "123 Main St",
            "city": "Anytown",
            "state": "CA",
            "zip": "12345",
        },
        "contacts": [
            {
                "type": "primary",
                "name": "John Doe",
                "phone": "555-1234",
                "details": {"department": "Sales", "position": "Manager"},
            },
            {
                "type": "secondary",
                "name": "Jane Smith",
                "phone": "555-5678",
                "details": {"department": "Support", "position": "Director"},
            },
        ],
    }

    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)

    # Example 1: Use default configuration
    print("\n=== Example 1: Default Configuration ===")
    config = TransmogConfig.default()
    processor = Processor(config=config)
    result = processor.process(data=data, entity_name="company")
    print(f"Main table record count: {len(result.get_main_table())}")
    print(f"Child tables: {result.get_table_names()}")
    print(f"Naming separator: '{config.naming.separator}'")
    print(f"Processing batch size: {config.processing.batch_size}")

    # Example 2: Memory-optimized configuration
    print("\n=== Example 2: Memory-Optimized Configuration ===")
    config = TransmogConfig.memory_optimized()
    processor = Processor(config=config)
    result = processor.process(data=data, entity_name="company")
    print(f"Main table record count: {len(result.get_main_table())}")
    print(f"Processing mode: {config.processing.processing_mode}")
    print(f"Batch size: {config.processing.batch_size}")

    # Example 3: Performance-optimized configuration
    print("\n=== Example 3: Performance-Optimized Configuration ===")
    config = TransmogConfig.performance_optimized()
    processor = Processor(config=config)
    result = processor.process(data=data, entity_name="company")
    print(f"Main table record count: {len(result.get_main_table())}")
    print(f"Processing mode: {config.processing.processing_mode}")
    print(f"Batch size: {config.processing.batch_size}")

    # Example 4: Custom configuration with fluent API
    print("\n=== Example 4: Custom Configuration with Fluent API ===")
    config = (
        TransmogConfig.default()
        .with_naming(
            separator=".", abbreviate_table_names=False, max_table_component_length=15
        )
        .with_processing(
            cast_to_string=False,
            include_empty=True,
            batch_size=500,
            processing_mode=ProcessingMode.STANDARD,
        )
        .with_metadata(
            id_field="record_id", parent_field="parent_id", time_field="processed_at"
        )
        .with_error_handling(
            recovery_strategy="skip", allow_malformed_data=True, max_retries=3
        )
    )
    processor = Processor(config=config)
    result = processor.process(data=data, entity_name="company")
    print(f"Main table record count: {len(result.get_main_table())}")
    print(f"Naming separator: '{config.naming.separator}'")
    print(f"Allow malformed data: {config.error_handling.allow_malformed_data}")

    # Save the result
    result.write_all_json(base_path=output_dir)
    print(f"Output written to: {output_dir}")

    # Example 5: Factory methods for processor creation
    print("\n=== Example 5: Factory Methods for Processor Creation ===")

    # Demonstrate different factory methods
    print("Default processor created")
    Processor.default()

    print("Memory-optimized processor created")
    Processor.memory_optimized()

    print("Performance-optimized processor created")
    Processor.performance_optimized()

    print("Processor with deterministic IDs created")
    Processor.with_deterministic_ids(
        {
            "": "id",  # Root level uses "id" field
            "company_contacts": "name",  # Contacts table uses "name" field
        }
    )

    print("Processor with partial recovery created")
    Processor.with_partial_recovery()

    # Example 6: Configuring conversion modes
    print("\n=== Example 6: Conversion Modes for Memory Management ===")

    # Process data
    processor = Processor()
    result = processor.process(data=data, entity_name="company")

    # Default mode (EAGER) - keeps converted data in memory
    print("Default conversion mode (EAGER):")
    default_json = result.to_json_bytes()
    print(f"  JSON bytes size: {len(default_json['main'])} bytes (cached for reuse)")

    # LAZY mode - converts on demand
    lazy_result = result.with_conversion_mode(ConversionMode.LAZY)
    print("LAZY conversion mode:")
    lazy_json = lazy_result.to_json_bytes()
    print(f"  JSON bytes size: {len(lazy_json['main'])} bytes (not cached)")

    # MEMORY_EFFICIENT mode - clear intermediate data
    efficient_result = result.with_conversion_mode(ConversionMode.MEMORY_EFFICIENT)
    print("MEMORY_EFFICIENT conversion mode:")
    efficient_json = efficient_result.to_json_bytes()
    print(
        f"  JSON bytes size: {len(efficient_json['main'])} bytes (intermediate data cleared)"
    )

    # Write to files with memory-efficient mode
    efficient_result.write_all_csv(os.path.join(output_dir, "memory_efficient"))
    print(
        f"  Memory-efficient output written to: {os.path.join(output_dir, 'memory_efficient')}"
    )


if __name__ == "__main__":
    main()
