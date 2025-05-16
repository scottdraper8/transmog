"""Example Name: Configuration.

Demonstrates: Configuration options for the Transmog processor

Related Documentation:
- https://transmog.readthedocs.io/en/latest/user/essentials/configuration.html
- https://transmog.readthedocs.io/en/latest/api/config.html

Learning Objectives:
- How to configure Transmog with TransmogConfig
- How to use the fluent API for configuration
- How to create and apply custom configurations
- How to use predefined configurations
"""

import os
from pprint import pprint

# Import from transmog package
import transmog as tm


def main():
    """Run the configuration examples."""
    # Create sample data
    data = {
        "id": 101,
        "name": "Example Organization",
        "status": "active",
        "founded": 1995,
        "details": {
            "description": "A sample organization for examples",
            "industry": "Technology",
            "employees": 500,
            "public": True,
            "valuation": None,
        },
        "locations": [
            {
                "id": 1,
                "name": "Headquarters",
                "address": {
                    "street": "123 Main St",
                    "city": "San Francisco",
                    "state": "CA",
                    "postal_code": "94105",
                },
            },
            {
                "id": 2,
                "name": "Branch Office",
                "address": {
                    "street": "456 Market St",
                    "city": "New York",
                    "state": "NY",
                    "postal_code": "10001",
                },
            },
        ],
    }

    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "..", "data", "output")
    os.makedirs(output_dir, exist_ok=True)

    # Example 1: Default configuration
    print("\n=== Default Configuration ===")

    processor = tm.Processor()
    result = processor.process(data=data, entity_name="org")

    print("Default Configuration Settings:")
    print("- Separator: '_'")
    print("- Cast to string: True")
    print("- Skip null: True")
    print("- Processing mode: STANDARD")

    print("\nResult with Default Configuration:")
    if result.get_main_table():
        pprint(result.get_main_table()[0])

    # Example 2: Using Predefined Configurations
    print("\n=== Predefined Configurations ===")

    # Memory-optimized configuration
    # Create processor but don't use it - just for demonstration
    _ = tm.Processor.memory_optimized()
    print("\nMemory-Optimized Configuration Settings:")
    print("- Processing mode: MEMORY_OPTIMIZED")
    print("- Batch size: Smaller")

    # Performance-optimized configuration
    # Create processor but don't use it - just for demonstration
    _ = tm.Processor.performance_optimized()
    print("\nPerformance-Optimized Configuration Settings:")
    print("- Processing mode: PERFORMANCE_OPTIMIZED")
    print("- Batch size: Larger")

    # Example 3: Custom Configuration with Fluent API
    print("\n=== Custom Configuration with Fluent API ===")

    # Create custom configuration using fluent API
    custom_config = (
        tm.TransmogConfig.default()
        .with_naming(
            separator=".",  # Use dot as separator
            abbreviate_table_names=True,  # Abbreviate table names
            separator_replacement="_",  # Replace separators in input keys
            # with underscore
        )
        .with_processing(
            cast_to_string=False,  # Keep original types
            skip_null=False,  # Include null values
            arrays_to_string=False,  # Don't convert arrays to strings
            batch_size=500,  # Process in batches of 500
        )
        .with_metadata(
            id_field="record_id",  # Custom ID field name
            parent_field="parent_id",  # Custom parent field name
            datetime_field="processed_at",  # Custom datetime field name
        )
    )

    # Use the custom configuration
    custom_processor = tm.Processor(config=custom_config)
    custom_result = custom_processor.process(data=data, entity_name="org")

    print("Custom Configuration Settings:")
    print("- Separator: '.'")
    print("- Cast to string: False")
    print("- Skip null: False")
    print("- ID field: 'record_id'")

    print("\nResult with Custom Configuration:")
    if custom_result.get_main_table():
        record = custom_result.get_main_table()[0]
        # Print a subset of fields to demonstrate changes
        for field in ["record_id", "id", "name", "details.industry"]:
            if field in record:
                print(f"{field}: {record[field]}")

    # Example 4: Specific Configurations for Different Uses
    print("\n=== Specific Configurations for Different Uses ===")

    # Configuration for CSV output
    csv_config = tm.TransmogConfig.default().with_processing(
        cast_to_string=True,  # Ensure all values are strings for CSV
        arrays_to_string=True,  # Convert arrays to string representation
    )

    csv_processor = tm.Processor(config=csv_config)
    # Process data but not used - just for demonstration
    _ = csv_processor.process(data=data, entity_name="org")

    # Configuration for type preservation (for Parquet/SQL)
    typed_config = tm.TransmogConfig.default().with_processing(
        cast_to_string=False,  # Preserve original types
        cast_from_string=False,  # Don't attempt to cast strings to other types
        skip_null=False,  # Include nulls for proper schema inference
    )

    typed_processor = tm.Processor(config=typed_config)
    # Process data but not used - just for demonstration
    _ = typed_processor.process(data=data, entity_name="org")

    print("Configuration Approaches Available:")
    print("- CSV-optimized: All fields cast to strings, arrays converted to strings")
    print("- Type-preserving: Original types preserved, nulls included")
    print("- Memory-optimized: Reduced memory footprint for large datasets")
    print("- Performance-optimized: Faster processing with more memory usage")


if __name__ == "__main__":
    main()
