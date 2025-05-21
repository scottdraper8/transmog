"""Example Name: Configuration Options.

Demonstrates: Different ways to configure the Transmog processor

Related Documentation:
- https://transmog.readthedocs.io/en/latest/user/essentials/configuration.html

Learning Objectives:
- How to use different configuration options
- How to create custom configurations
- How to use the new configuration shortcuts
- How to handle configuration validation
"""

import os
from pprint import pprint

# Import from transmog package
import transmog as tm
from transmog.error import ConfigurationError


def main():
    """Run the configuration example."""
    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "..", "data", "output")
    os.makedirs(output_dir, exist_ok=True)

    # Sample data for testing configurations
    data = {
        "id": 123,
        "name": "Example Entity",
        "details": {
            "category": "Test",
            "active": True,
            "score": 92.5,
            "tags": ["tag1", "tag2", "tag3"],
        },
        "items": [
            {"id": "i1", "name": "Item 1", "price": 10.99},
            {"id": "i2", "name": "Item 2", "price": 20.99},
        ],
    }

    # Example 1: Default configuration
    print("\n=== Default Configuration ===")
    processor = tm.Processor()
    result = processor.process(data=data, entity_name="entity")
    print_result_overview(result)

    # Example 2: Predefined Configurations (NEW)
    print("\n=== Predefined Configurations ===")

    # Simple mode with minimal metadata
    print("\n-- Simple Mode --")
    simple_processor = tm.Processor(tm.TransmogConfig.simple_mode())
    simple_result = simple_processor.process(data=data, entity_name="entity")
    print_result_overview(simple_result)

    # CSV-optimized configuration
    print("\n-- CSV Optimized --")
    csv_processor = tm.Processor(tm.TransmogConfig.csv_optimized())
    csv_result = csv_processor.process(data=data, entity_name="entity")
    print_result_overview(csv_result)

    # Error-tolerant configuration
    print("\n-- Error Tolerant --")
    tolerant_processor = tm.Processor(tm.TransmogConfig.error_tolerant())
    tolerant_result = tolerant_processor.process(data=data, entity_name="entity")
    print_result_overview(tolerant_result)

    # Example 3: Custom configurations - Component-specific updates
    print("\n=== Custom Component Configurations ===")

    # Update naming configuration
    print("\n-- Custom Naming Configuration --")
    naming_config = tm.TransmogConfig.default().with_naming(
        separator=".",
        deep_nesting_threshold=3,
        max_field_component_length=4,
    )
    naming_processor = tm.Processor(naming_config)
    naming_result = naming_processor.process(data=data, entity_name="entity")
    print("First record field names:")
    pprint(list(naming_result.get_main_table()[0].keys())[:10])

    # Update processing configuration
    print("\n-- Custom Processing Configuration --")
    processing_config = tm.TransmogConfig.default().with_processing(
        cast_to_string=False,
        include_empty=True,
        skip_null=False,
    )
    processing_processor = tm.Processor(processing_config)
    processing_result = processing_processor.process(data=data, entity_name="entity")
    print("Data with original types:")
    main_record = processing_result.get_main_table()[0]
    print(f"Score type: {type(main_record.get('details_score'))}")
    print(f"Active type: {type(main_record.get('details_active'))}")

    # Example 4: Configuration shortcuts (NEW)
    print("\n=== Configuration Shortcuts ===")

    # Use dot notation
    print("\n-- Using Dot Notation --")
    dot_config = tm.TransmogConfig.default().use_dot_notation()
    dot_processor = tm.Processor(dot_config)
    dot_result = dot_processor.process(data=data, entity_name="entity")
    print("Field names with dot notation:")
    pprint(list(dot_result.get_main_table()[0].keys())[:5])

    # Disable arrays
    print("\n-- Disable Arrays --")
    no_arrays_config = tm.TransmogConfig.default().disable_arrays()
    no_arrays_processor = tm.Processor(no_arrays_config)
    no_arrays_result = no_arrays_processor.process(data=data, entity_name="entity")
    print(f"Child tables: {no_arrays_result.get_table_names()}")

    # String formatting
    print("\n-- String Formatting --")
    string_config = tm.TransmogConfig.default().use_string_format()
    string_processor = tm.Processor(string_config)
    string_result = string_processor.process(data=data, entity_name="entity")
    main_record = string_result.get_main_table()[0]
    print(f"Score type: {type(main_record.get('details_score'))}")
    print(f"Active value: {main_record.get('details_active')}")

    # Example 5: Configuration validation
    print("\n=== Configuration Validation ===")

    # Invalid separator
    try:
        # This will raise a validation error before assignment
        tm.TransmogConfig.default().with_naming(separator="")
        print("This shouldn't happen - validation should catch empty separator")
    except ConfigurationError as e:
        print(f"Caught separator validation error: {e}")

    # Duplicate field names
    try:
        # This will raise a validation error before assignment
        tm.TransmogConfig.default().with_metadata(
            id_field="record_id",
            parent_field="record_id",  # Same as id_field
        )
        print("This shouldn't happen - validation should catch duplicate fields")
    except ConfigurationError as e:
        print(f"Caught field validation error: {e}")

    # Invalid batch size
    try:
        # This will raise a validation error before assignment
        tm.TransmogConfig.default().with_processing(batch_size=-1)
        print("This shouldn't happen - validation should catch negative batch size")
    except ConfigurationError as e:
        print(f"Caught batch size validation error: {e}")


def print_result_overview(result):
    """Print a quick overview of a result."""
    main_table = result.get_main_table()
    tables = result.get_table_names()

    print(f"Main table has {len(main_table)} records")
    print(f"Created {len(tables)} child tables: {', '.join(tables)}")
    if tables:
        child_table = result.get_child_table(tables[0])
        print(f"First child table '{tables[0]}' has {len(child_table)} records")


if __name__ == "__main__":
    main()
