"""Example Name: Error Handling.

Demonstrates: Error handling and recovery strategies in Transmog.

Related Documentation:
- https://transmog.readthedocs.io/en/latest/user/advanced/error-handling.html
- https://transmog.readthedocs.io/en/latest/api/error.html

Learning Objectives:
- How to handle malformed or problematic data
- How to implement different error recovery strategies
- How to configure error handling behavior
- How to balance data quality and processing completeness
"""

import json
import os
from pprint import pprint

# Import from transmog package
import transmog as tm
from transmog.error import LENIENT, PARTIAL, SKIP


def create_problematic_data():
    """Create sample data with various issues for error handling demonstration."""
    # A variety of data problems:
    # 1. Circular reference (not JSON serializable)
    # 2. Missing required fields
    # 3. Incorrect data types
    # 4. Inconsistent array structures
    # 5. Invalid nested objects

    # Create data with circular reference
    circular = {}
    circular["self_reference"] = circular

    return [
        # Valid record (no issues)
        {
            "id": 1,
            "name": "Valid Record",
            "attributes": {"category": "test", "active": True, "score": 95.5},
            "tags": ["valid", "complete", "correct"],
        },
        # Record with circular reference (JSON serialization issue)
        {
            "id": 2,
            "name": "Circular Reference",
            "attributes": circular,
            "tags": ["circular", "reference", "problem"],
        },
        # Record with missing fields
        {
            "id": 3,
            # name is missing
            "attributes": {
                # category is missing
                "active": True,
                "score": 85.0,
            },
            # tags is missing
        },
        # Record with type errors
        {
            "id": "4",  # string instead of integer
            "name": 404,  # number instead of string
            "attributes": {
                "category": ["wrong", "type"],  # array instead of string
                "active": "false",  # string instead of boolean
                "score": "invalid",  # string that can't be parsed as number
            },
            "tags": "not-an-array",  # string instead of array
        },
        # Record with invalid nested structure
        {
            "id": 5,
            "name": "Bad Structure",
            "attributes": None,  # null instead of object
            "tags": [1, 2, None, {"invalid": "object"}],  # mixed types in array
        },
        # Record with deeply nested invalid content
        {
            "id": 6,
            "name": "Deep Error",
            "attributes": {
                "category": "test",
                "active": True,
                "metadata": {
                    "created": "2023-01-01",
                    "nested": {
                        "valid": True,
                        "problems": circular,  # circular reference deep in structure
                    },
                },
            },
            "tags": ["deep", "nested", "error"],
        },
    ]


def main():
    """Run the error handling example."""
    # Create output directory
    output_dir = os.path.join(
        os.path.dirname(__file__), "..", "data", "output", "error_handling"
    )
    os.makedirs(output_dir, exist_ok=True)

    # Get problematic data
    data = create_problematic_data()
    print(f"Created {len(data)} records with various issues")

    print("=== Error Handling Example ===")

    # Example 1: Default error handling (strict mode)
    print("\n=== Default Error Handling (Strict Mode) ===")
    print("Description: Raises exceptions for any errors encountered")

    try:
        # Create processor with default (strict) error handling
        processor = tm.Processor()

        # This will fail on the first problematic record
        _ = processor.process(data=data, entity_name="records")
        print("Processing completed successfully")

    except Exception as e:
        print(f"Error encountered: {type(e).__name__}: {e}")

    # Example 2: Skip strategy - Skip problematic records
    print("\n=== Skip Strategy ===")
    print("Description: Skip problematic records, process valid ones")

    # Create configuration with skip strategy
    skip_config = tm.TransmogConfig.default().with_error_handling(
        allow_malformed_data=True, recovery_strategy=SKIP
    )

    processor = tm.Processor(config=skip_config)

    # Process with skip strategy
    skip_result = processor.process(data=data, entity_name="records")

    # Print processing results
    print("\nProcessing completed with skip strategy")
    main_table = skip_result.get_main_table()
    print(f"Processed {len(main_table)} records successfully")
    print("Processed record IDs:", [record.get("id") for record in main_table])

    # Example 3: Partial strategy - Extract valid parts of records
    print("\n=== Partial Strategy ===")
    print("Description: Extract valid portions of records, skip problematic parts")

    # Create configuration with partial strategy
    partial_config = tm.TransmogConfig.default().with_error_handling(
        allow_malformed_data=True, recovery_strategy=PARTIAL
    )

    processor = tm.Processor(config=partial_config)

    # Process with partial strategy
    partial_result = processor.process(data=data, entity_name="records")

    # Print processing results
    print("\nProcessing completed with partial strategy")
    main_table = partial_result.get_main_table()
    print(f"Processed {len(main_table)} records (including partial records)")

    # Print details of a record with partially extracted data
    print("\nPartial extraction example:")
    for record in main_table:
        if record.get("id") in [3, 5, 6]:  # Records with known issues
            print(f"Record {record.get('id')} was partially processed:")
            pprint(record)
            break

    # Example 4: Lenient strategy - Maximum recovery effort
    print("\n=== Lenient Strategy ===")
    print("Description: Apply multiple recovery methods to salvage data")

    # Create configuration with lenient strategy
    lenient_config = tm.TransmogConfig.default().with_error_handling(
        allow_malformed_data=True, recovery_strategy=LENIENT
    )

    processor = tm.Processor(config=lenient_config)

    # Process with lenient strategy
    lenient_result = processor.process(data=data, entity_name="records")

    # Print processing results
    print("\nProcessing completed with lenient strategy")
    main_table = lenient_result.get_main_table()
    print(f"Processed {len(main_table)} records with lenient recovery")

    # Count how many records include each field to demonstrate recovery effectiveness
    field_counts = {}
    for record in main_table:
        for field in record:
            if field not in field_counts:
                field_counts[field] = 0
            field_counts[field] += 1

    print("\nField extraction statistics:")
    for field, count in field_counts.items():
        if not field.startswith("__"):  # Skip metadata fields
            print(f"- {field}: {count}/{len(main_table)} records")

    # Example 5: Custom error handling configuration
    print("\n=== Custom Error Handling Configuration ===")
    print("Description: Tailored error handling for specific requirements")

    # Create custom error handling configuration
    custom_config = tm.TransmogConfig.default().with_error_handling(
        allow_malformed_data=True,  # Allow processing problematic data
        recovery_strategy=PARTIAL,  # Use partial recovery strategy
        max_retries=2,  # Try recovery operations up to 2 times
        propagate_errors=False,  # Don't propagate errors to caller
        replace_circular_refs=True,  # Replace circular references with placeholder
        circular_placeholder="[CIRCULAR]",  # Custom placeholder for circular references
    )

    processor = tm.Processor(config=custom_config)

    # Process with custom error handling
    custom_result = processor.process(data=data, entity_name="records")

    # Print processing results
    print("\nProcessing completed with custom error handling")
    main_table = custom_result.get_main_table()
    print(f"Processed {len(main_table)} records with custom configuration")

    # Look for circular reference placeholder
    for record in main_table:
        if record.get("id") in [2, 6]:  # Records with circular references
            # Flatten the record to search for placeholders
            json_str = json.dumps(record)
            if "[CIRCULAR]" in json_str:
                print(
                    f"\nFound circular reference placeholders in record "
                    f"{record.get('id')}"
                )

    # Write results from different strategies to files
    lenient_result.write_all_json(
        base_path=os.path.join(output_dir, "lenient"), indent=2
    )

    skip_result.write_all_json(base_path=os.path.join(output_dir, "skip"), indent=2)

    partial_result.write_all_json(
        base_path=os.path.join(output_dir, "partial"), indent=2
    )

    custom_result.write_all_json(base_path=os.path.join(output_dir, "custom"), indent=2)

    print(f"\nOutput files written to: {output_dir}")


if __name__ == "__main__":
    main()
