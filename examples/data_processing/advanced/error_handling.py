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

import os
from pprint import pprint

# Import from transmog package
import transmog as tm


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
        # Default behavior - raises on errors
        result = tm.flatten(data, name="records")
        print("Processing completed successfully")
        print(f"Processed {len(result.main)} records")

    except Exception as e:
        print(f"Error encountered: {type(e).__name__}: {e}")

    # Example 2: Skip strategy - Skip problematic records
    print("\n=== Skip Strategy ===")
    print("Description: Skip problematic records, process valid ones")

    # Process with skip strategy
    skip_result = tm.flatten(data, name="records", on_error="skip")

    # Print processing results
    print("\nProcessing completed with skip strategy")
    print(f"Processed {len(skip_result.main)} records successfully")
    print("Processed record IDs:", [record.get("id") for record in skip_result.main])

    # Example 3: Warn strategy - Log warnings but continue processing
    print("\n=== Warn Strategy ===")
    print("Description: Log warnings for problematic data but continue processing")

    # Process with warn strategy
    warn_result = tm.flatten(data, name="records", on_error="warn")

    # Print processing results
    print("\nProcessing completed with warn strategy")
    print(f"Processed {len(warn_result.main)} records (including partial records)")

    # Print details of a record with partially extracted data
    print("\nPartial extraction example:")
    for record in warn_result.main:
        if record.get("id") in [3, 5, 6]:  # Records with known issues
            print(f"Record {record.get('id')} was partially processed:")
            pprint(record)
            break

    # Example 4: Advanced error handling with Processor class
    print("\n=== Advanced Error Handling Configuration ===")
    print("Description: Tailored error handling for specific requirements")

    # For advanced error handling, access the Processor class directly
    from transmog.config import TransmogConfig
    from transmog.process import Processor

    # Create custom error handling configuration
    custom_config = TransmogConfig.default().with_error_handling(
        allow_malformed_data=True,  # Allow processing problematic data
        recovery_strategy="partial",  # Use partial recovery strategy
        max_retries=10,  # Try recovery operations up to 10 times
    )

    processor = Processor(config=custom_config)

    # Process with custom error handling
    custom_result = processor.process(data=data, entity_name="records")

    # Print processing results
    print("\nProcessing completed with custom error handling")
    main_table = custom_result.get_main_table()
    print(f"Processed {len(main_table)} records with custom configuration")

    # Look for circular reference handling
    for record in main_table:
        if record.get("id") in [2, 6]:  # Records with circular references
            print(f"\nRecord {record.get('id')} with circular reference was processed:")
            pprint(record)
            break

    # Example 5: Comparison of different strategies
    print("\n=== Strategy Comparison ===")
    print("Description: Compare results from different error handling strategies")

    strategies = [
        ("skip", skip_result),
        ("warn", warn_result),
    ]

    print("\nComparison of error handling strategies:")
    print(f"{'Strategy':<10} {'Records':<8} {'Tables':<8} {'Description'}")
    print("-" * 60)

    for strategy_name, result in strategies:
        num_records = len(result.main)
        num_tables = len(result.tables) + 1  # +1 for main table

        if strategy_name == "skip":
            description = "Only valid records"
        elif strategy_name == "warn":
            description = "All records, partial data"
        else:
            description = "Custom recovery"

        print(f"{strategy_name:<10} {num_records:<8} {num_tables:<8} {description}")

    # Count how many records include each field to demonstrate recovery effectiveness
    print("\nField extraction statistics (warn strategy):")
    field_counts = {}
    for record in warn_result.main:
        for field in record:
            if field not in field_counts:
                field_counts[field] = 0
            field_counts[field] += 1

    for field, count in field_counts.items():
        if not field.startswith("_"):  # Skip metadata fields
            print(f"- {field}: {count}/{len(warn_result.main)} records")

    # Write results from different strategies to files
    print("\n=== Saving Results ===")

    # Save with different strategies
    skip_result.save(os.path.join(output_dir, "skip", "main.json"))
    warn_result.save(os.path.join(output_dir, "lenient", "main.json"))

    # Save individual tables for detailed analysis
    for table_name, table_data in skip_result.tables.items():
        skip_result.save(
            os.path.join(output_dir, "skip", f"{table_name}.json"), table=table_name
        )

    for table_name, table_data in warn_result.tables.items():
        warn_result.save(
            os.path.join(output_dir, "lenient", f"{table_name}.json"), table=table_name
        )

    print("Results saved to output directory")
    print(f"- Skip strategy results: {os.path.join(output_dir, 'skip')}")
    print(f"- Warn strategy results: {os.path.join(output_dir, 'lenient')}")

    # Example 6: Error handling with different data types
    print("\n=== Error Handling with Different Data Types ===")

    # Create data with different problematic types
    mixed_data = [
        {"id": 1, "data": [1, 2, 3]},  # Valid array
        {"id": 2, "data": "not_an_array"},  # String instead of array
        {"id": 3, "data": {"nested": "object"}},  # Object instead of array
        {"id": 4, "data": None},  # Null value
        {"id": 5},  # Missing field entirely
    ]

    # Process with different strategies
    mixed_skip = tm.flatten(mixed_data, name="mixed", on_error="skip")
    mixed_warn = tm.flatten(mixed_data, name="mixed", on_error="warn")

    print(f"Mixed data - Skip strategy: {len(mixed_skip.main)} records")
    print(f"Mixed data - Warn strategy: {len(mixed_warn.main)} records")

    print("\nExample completed successfully!")
    print("Check the output directory for detailed results from each strategy.")


if __name__ == "__main__":
    main()
