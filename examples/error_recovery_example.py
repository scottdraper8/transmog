"""Error Recovery Example.

This example demonstrates Transmog's error recovery capabilities
for handling problematic data with different recovery strategies.
"""

import os
import sys

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from transmog package
from transmog import Processor, TransmogConfig
from transmog.error import (
    LENIENT,  # Partial recovery: preserve valid portions
    ProcessingError,
    SkipAndLogRecovery,
)


def main():
    """Run the error recovery example."""
    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "output", "error_recovery")
    os.makedirs(output_dir, exist_ok=True)

    # Sample dataset with problematic records
    good_data = {
        "id": 123,
        "name": "Good Record",
        "values": [10, 20, 30],
        "metadata": {"created_at": "2023-01-01", "status": "active"},
    }

    # Problem 1: Inconsistent types
    inconsistent_data = {
        "id": "abc",  # String instead of number
        "name": 456,  # Number instead of string
        "values": "not an array",  # String instead of array
        "metadata": {
            "created_at": 20230101,  # Number instead of string
            "status": True,  # Boolean instead of string
        },
    }

    # Problem 2: Missing required fields
    missing_fields_data = {
        # Missing "id" field
        "name": "Missing Fields Record",
        # Missing "values" array
        "metadata": {
            # Missing "created_at"
            "status": "active"
        },
    }

    # Problem 3: Malformed nested structure
    malformed_data = {
        "id": 789,
        "name": "Malformed Record",
        "values": [40, 50, "invalid", {"nested": "object"}],  # Mixed types
        "metadata": "not an object",  # String instead of object
    }

    # Problem 4: Infinite/NaN values
    invalid_numeric_data = {
        "id": 456,
        "name": "Invalid Numeric",
        "values": [float("inf"), float("-inf"), float("nan")],
        "metadata": {"created_at": "2023-01-02", "status": "active"},
    }

    # Create a combined dataset with problematic records
    mixed_dataset = [
        good_data,
        inconsistent_data,
        missing_fields_data,
        malformed_data,
        invalid_numeric_data,
    ]

    print("=== Sample Dataset ===")
    print(f"Total records: {len(mixed_dataset)}")
    print("Types of problems included:")
    print("1. Inconsistent types")
    print("2. Missing required fields")
    print("3. Malformed nested structures")
    print("4. Invalid numeric values (Inf/NaN)")

    # Example 1: Strict Recovery (Default)
    print("\n=== Example 1: Strict Recovery (Default) ===")
    processor = Processor.default()  # Uses STRICT recovery by default

    try:
        # This will fail on the first problematic record
        result = processor.process(mixed_dataset, entity_name="records")
        print("Processing completed successfully (unexpected)")
    except ProcessingError as e:
        print(f"Error encountered (expected): {e}")
        print("Strict recovery fails on first error (default behavior)")

    # Example 2: Individual processing with try/except
    print("\n=== Example 2: Manual Error Handling ===")
    processor = Processor.default()

    # Process records individually with manual error handling
    successful = []
    failed = []

    for i, record in enumerate(mixed_dataset):
        try:
            result = processor.process(record, entity_name="record")
            successful.append(i)
        except Exception as e:
            failed.append((i, str(e)))

    print(f"Successfully processed: {len(successful)} records")
    print(f"Failed to process: {len(failed)} records")
    for idx, error in failed:
        print(f"  Record {idx}: {error[:60]}...")

    # Example 3: Skip and Log Recovery
    print("\n=== Example 3: Skip and Log Recovery ===")

    # Create a processor with skip_and_log recovery
    skip_config = TransmogConfig.default().with_error_handling(
        recovery_strategy=SkipAndLogRecovery(), allow_malformed_data=True
    )
    processor = Processor(skip_config)

    # Process with skip and log
    result = processor.process(mixed_dataset, entity_name="records")

    print(f"Processed with skip_and_log: {len(result.get_main_table())} records")
    print("Skipped problematic records, continued processing")

    # Write to output
    result.write_all_json(os.path.join(output_dir, "skip_and_log"))
    print(f"Output written to: {os.path.join(output_dir, 'skip_and_log')}")

    # Example 4: Partial (Lenient) Recovery
    print("\n=== Example 4: Partial (Lenient) Recovery ===")

    # Create processor with partial recovery
    processor = Processor.with_partial_recovery()  # Pre-configured factory method

    # Process with partial recovery
    result = processor.process(mixed_dataset, entity_name="records")

    print(f"Processed with partial recovery: {len(result.get_main_table())} records")
    main_table = result.get_main_table()

    # Show what was recovered
    print("\nRecovered Records:")
    for i, record in enumerate(main_table):
        has_error = any(k.startswith("__error") for k in record.keys())
        error_count = sum(1 for k in record.keys() if k.startswith("__error"))
        status = "With errors" if has_error else "Clean"
        print(f"  Record {i}: {status}, Fields: {len(record)}, Errors: {error_count}")

    # Show sample of error fields from first problematic record
    for record in main_table:
        error_fields = {k: v for k, v in record.items() if k.startswith("__error")}
        if error_fields:
            print("\nSample Error Fields:")
            for k, v in list(error_fields.items())[:3]:  # Show first 3 errors
                print(f"  {k}: {v}")
            break

    # Write to output
    result.write_all_json(os.path.join(output_dir, "partial_recovery"))
    print(f"Output written to: {os.path.join(output_dir, 'partial_recovery')}")

    # Example 5: Customizing Error Handling
    print("\n=== Example 5: Customizing Error Handling ===")

    # Create a configuration with custom error handling
    custom_error_config = (
        TransmogConfig.default()
        .with_error_handling(
            recovery_strategy=LENIENT,
            allow_malformed_data=True,
            max_retries=2,
            log_level="WARNING",
        )
        .with_processing(
            cast_to_string=True,  # Cast all values to strings for consistency
            include_empty=True,  # Include empty values
            skip_null=False,  # Don't skip null values
        )
    )

    processor = Processor(custom_error_config)
    result = processor.process(mixed_dataset, entity_name="records")

    print(
        f"Processed with custom error handling: {len(result.get_main_table())} records"
    )

    # Write to output
    result.write_all_json(os.path.join(output_dir, "custom_error_handling"))
    print(f"Output written to: {os.path.join(output_dir, 'custom_error_handling')}")

    # Example 6: Working with recovered data
    print("\n=== Example 6: Working with Recovered Data ===")

    # Process with lenient recovery
    processor = Processor.with_partial_recovery()
    result = processor.process(mixed_dataset, entity_name="records")

    # Extract data with and without errors
    main_table = result.get_main_table()
    clean_records = []
    error_records = []

    for record in main_table:
        has_error = any(k.startswith("__error") for k in record.keys())
        if has_error:
            error_records.append(record)
        else:
            clean_records.append(record)

    print(f"Total records: {len(main_table)}")
    print(f"Clean records: {len(clean_records)}")
    print(f"Records with errors: {len(error_records)}")

    # Demonstrate accessing data
    if clean_records:
        print("\nExample of clean record fields:")
        sample = clean_records[0]
        for key in list(sample.keys())[:5]:  # First 5 fields
            print(f"  {key}: {sample[key]}")

    if error_records:
        print("\nExample of error record fields:")
        sample = error_records[0]
        normal_fields = {k: v for k, v in sample.items() if not k.startswith("__error")}
        error_fields = {k: v for k, v in sample.items() if k.startswith("__error")}

        print(f"  Normal fields: {len(normal_fields)}")
        print(f"  Error fields: {len(error_fields)}")

        # Show a few normal fields
        for key in list(normal_fields.keys())[:3]:
            print(f"  {key}: {normal_fields[key]}")

        # Show error annotations
        for key in list(error_fields.keys())[:2]:
            print(f"  {key}: {error_fields[key]}")

    print("\nConclusion:")
    print("1. STRICT (default): Fails on first error")
    print("2. Skip and Log: Skips problematic records, processes valid ones")
    print("3. LENIENT: Preserves as much data as possible, includes error annotations")
    print("Choose based on your data quality and requirements")


if __name__ == "__main__":
    main()
