"""Deep nesting handling examples for the Transmog package.

This module demonstrates how Transmog handles deeply nested structures
using the deep_nesting_threshold parameter.
"""

import os
import sys

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from src package
from transmog import Processor, TransmogConfig


def main():
    """Run the example."""
    # Sample deeply nested JSON data
    deeply_nested_data = {
        "customer": {
            "id": 12345,
            "name": "Example Corporation",
            "billing_information": {
                "account_number": "ACC-123456",
                "payment_methods": [
                    {
                        "type": "credit_card",
                        "card_information": {
                            "card_number_last_four": "1234",
                            "expiration_date": "12/2025",
                            "billing_address": {
                                "street": "123 Corporate Drive",
                                "additional_address_line": "Suite 400",
                                "city": "Enterprise City",
                                "state": "CA",
                                "postal_code": "91234",
                                "address_verification_results": {
                                    "verification_timestamp": "2023-01-15T12:34:56Z",
                                    "verification_status": "verified",
                                    "verification_details": {
                                        "service_provider": "Address Validation Inc.",
                                        "reference_id": "VAL-987654321",
                                    },
                                },
                            },
                        },
                        "authorization_information": {
                            "authorized_users": [
                                {
                                    "user_id": "AUTH001",
                                    "name": "Jane Smith",
                                    "authorization_level": "primary",
                                    "contact_information": {
                                        "email": "jane@example.com",
                                        "phone_numbers": [
                                            {
                                                "type": "work",
                                                "number": "555-123-4567",
                                                "extension": "123",
                                                "verification_status": "verified",
                                                "verification_date": "2023-02-01",
                                            },
                                            {
                                                "type": "mobile",
                                                "number": "555-987-6543",
                                                "verification_status": "pending",
                                            },
                                        ],
                                    },
                                },
                            ],
                        },
                    }
                ],
            },
        }
    }

    print("\n=== DEEPLY NESTED DATA EXAMPLE ===\n")

    # Create output directory if it doesn't exist
    output_dir = os.path.join(
        os.path.dirname(__file__), "..", "data", "output", "deep_nesting"
    )
    os.makedirs(output_dir, exist_ok=True)

    # Example 1: Default deep nesting threshold (4)
    print("\n--- Default Deep Nesting Threshold (4) ---\n")
    config = (
        TransmogConfig.default()
        .with_naming(deeply_nested_threshold=4)
        .with_processing(visit_arrays=False)
    )
    processor = Processor(config)

    # Process the data
    result = processor.process(deeply_nested_data, entity_name="customer")

    # Print results
    print(f"Main table record count: {len(result.get_main_table())}")
    print(f"Child tables: {result.get_table_names()}")

    # Show a sample of field names in the main table
    main_record = result.get_main_table()[0]
    print("\nSample of field names in main table:")
    for i, (key, value) in enumerate(main_record.items()):
        if i < 10 and "billing" in key:  # Just show a few examples
            print(f"  {key}: {value}")

    # Show an example of the deepest nested table structure
    deepest_table = None
    max_nesting = 0

    for table_name in result.get_table_names():
        nesting_level = table_name.count("_") + 1
        if nesting_level > max_nesting:
            max_nesting = nesting_level
            deepest_table = table_name

    if deepest_table:
        print(f"\nDeepest nested table: {deepest_table}")
        print(f"Nesting level: {max_nesting}")
        print(f"Record count: {len(result.get_child_table(deepest_table))}")

    # Write to Parquet to visualize the structure
    print("\nWriting to Parquet...")
    output_path = os.path.join(output_dir, "default_threshold")
    result.write_all_parquet(output_path)
    print(f"Tables written to: {output_path}/")

    # Example 2: Higher deep nesting threshold (6)
    print("\n--- Higher Deep Nesting Threshold (6) ---\n")
    config = (
        TransmogConfig.default()
        .with_naming(deeply_nested_threshold=6)
        .with_processing(visit_arrays=False)
    )
    processor = Processor(config)

    # Process the data
    result = processor.process(deeply_nested_data, entity_name="customer")

    # Print results
    print(f"Main table record count: {len(result.get_main_table())}")
    print(f"Child tables: {result.get_table_names()}")

    # Show a sample of field names in the main table
    main_record = result.get_main_table()[0]
    print("\nSample of field names in main table:")
    for i, (key, value) in enumerate(main_record.items()):
        if i < 10 and "billing" in key:  # Just show a few examples
            print(f"  {key}: {value}")

    # Show an example of the deepest nested table structure
    deepest_table = None
    max_nesting = 0

    for table_name in result.get_table_names():
        nesting_level = table_name.count("_") + 1
        if nesting_level > max_nesting:
            max_nesting = nesting_level
            deepest_table = table_name

    if deepest_table:
        print(f"\nDeepest nested table: {deepest_table}")
        print(f"Nesting level: {max_nesting}")
        print(f"Record count: {len(result.get_child_table(deepest_table))}")

    # Write to Parquet to visualize the structure
    print("\nWriting to Parquet...")
    output_path = os.path.join(output_dir, "high_threshold")
    result.write_all_parquet(output_path)
    print(f"Tables written to: {output_path}/")

    # Example 3: Lower deep nesting threshold (2)
    print("\n--- Lower Deep Nesting Threshold (2) ---\n")
    config = (
        TransmogConfig.default()
        .with_naming(deeply_nested_threshold=2)
        .with_processing(visit_arrays=False)
    )
    processor = Processor(config)

    # Process the data
    result = processor.process(deeply_nested_data, entity_name="customer")

    # Print results
    print(f"Main table record count: {len(result.get_main_table())}")
    print(f"Child tables: {result.get_table_names()}")

    # Show a sample of field names in the main table
    main_record = result.get_main_table()[0]
    print("\nSample of field names in main table:")
    for i, (key, value) in enumerate(main_record.items()):
        if i < 10 and "billing" in key:  # Just show a few examples
            print(f"  {key}: {value}")

    # Show an example of the deepest nested table structure
    deepest_table = None
    max_nesting = 0

    for table_name in result.get_table_names():
        nesting_level = table_name.count("_") + 1
        if nesting_level > max_nesting:
            max_nesting = nesting_level
            deepest_table = table_name

    if deepest_table:
        print(f"\nDeepest nested table: {deepest_table}")
        print(f"Nesting level: {max_nesting}")
        print(f"Record count: {len(result.get_child_table(deepest_table))}")

    # Write to Parquet to visualize the structure
    print("\nWriting to Parquet...")
    output_path = os.path.join(output_dir, "low_threshold")
    result.write_all_parquet(output_path)
    print(f"Tables written to: {output_path}/")

    print("\n=== End of Deep Nesting Examples ===\n")


if __name__ == "__main__":
    main()
