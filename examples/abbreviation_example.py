"""
Example demonstrating field and table name abbreviation in Transmogrify.

This example shows how field and table name abbreviation works in Transmogrify,
including custom abbreviation and component length settings.
"""

import json
import os
import sys
from pprint import pprint

# Add parent directory to path to import transmogrify without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from src package
from src.transmogrify import Processor, configure
from src.transmogrify.naming.abbreviator import get_common_abbreviations


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
                                {
                                    "user_id": "AUTH002",
                                    "name": "John Doe",
                                    "authorization_level": "secondary",
                                    "contact_information": {
                                        "email": "john@example.com",
                                        "phone_numbers": [
                                            {
                                                "type": "work",
                                                "number": "555-345-6789",
                                            }
                                        ],
                                    },
                                },
                            ],
                        },
                    }
                ],
                "billing_history": {
                    "last_invoice_date": "2023-03-15",
                    "payment_reliability_score": 98,
                    "transaction_history": [
                        {
                            "transaction_id": "TXN-001",
                            "date": "2023-03-15",
                            "amount": 1250.00,
                            "status": "completed",
                            "items": [
                                {
                                    "item_id": "ITEM-001",
                                    "description": "Enterprise Subscription",
                                    "quantity": 1,
                                    "unit_price": 1000.00,
                                    "metadata": {
                                        "department": "IT",
                                        "project_code": "PRJ-2023-001",
                                        "approval_chain": [
                                            {
                                                "approver_id": "EMP-001",
                                                "approval_date": "2023-03-10",
                                                "approver_comments": "Approved for annual renewal",
                                            }
                                        ],
                                    },
                                },
                                {
                                    "item_id": "ITEM-002",
                                    "description": "Additional User Licenses",
                                    "quantity": 5,
                                    "unit_price": 50.00,
                                    "metadata": {
                                        "department": "Sales",
                                        "project_code": "PRJ-2023-002",
                                    },
                                },
                            ],
                        },
                        {
                            "transaction_id": "TXN-002",
                            "date": "2023-02-15",
                            "amount": 1250.00,
                            "status": "completed",
                            "items": [
                                {
                                    "item_id": "ITEM-001",
                                    "description": "Enterprise Subscription",
                                    "quantity": 1,
                                    "unit_price": 1000.00,
                                },
                                {
                                    "item_id": "ITEM-002",
                                    "description": "Additional User Licenses",
                                    "quantity": 5,
                                    "unit_price": 50.00,
                                },
                            ],
                        },
                    ],
                },
            },
        }
    }

    print("\n=== DEEPLY NESTED DATA EXAMPLE ===\n")

    # Create output directory if it doesn't exist
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)

    # Example 1: Default settings with abbreviation enabled
    print("\n--- Default Settings (Abbreviation Enabled) ---\n")
    processor = Processor(
        abbreviate_table_names=True,
        abbreviate_field_names=True,
        max_table_component_length=3,
        max_field_component_length=3,
        preserve_leaf_component=True,
        visit_arrays=False,
    )

    # Process the data
    result = processor.process(deeply_nested_data, entity_name="customer")

    # Print results
    print(f"Main table record count: {len(result.get_main_table())}")
    print(f"Child tables: {result.get_table_names()}")

    # Show a sample of abbreviated field names in the main table
    main_record = result.get_main_table()[0]
    print("\nSample of abbreviated field names in main table:")
    for i, (key, value) in enumerate(main_record.items()):
        if i < 10 and "bill" in key:  # Just show a few examples
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
    output_path = os.path.join(output_dir, "abbreviated")
    file_paths = result.write_all_parquet(output_path)
    print(f"Tables written to: {output_path}/")

    # Example 2: No abbreviation
    print("\n--- No Abbreviation ---\n")
    processor = Processor(
        abbreviate_table_names=False,
        abbreviate_field_names=False,
        visit_arrays=False,
    )

    # Process the data
    result = processor.process(deeply_nested_data, entity_name="customer")

    # Print results
    print(f"Main table record count: {len(result.get_main_table())}")
    print(f"Child tables: {result.get_table_names()}")

    # Show a sample of unabbreviated field names in the main table
    main_record = result.get_main_table()[0]
    print("\nSample of unabbreviated field names in main table:")
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
    output_path = os.path.join(output_dir, "unabbreviated")
    file_paths = result.write_all_parquet(output_path)
    print(f"Tables written to: {output_path}/")

    # Example 3: Extreme abbreviation (aggressive settings)
    print("\n--- Extreme Abbreviation ---\n")
    processor = Processor(
        abbreviate_table_names=True,
        abbreviate_field_names=True,
        max_table_component_length=3,
        max_field_component_length=3,
        preserve_leaf_component=False,
        visit_arrays=False,
    )

    # Process the data
    result = processor.process(deeply_nested_data, entity_name="customer")

    # Print results
    print(f"Main table record count: {len(result.get_main_table())}")
    print(f"Child tables: {result.get_table_names()}")

    # Show a sample of extremely abbreviated field names in the main table
    main_record = result.get_main_table()[0]
    print("\nSample of extremely abbreviated field names in main table:")
    for i, (key, value) in enumerate(main_record.items()):
        if i < 10 and "bil" in key:  # Just show a few examples
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
    output_path = os.path.join(output_dir, "extreme_abbreviated")
    file_paths = result.write_all_parquet(output_path)
    print(f"Tables written to: {output_path}/")

    # Example 4: Custom abbreviations
    print("\n--- Custom Abbreviations ---\n")

    # Add some custom abbreviations
    custom_abbrevs = {
        "information": "i",
        "authorization": "az",
        "verification": "v",
        "transaction": "tx",
        "billing": "b",
        "payment": "p",
        "customer": "c",
    }

    processor = Processor(
        abbreviate_table_names=True,
        abbreviate_field_names=True,
        max_table_component_length=5,
        max_field_component_length=5,
        preserve_leaf_component=True,
        custom_abbreviations=custom_abbrevs,
        visit_arrays=False,
    )

    # Process the data
    result = processor.process(deeply_nested_data, entity_name="customer")

    # Print results
    print(f"Main table record count: {len(result.get_main_table())}")
    print(f"Child tables: {result.get_table_names()}")

    # Show a sample of custom abbreviated field names in the main table
    main_record = result.get_main_table()[0]
    print("\nSample of custom abbreviated field names in main table:")
    for i, (key, value) in enumerate(main_record.items()):
        if i < 10 and "b_" in key:  # Just show a few examples
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
    output_path = os.path.join(output_dir, "custom_abbreviated")
    file_paths = result.write_all_parquet(output_path)
    print(f"Tables written to: {output_path}/")


if __name__ == "__main__":
    main()
