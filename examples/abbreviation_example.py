#!/usr/bin/env python
"""
Example demonstrating the abbreviation system in Transmog.
"""

from transmog import Processor


def main():
    """Run the example."""
    # Create test data with nested fields
    data = {
        "customer": {
            "personal_information": {
                "first_name": "John",
                "last_name": "Doe",
                "contact_details": {
                    "email": "john.doe@example.com",
                    "phone_number": "555-1234",
                },
            },
            "billing_address": {
                "street": "123 Main St",
                "city": "Anytown",
                "state": "CA",
                "postal_code": "12345",
            },
            "shipping_address": {
                "street": "456 Second Ave",
                "city": "Elsewhere",
                "state": "NY",
                "postal_code": "67890",
            },
        },
        "order": {
            "order_number": "ORD-12345",
            "order_date": "2023-05-01",
            "order_items": [
                {
                    "product_id": "PROD-1",
                    "product_name": "Widget",
                    "quantity": 2,
                    "unit_price": 19.99,
                },
                {
                    "product_id": "PROD-2",
                    "product_name": "Gadget",
                    "quantity": 1,
                    "unit_price": 29.99,
                },
            ],
        },
    }

    # Custom abbreviations
    custom_abbrev = {
        "information": "info",
        "address": "addr",
        "details": "dtls",
        "number": "num",
        "product": "prod",
    }

    # Example 1: Default behavior (preserve root and leaf)
    print("\n=== Example 1: Default Abbreviation ===")
    processor = Processor(
        abbreviate_field_names=True,
        max_field_component_length=4,
        preserve_root_component=True,
        preserve_leaf_component=True,
        custom_abbreviations=custom_abbrev,
    )
    result = processor.process(data)
    print_flattened_fields(result.get_main_table())

    # Example 2: Abbreviate leaf components
    print("\n=== Example 2: Abbreviate Leaf Components ===")
    processor = Processor(
        abbreviate_field_names=True,
        max_field_component_length=4,
        preserve_root_component=True,
        preserve_leaf_component=False,
        custom_abbreviations=custom_abbrev,
    )
    result = processor.process(data)
    print_flattened_fields(result.get_main_table())

    # Example 3: Abbreviate root components
    print("\n=== Example 3: Abbreviate Root Components ===")
    processor = Processor(
        abbreviate_field_names=True,
        max_field_component_length=4,
        preserve_root_component=False,
        preserve_leaf_component=True,
        custom_abbreviations=custom_abbrev,
    )
    result = processor.process(data)
    print_flattened_fields(result.get_main_table())

    # Example 4: Abbreviate all components
    print("\n=== Example 4: Abbreviate All Components ===")
    processor = Processor(
        abbreviate_field_names=True,
        max_field_component_length=4,
        preserve_root_component=False,
        preserve_leaf_component=False,
        custom_abbreviations=custom_abbrev,
    )
    result = processor.process(data)
    print_flattened_fields(result.get_main_table())


def print_flattened_fields(records):
    """Print flattened field names."""
    if not records:
        print("No records found.")
        return

    # Get the first record
    record = records[0]

    # Print field names, sorted for readability
    field_names = [name for name in sorted(record.keys()) if not name.startswith("__")]
    max_length = max(len(name) for name in field_names)

    print(f"Found {len(field_names)} fields:")
    for name in field_names:
        # Skip metadata fields
        if name.startswith("__"):
            continue
        print(f"  {name:{max_length}} = {record[name]}")


if __name__ == "__main__":
    main()
