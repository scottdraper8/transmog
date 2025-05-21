#!/usr/bin/env python
"""Example demonstrating Transmog's simplified naming system.

This example shows how the naming system handles regular and deeply nested paths.
"""

from transmog import Processor, TransmogConfig


def main():
    """Run the example."""
    # Create example data with deeply nested structures
    deeply_nested_data = {
        "customer": {
            "id": "cust-001",
            "name": "Example Customer",
            "contact": {
                "email": "example@example.com",
                "phone": "555-123-4567",
                "address": {
                    "street": "123 Main St",
                    "city": "Exampleville",
                    "state": "CA",
                    "postal_code": "12345",
                    "geo": {
                        "latitude": 37.7749,
                        "longitude": -122.4194,
                        "coordinates": {
                            "detailed": {"x": 123.456, "y": 789.012, "z": 0.0}
                        },
                    },
                },
            },
            "orders": [
                {
                    "order_id": "ord-001",
                    "date": "2023-01-15",
                    "items": [
                        {
                            "product_id": "prod-001",
                            "quantity": 2,
                            "details": {
                                "name": "Widget",
                                "price": 19.99,
                                "specifications": {
                                    "weight": "0.5kg",
                                    "dimensions": {
                                        "length": 10,
                                        "width": 5,
                                        "height": 2,
                                        "measurements": {
                                            "precise": {
                                                "length_mm": 100.5,
                                                "width_mm": 50.2,
                                                "height_mm": 20.1,
                                            }
                                        },
                                    },
                                },
                            },
                        }
                    ],
                }
            ],
        }
    }

    print("=== Standard Naming Example ===\n")

    # Create a processor with default naming settings (deeply_nested_threshold=4)
    processor = Processor()

    # Process the data with the entity name
    result = processor.process(deeply_nested_data, entity_name="customer_data")

    # Print the main record
    print("Main table fields:")
    main_record = result.get_main_table()[0]
    for key, value in sorted(main_record.items()):
        # Only show a few example fields
        if "address" in key and "geo" in key and "coordinates" in key and len(key) > 60:
            print(f"  {key}: {value}")

    # Show tables
    print("\nGenerated tables:")
    for table_name in sorted(result.get_table_names()):
        table = result.get_child_table(table_name)
        print(f"  {table_name}: {len(table)} records")

    # Find and print a deeply nested table name
    deeply_nested_tables = [
        name for name in result.get_table_names() if "nested" in name
    ]

    print("\nDeeply nested tables:")
    for table in deeply_nested_tables:
        print(f"  {table}")

    # Example 2: Custom deeply nested threshold
    print("\n=== Custom Deeply Nested Threshold Example ===\n")

    # Create a processor with a very high deeply nested threshold (no simplification)
    config = TransmogConfig.default().with_naming(deeply_nested_threshold=20)
    processor = Processor(config=config)

    # Process the data
    result = processor.process(deeply_nested_data, entity_name="customer_data")

    # Show tables
    print("Generated tables with high threshold (no simplification):")
    for table_name in sorted(result.get_table_names()):
        if "coordinates" in table_name or "dimensions" in table_name:
            print(f"  {table_name}: {len(result.get_child_table(table_name))} records")

    # Example 3: Very low deeply nested threshold
    print("\n=== Low Deeply Nested Threshold Example ===\n")

    # Create a processor with a low deeply nested threshold
    config = TransmogConfig.default().with_naming(deeply_nested_threshold=2)
    processor = Processor(config=config)

    # Process the data
    result = processor.process(deeply_nested_data, entity_name="customer_data")

    # Show tables
    print("Generated tables with low threshold (more simplification):")
    for table_name in sorted(result.get_table_names()):
        print(f"  {table_name}: {len(result.get_child_table(table_name))} records")


if __name__ == "__main__":
    main()
