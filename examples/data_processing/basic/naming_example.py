#!/usr/bin/env python
"""Example demonstrating Transmog's simplified naming system v1.1.0.

This example shows how the naming system handles regular and deeply nested paths.
"""

import transmog as tm


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

    # Use the simple API with default naming settings (nested_threshold=4)
    result = tm.flatten(deeply_nested_data, name="customer_data")

    # Print the main record
    print("Main table fields:")
    main_record = result.main[0]
    for key, value in sorted(main_record.items()):
        # Only show a few example fields
        if "address" in key and "geo" in key and "coordinates" in key and len(key) > 60:
            print(f"  {key}: {value}")

    # Show tables
    print("\nGenerated tables:")
    for table_name in sorted(result.tables.keys()):
        table = result.tables[table_name]
        print(f"  {table_name}: {len(table)} records")

    # Find and print a deeply nested table name
    deeply_nested_tables = [name for name in result.tables.keys() if "nested" in name]

    print("\nDeeply nested tables:")
    for table in deeply_nested_tables:
        print(f"  {table}")

    # Example 2: Custom deeply nested threshold
    print("\n=== Custom Deeply Nested Threshold Example ===\n")

    # Use a very high deeply nested threshold (no simplification)
    result = tm.flatten(deeply_nested_data, name="customer_data", nested_threshold=20)

    # Show tables
    print("Generated tables with high threshold (no simplification):")
    for table_name in sorted(result.tables.keys()):
        if "coordinates" in table_name or "dimensions" in table_name:
            print(f"  {table_name}: {len(result.tables[table_name])} records")

    # Example 3: Very low deeply nested threshold
    print("\n=== Low Deeply Nested Threshold Example ===\n")

    # Use a low deeply nested threshold
    result = tm.flatten(deeply_nested_data, name="customer_data", nested_threshold=2)

    # Show tables
    print("Generated tables with low threshold (more simplification):")
    for table_name in sorted(result.tables.keys()):
        print(f"  {table_name}: {len(result.tables[table_name])} records")

    # Example 4: Custom separator
    print("\n=== Custom Separator Example ===\n")

    # Use a dot separator instead of underscore
    result = tm.flatten(deeply_nested_data, name="customer_data", separator=".")

    # Show tables
    print("Generated tables with dot separator:")
    for table_name in sorted(result.tables.keys()):
        print(f"  {table_name}: {len(result.tables[table_name])} records")

    # Show some field names with dot separator
    print("\nSample field names with dot separator:")
    main_record = result.main[0]
    for key in sorted(main_record.keys())[:5]:
        if "." in key:
            print(f"  {key}")


if __name__ == "__main__":
    main()
