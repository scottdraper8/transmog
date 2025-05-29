#!/usr/bin/env python3
"""Example script demonstrating how to convert between multiple formats using Transmog.

This example shows converting from JSON to CSV, Parquet, and Excel formats.
"""

import json
import os

from transmog import Processor


def main():
    """Main function to demonstrate multi-format conversion capabilities."""
    # Initialize processor
    processor = Processor()

    # Create sample JSON data
    sample_data = [
        {
            "id": 1,
            "name": "Product A",
            "price": 19.99,
            "in_stock": True,
            "tags": ["electronics", "gadgets"],
        },
        {
            "id": 2,
            "name": "Product B",
            "price": 29.99,
            "in_stock": False,
            "tags": ["clothing", "accessories"],
        },
        {
            "id": 3,
            "name": "Product C",
            "price": 9.99,
            "in_stock": True,
            "tags": ["home", "kitchen"],
        },
    ]

    # Create output directory if it doesn't exist
    os.makedirs("output", exist_ok=True)

    # Save sample data to JSON file
    sample_file = "output/sample_data.json"
    with open(sample_file, "w") as f:
        json.dump(sample_data, f, indent=2)

    print(f"Created sample JSON data in {sample_file}")

    # Process the JSON data
    print("Processing JSON data...")
    result = processor.process(sample_data, entity_name="products")

    # Get the main table
    main_table = result.get_main_table()

    # Print some info about the data
    print(f"Processed {len(main_table)} records")
    if main_table:
        print(f"Fields: {list(main_table[0].keys())}")
    else:
        print("No records found")

    # Write to various formats

    # 1. Write to CSV
    csv_output = "output/converted_data.csv"
    print(f"Writing CSV to {csv_output}")
    result.write("csv", "output")

    # 2. Write to JSON with pretty formatting
    json_output = "output/converted_data.json"
    print(f"Writing JSON to {json_output}")
    result.write("json", "output", indent=2)

    # Check if Parquet support is available
    try:
        # 3. Write to Parquet if pyarrow is available
        result.write("parquet", "output")
        print("Writing Parquet to output/products.parquet")
    except Exception as e:
        print(f"Skipping Parquet output: {e}")

    print("Format conversion complete!")
    print("Files have been saved to the 'output' directory.")


if __name__ == "__main__":
    main()
