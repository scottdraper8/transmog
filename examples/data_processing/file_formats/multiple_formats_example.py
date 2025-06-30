#!/usr/bin/env python3
"""Example script demonstrating how to convert between multiple formats using Transmog v1.1.0.

This example shows converting from JSON to CSV, Parquet, and other formats
using both the simple API and advanced features.
"""

import json
import os

import transmog as tm


def main():
    """Main function to demonstrate multi-format conversion capabilities."""
    # Create sample JSON data
    sample_data = [
        {
            "id": 1,
            "name": "Product A",
            "price": 19.99,
            "in_stock": True,
            "tags": ["electronics", "gadgets"],
            "specs": {"weight": "2.5kg", "dimensions": {"width": 10, "height": 5}},
        },
        {
            "id": 2,
            "name": "Product B",
            "price": 29.99,
            "in_stock": False,
            "tags": ["clothing", "accessories"],
            "specs": {"size": "M", "color": "blue"},
        },
        {
            "id": 3,
            "name": "Product C",
            "price": 9.99,
            "in_stock": True,
            "tags": ["home", "kitchen"],
            "specs": {"material": "stainless steel", "capacity": "2L"},
        },
    ]

    # Create output directory if it doesn't exist
    os.makedirs("output", exist_ok=True)

    # Save sample data to JSON file for demonstration
    sample_file = "output/sample_data.json"
    with open(sample_file, "w") as f:
        json.dump(sample_data, f, indent=2)

    print(f"Created sample JSON data in {sample_file}")

    # Example 1: Basic conversion using the simple API
    print("\n=== Example 1: Basic Multi-Format Conversion ===")

    # Process the data using the simple API
    result = tm.flatten(sample_data, name="products", id_field="id")

    # Print some info about the data
    print(f"Processed {len(result.main)} main records")
    print(f"Child tables: {list(result.tables.keys())}")
    print(f"Total tables: {len(result.all_tables)}")

    if result.main:
        print(f"Main table fields: {list(result.main[0].keys())}")

    # Convert to different formats using the simple save method
    print("\nSaving to different formats...")

    # Save as JSON
    result.save("output/products.json")
    print("✓ Saved as JSON")

    # Save as CSV (will create multiple files if there are child tables)
    csv_files = result.save("output/products_csv/")
    print(f"✓ Saved as CSV: {len(csv_files)} files created")

    # Save as Parquet
    try:
        parquet_files = result.save("output/products.parquet")
        print("✓ Saved as Parquet")
    except Exception as e:
        print(f"⚠ Parquet not available: {e}")

    # Example 2: File processing with format conversion
    print("\n=== Example 2: File-to-File Conversion ===")

    # Process the JSON file directly
    file_result = tm.flatten_file(sample_file, name="file_products")

    # Save to different formats
    file_result.save("output/from_file.csv")
    print("✓ Converted JSON file to CSV")

    # Example 3: Streaming conversion for large datasets
    print("\n=== Example 3: Streaming Conversion (for large files) ===")

    # For demonstration, use the same small dataset
    # In practice, this would be used for very large files

    # Stream directly to JSON
    tm.flatten_stream(
        sample_data, "output/streamed_json/", name="streamed_products", format="json"
    )
    print("✓ Streamed to JSON files")

    # Stream to CSV
    tm.flatten_stream(
        sample_data, "output/streamed_csv/", name="streamed_products", format="csv"
    )
    print("✓ Streamed to CSV files")

    # Stream to Parquet with compression
    try:
        tm.flatten_stream(
            sample_data,
            "output/streamed_parquet/",
            name="streamed_products",
            format="parquet",
            compression="snappy",
        )
        print("✓ Streamed to compressed Parquet files")
    except Exception as e:
        print(f"⚠ Parquet streaming not available: {e}")

    # Example 4: Advanced format options
    print("\n=== Example 4: Advanced Format Options ===")

    # For advanced features, use the Processor directly
    from transmog.process import Processor
    from transmog.config import TransmogConfig

    # Create custom configuration
    config = (
        TransmogConfig.default()
        .with_naming(separator=".")  # Use dots instead of underscores
        .with_processing(cast_to_string=False)  # Keep original types
    )

    processor = Processor(config)
    advanced_result = processor.process(sample_data, entity_name="advanced_products")

    # Save with custom options
    advanced_result.write("json", "output/advanced", indent=4)
    print("✓ Saved with advanced formatting (dot notation, preserved types)")

    print("\n=== Summary ===")
    print("Transmog v1.1.0 provides multiple ways to handle format conversion:")
    print("1. Simple API (tm.flatten + result.save): Best for most use cases")
    print("2. File processing (tm.flatten_file): Direct file-to-file conversion")
    print("3. Streaming (tm.flatten_stream): Memory-efficient for large datasets")
    print("4. Advanced Processor: Full control over processing options")
    print("\nAll output files saved to the 'output/' directory.")


if __name__ == "__main__":
    main()
