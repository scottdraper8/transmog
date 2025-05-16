#!/usr/bin/env python3
"""Example script demonstrating how to convert between multiple formats using Transmog.

This example shows converting from JSON to CSV, Parquet, and Excel formats.
"""

from transmog import Processor


def main():
    """Main function to demonstrate multi-format conversion capabilities."""
    # Initialize processor
    processor = Processor()

    # Sample input file (assuming JSON format)
    input_file = "data/sample_data.json"

    print(f"Processing JSON data from {input_file}...")

    # Process the JSON file
    result = processor.process_json(input_file, infer_types=True)

    # Print some info about the data
    print(f"Processed {result.get_main_table().get_row_count()} records")
    print(f"Fields: {result.get_main_table().get_fields()}")

    # Create output directory if it doesn't exist
    import os

    os.makedirs("output", exist_ok=True)

    # Write to various formats

    # 1. Write to CSV
    csv_output = "output/converted_data.csv"
    print(f"Writing CSV to {csv_output}")
    result.write_csv(csv_output)

    # 2. Write to Parquet
    parquet_output = "output/converted_data.parquet"
    print(f"Writing Parquet to {parquet_output}")
    result.write_parquet(parquet_output)

    # 3. Write to Excel
    excel_output = "output/converted_data.xlsx"
    print(f"Writing Excel to {excel_output}")
    result.write_excel(excel_output)

    # 4. Write back to JSON with pretty formatting
    json_output = "output/converted_data.json"
    print(f"Writing JSON to {json_output}")
    result.write_json(json_output, pretty=True)

    print("Format conversion complete!")
    print("Files have been saved to the 'output' directory.")


if __name__ == "__main__":
    main()
