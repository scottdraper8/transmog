"""Example Name: Output Formats.

Demonstrates: Working with different output formats in Transmog.

Related Documentation:
- https://transmog.readthedocs.io/en/latest/user/output/output-formats.html
- https://transmog.readthedocs.io/en/latest/api/io.html

Learning Objectives:
- How to output processed data in different formats
- How to configure output format options
- How to optimize output for different use cases
- How to handle output file naming and organization
"""

import csv
import json
import os

# Import from transmog package
import transmog as tm


def generate_sample_data():
    """Generate sample data with various data types and structures."""
    return {
        "organization": {
            "id": 42,
            "name": "Example Corp",
            "founded": 1995,
            "active": True,
            "headquarters": {
                "address": "123 Main St",
                "city": "San Francisco",
                "state": "CA",
                "zip": "94105",
                "geo": {"latitude": 37.7749, "longitude": -122.4194},
            },
            "metrics": {
                "employees": 500,
                "revenue": 10500000.50,
                "growth_rate": 0.15,
                "public": True,
            },
            "departments": [
                {"id": 1, "name": "Engineering", "budget": 2000000, "headcount": 200},
                {"id": 2, "name": "Marketing", "budget": 1500000, "headcount": 100},
                {"id": 3, "name": "Sales", "budget": 3000000, "headcount": 150},
            ],
            "locations": [
                {
                    "id": 101,
                    "name": "Headquarters",
                    "type": "office",
                    "address": {
                        "street": "123 Main St",
                        "city": "San Francisco",
                        "state": "CA",
                    },
                },
                {
                    "id": 102,
                    "name": "East Coast Office",
                    "type": "office",
                    "address": {
                        "street": "456 Park Ave",
                        "city": "New York",
                        "state": "NY",
                    },
                },
                {
                    "id": 103,
                    "name": "Warehouse",
                    "type": "warehouse",
                    "address": {
                        "street": "789 Industrial Blvd",
                        "city": "Dallas",
                        "state": "TX",
                    },
                },
            ],
        }
    }


def main():
    """Run the output formats example."""
    # Create output directory
    output_dir = os.path.join(
        os.path.dirname(__file__), "..", "data", "output", "formats"
    )
    os.makedirs(output_dir, exist_ok=True)

    # Generate sample data
    data = generate_sample_data()

    print("=== Output Formats Example ===")

    # Process the data
    processor = tm.Processor()
    result = processor.process(data=data, entity_name="company")

    print(f"Processed data into {len(result.get_table_names()) + 1} tables")

    # Example 1: JSON Output
    print("\n=== JSON Output ===")

    # Get JSON as Python objects
    json_objects = result.to_json_objects()
    print(f"Generated {len(json_objects)} JSON object collections")

    # Get JSON as bytes
    json_bytes = result.to_json_bytes(indent=2)
    print(f"Generated {len(json_bytes)} JSON byte collections")
    print(f"Total JSON bytes size: {sum(len(v) for v in json_bytes.values())} bytes")

    # Write to JSON files
    json_dir = os.path.join(output_dir, "json")
    json_files = result.write_all_json(base_path=json_dir, indent=2)
    print(f"Wrote {len(json_files)} JSON files to: {json_dir}")

    # Extract sample JSON for display
    if json_files:
        with open(json_files[0]) as f:
            json_content = json.load(f)
            print("\nSample JSON output (first 3 fields of first record):")
            first_record = json_content[0] if json_content else {}
            sample = {
                k: first_record[k]
                for k in list(first_record.keys())[:3]
                if k in first_record
            }
            print(json.dumps(sample, indent=2))

    # Example 2: CSV Output
    print("\n=== CSV Output ===")

    # Get CSV as bytes
    csv_bytes = result.to_csv_bytes()
    print(f"Generated {len(csv_bytes)} CSV byte collections")
    print(f"Total CSV bytes size: {sum(len(v) for v in csv_bytes.values())} bytes")

    # Write to CSV files
    csv_dir = os.path.join(output_dir, "csv")
    csv_files = result.write_all_csv(base_path=csv_dir)
    print(f"Wrote {len(csv_files)} CSV files to: {csv_dir}")

    # Extract sample CSV for display
    if csv_files:
        with open(csv_files[0], newline="") as f:
            reader = csv.reader(f)
            header = next(reader, [])
            first_row = next(reader, [])
            if header and first_row:
                print("\nSample CSV output (first 3 fields):")
                for i in range(min(3, len(header))):
                    print(f"{header[i]}: {first_row[i]}")

    # Example 3: Parquet Output
    print("\n=== Parquet Output ===")

    try:
        # Get PyArrow tables
        pa_tables = result.to_pyarrow_tables()
        print(f"Generated {len(pa_tables)} PyArrow tables")

        # Get Parquet as bytes
        parquet_bytes = result.to_parquet_bytes(compression="snappy")
        print(f"Generated {len(parquet_bytes)} Parquet byte collections")
        print(
            f"Total Parquet bytes size: "
            f"{sum(len(v) for v in parquet_bytes.values())} bytes"
        )

        # Write to Parquet files
        parquet_dir = os.path.join(output_dir, "parquet")
        parquet_files = result.write_all_parquet(
            base_path=parquet_dir, compression="snappy"
        )
        print(f"Wrote {len(parquet_files)} Parquet files to: {parquet_dir}")

        # Show Parquet schema for a sample table
        sample_table = next(iter(pa_tables.values()), None)
        if sample_table:
            print("\nSample Parquet schema:")
            for field in sample_table.schema.names[:5]:  # Show first 5 fields
                field_type = sample_table.schema.field(field).type
                print(f"{field}: {field_type}")

    except ImportError:
        print("PyArrow not available. Install with: pip install pyarrow")

    # Example 4: Type-Preserved Output
    print("\n=== Type-Preserved Output ===")

    # Create processor with type preservation
    type_config = tm.TransmogConfig.default().with_processing(
        cast_to_string=False,  # Don't cast values to strings
        cast_from_string=True,  # Try to parse strings to appropriate types
        skip_null=False,  # Include null values for complete schema
    )

    type_processor = tm.Processor(config=type_config)
    type_result = type_processor.process(data=data, entity_name="company")

    # Get Python dictionaries with original types
    typed_dicts = type_result.to_dict()

    # Show a sample record with type information
    main_table = typed_dicts.get("company", [])
    if main_table:
        record = main_table[0]
        print("\nSample with preserved types:")
        for key, value in record.items():
            if not key.startswith("__") and value is not None:  # Skip metadata and null
                print(f"{key} ({type(value).__name__}): {value}")

    # Try writing typed data to Parquet (good for preserving schema)
    try:
        typed_parquet_dir = os.path.join(output_dir, "typed_parquet")
        typed_parquet_files = type_result.write_all_parquet(
            base_path=typed_parquet_dir, compression="snappy"
        )
        print(f"Wrote {len(typed_parquet_files)} type-preserved Parquet files")

    except ImportError:
        print("PyArrow not available. Type-preserved Parquet output skipped.")

    # Example 5: Custom File Naming
    print("\n=== Custom File Naming ===")

    # Create processor with custom naming configuration
    naming_config = tm.TransmogConfig.default().with_naming(
        separator=".",  # Use dots in field names
        abbreviate_table_names=True,  # Abbreviate table names
        max_table_component_length=10,  # Limit name component length
        separator_replacement="_",  # Replace separators in input keys
    )

    naming_processor = tm.Processor(config=naming_config)
    naming_result = naming_processor.process(data=data, entity_name="company")

    # Show the table names that will be used for files
    print("\nAbbreviated table names:")
    for original_name in naming_result.get_table_names():
        formatted_name = naming_result.get_formatted_table_name(original_name)
        print(f"Original: {original_name}")
        print(f"Formatted: {formatted_name}")

    # Write to files with custom naming
    custom_dir = os.path.join(output_dir, "custom_naming")
    custom_files = naming_result.write_all_json(base_path=custom_dir, indent=2)
    print(f"Wrote {len(custom_files)} files with custom naming to: {custom_dir}")

    print(f"\nAll output files written to: {output_dir}")


if __name__ == "__main__":
    main()
