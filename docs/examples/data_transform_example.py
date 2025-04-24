#!/usr/bin/env python3
"""
Example script demonstrating data transformation with Transmog.
This example shows how to map fields and convert data types during transformation.
"""

from transmog import Processor, FieldMap, DataType


def main():
    # Initialize processor
    processor = Processor()

    # Sample input file
    input_file = "tests/fixtures/sample_with_nulls.csv"

    print(f"Processing and transforming data from {input_file}...")

    # Define field mappings
    field_maps = [
        # Simple rename: 'id' -> 'user_id'
        FieldMap(source_field="id", target_field="user_id", data_type=DataType.INTEGER),
        # Rename and format: 'name' -> 'full_name' with uppercase transformation
        FieldMap(
            source_field="name",
            target_field="full_name",
            data_type=DataType.STRING,
            transform=lambda x: x.upper() if x else None,
        ),
        # Type conversion: 'age' -> 'age_years' as integer
        FieldMap(
            source_field="age",
            target_field="age_years",
            data_type=DataType.INTEGER,
            default_value=0,  # Use default value when null
        ),
        # Boolean to string conversion: 'active' -> 'status'
        FieldMap(
            source_field="active",
            target_field="status",
            data_type=DataType.STRING,
            transform=lambda x: "Active" if x else "Inactive",
        ),
        # Float to string with formatting: 'score' -> 'grade'
        FieldMap(
            source_field="score",
            target_field="grade",
            data_type=DataType.STRING,
            transform=lambda x: f"{float(x):.1f}%" if x is not None else "N/A",
        ),
        # Computed field - not directly mapped from source
        FieldMap(
            target_field="record_status",
            data_type=DataType.STRING,
            compute=lambda row: "Complete" if all(row.values()) else "Incomplete",
        ),
    ]

    # Process the CSV file with transformations
    result = processor.process_csv(
        input_file,
        entity_name="users",
        field_maps=field_maps,
        null_values=["", "NULL", "N/A", "NA", "-", "none"],
    )

    # Create output directory if it doesn't exist
    import os

    os.makedirs("output", exist_ok=True)

    # Write the transformed data
    output_file = "output/transformed_data.csv"
    result.write_csv(output_file)

    # Show transformation summary
    print(f"\nTransformation complete")
    print(f"Records processed: {result.record_count}")
    print(f"Output fields: {', '.join(result.field_names)}")
    print(f"Transformed data written to {output_file}")

    # Also save as JSON for inspection
    json_output = "output/transformed_data.json"
    result.write_json(json_output, indent=2)
    print(f"JSON version saved to {json_output}")


if __name__ == "__main__":
    main()
