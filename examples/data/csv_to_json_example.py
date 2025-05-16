#!/usr/bin/env python3
"""Demonstrates how to convert CSV to JSON with transformations using Transmog."""

from transmog import Processor


def transform_customer_data(record):
    """Transforms customer data.

    - Converting names to title case
    - Calculating age category
    - Adding a full_name field.
    """
    # Handle name fields
    if "first_name" in record and record["first_name"]:
        record["first_name"] = record["first_name"].title()

    if "last_name" in record and record["last_name"]:
        record["last_name"] = record["last_name"].title()

    # Add full name
    if "first_name" in record and "last_name" in record:
        if record["first_name"] and record["last_name"]:
            record["full_name"] = f"{record['first_name']} {record['last_name']}"
        elif record["first_name"]:
            record["full_name"] = record["first_name"]
        elif record["last_name"]:
            record["full_name"] = record["last_name"]
        else:
            record["full_name"] = None

    # Add age category
    if "age" in record and record["age"] is not None:
        try:
            age = int(record["age"])
            if age < 18:
                record["age_category"] = "minor"
            elif age < 65:
                record["age_category"] = "adult"
            else:
                record["age_category"] = "senior"
        except (ValueError, TypeError):
            # Handle case where age isn't a valid number
            record["age_category"] = None

    # Convert active field to boolean
    if "active" in record:
        if isinstance(record["active"], str):
            record["active"] = record["active"].lower() in [
                "true",
                "yes",
                "1",
                "t",
                "y",
            ]

    return record


def main():
    """Main function to demonstrate CSV to JSON conversion."""
    # Initialize processor
    processor = Processor()

    # Define values that should be treated as null
    null_values = ["", "NULL", "N/A", "NA", "-", "none"]

    # Process the CSV file with transformations
    print("Processing CSV data...")
    result = processor.process_csv(
        "tests/fixtures/sample_with_nulls.csv",
        entity_name="customers",
        transform_function=transform_customer_data,
        null_values=null_values,
        infer_types=True,
    )

    # Show some stats about the processed data
    print(f"Processed {result.get_main_table().get_row_count()} records")
    print(f"Fields: {result.get_main_table().get_fields()}")

    # Write the transformed data to JSON
    output_file = "output/transformed_data.json"
    print(f"Writing transformed data to {output_file}")
    result.write_json(output_file, pretty=True)

    # Also write back to CSV to see the transformed values
    csv_output = "output/transformed_data.csv"
    print(f"Writing transformed data to {csv_output}")
    result.write_csv(csv_output)

    print("Transformation complete")


if __name__ == "__main__":
    main()
