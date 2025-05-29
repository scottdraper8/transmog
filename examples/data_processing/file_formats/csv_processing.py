"""Example Name: CSV Processing.

Demonstrates: Working with CSV data in Transmog.

Related Documentation:
- https://transmog.readthedocs.io/en/latest/user/processing/csv-processing.html
- https://transmog.readthedocs.io/en/latest/api/csv-reader.html

Learning Objectives:
- How to read data from CSV files
- How to handle CSV-specific processing options
- How to configure CSV reading behavior
- How to process and transform CSV data
"""

import csv
import os

# Import from transmog package
import transmog as tm


def create_sample_csv(filepath):
    """Create a sample CSV file for demonstration."""
    with open(filepath, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(
            [
                "id",
                "name",
                "department",
                "salary",
                "hire_date",
                "manager_id",
                "address_city",
                "address_state",
                "skills",
            ]
        )
        # Write data rows
        writer.writerows(
            [
                [
                    "1",
                    "John Smith",
                    "Engineering",
                    "85000",
                    "2020-01-15",
                    "5",
                    "San Francisco",
                    "CA",
                    "Python,Java,SQL",
                ],
                [
                    "2",
                    "Alice Johnson",
                    "Marketing",
                    "75000",
                    "2019-05-20",
                    "6",
                    "New York",
                    "NY",
                    "SEO,Content,Analytics",
                ],
                [
                    "3",
                    "Robert Davis",
                    "Engineering",
                    "90000",
                    "2018-11-10",
                    "5",
                    "Austin",
                    "TX",
                    "C++,Go,Rust",
                ],
                [
                    "4",
                    "Maria Garcia",
                    "Finance",
                    "95000",
                    "2021-03-22",
                    "7",
                    "Chicago",
                    "IL",
                    "Accounting,Excel,Tableau",
                ],
                [
                    "5",
                    "David Wilson",
                    "Engineering",
                    "120000",
                    "2015-08-05",
                    "",
                    "Seattle",
                    "WA",
                    "Python,Management,Architecture",
                ],
                [
                    "6",
                    "Susan Lee",
                    "Marketing",
                    "110000",
                    "2016-02-28",
                    "",
                    "Boston",
                    "MA",
                    "Strategy,Leadership,Analytics",
                ],
                [
                    "7",
                    "James Brown",
                    "Finance",
                    "115000",
                    "2017-10-15",
                    "",
                    "Denver",
                    "CO",
                    "Budgeting,Forecasting,Investment",
                ],
            ]
        )

    return filepath


def main():
    """Run the CSV processing example."""
    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "..", "..", "output")
    os.makedirs(output_dir, exist_ok=True)

    # Create subdirectories for different outputs
    csv_processing_dir = os.path.join(output_dir, "csv_processing")
    os.makedirs(csv_processing_dir, exist_ok=True)

    # Create sample CSV file
    csv_filepath = os.path.join(csv_processing_dir, "sample_employees.csv")
    create_sample_csv(csv_filepath)

    print("=== CSV Processing Example ===")
    print(f"Created sample CSV file: {csv_filepath}")

    # Example 1: Basic CSV Reading
    print("\n=== Basic CSV Reading ===")

    # Create a processor with default configuration
    processor = tm.Processor()

    # Process the CSV file with default settings
    csv_result = processor.process_file(file_path=csv_filepath, entity_name="employees")

    # Print the main table
    print("\nMain Table (First 3 rows):")
    main_table = csv_result.get_main_table()
    for i, record in enumerate(main_table[:3]):
        print(
            f"Row {i + 1}: id={record.get('id', 'N/A')}, "
            f"name={record.get('name', 'N/A')}, "
            f"department={record.get('department', 'N/A')}"
        )

    print(f"\nTotal records processed: {len(main_table)}")

    # Example 2: CSV With Custom Configuration
    print("\n=== CSV With Custom Configuration ===")

    # Create a custom configuration for CSV processing
    csv_config = tm.TransmogConfig.default().with_processing(
        skip_null=False,  # Include null values
        include_empty=True,  # Include empty values
        cast_to_string=False,  # Maintain original types
    )

    # Create processor with custom configuration
    csv_processor = tm.Processor(config=csv_config)

    # Process the CSV file with custom configuration
    custom_csv_result = csv_processor.process_file(
        file_path=csv_filepath, entity_name="employees"
    )

    # Print results with custom configuration
    main_table = custom_csv_result.get_main_table()
    print("\nProcessed with custom configuration:")
    if main_table:
        # Show a sample record with field types
        record = main_table[0]
        print("Sample record with types:")
        for key in ["id", "name", "salary", "hire_date"]:
            if key in record:
                print(f"{key} ({type(record[key]).__name__}): {record[key]}")

    # Example 3: CSV Transformation with Nested Structure
    print("\n=== CSV Transformation with Nested Structure ===")

    # Transform flat CSV structure into a nested JSON structure
    # using a processor that recognizes prefixes

    # Create nested structure interpretation configuration
    nested_config = (
        tm.TransmogConfig.default()
        .with_naming(
            separator="_"  # Use underscore as separator for nested path components
        )
        .with_processing(
            cast_to_string=False,  # Keep original types
        )
    )

    nested_processor = tm.Processor(config=nested_config)

    # Process CSV with nested structure interpretation
    nested_result = nested_processor.process_file(
        file_path=csv_filepath, entity_name="employees"
    )

    # Print the nested structure
    print("\nCSV interpreted with nested structure:")
    main_table = nested_result.get_main_table()
    if main_table:
        # Show how the address fields are grouped
        record = main_table[0]
        nested_fields = [k for k in record.keys() if "address" in k]
        print("Address fields after nesting interpretation:")
        for field in nested_fields:
            print(f"  {field}: {record[field]}")

    # Example 4: CSV Processing with Skills Array
    print("\n=== CSV Processing with Skills Array ===")

    # Create configuration for array splitting
    # Note: This is a workaround as array splitting might need to be
    # implemented differently in the latest version
    array_config = tm.TransmogConfig.default().with_processing(
        visit_arrays=True,  # Process arrays into child tables
        cast_to_string=False,  # Keep original types
    )

    array_processor = tm.Processor(config=array_config)

    # Process CSV with array splitting
    array_result = array_processor.process_file(
        file_path=csv_filepath, entity_name="employees"
    )

    # Print the skills arrays
    print("\nSkills processed as arrays:")
    main_table = array_result.get_main_table()
    if main_table:
        for _i, record in enumerate(main_table[:3]):
            skills = record.get("skills", [])
            if isinstance(skills, list):
                print(
                    f"Employee {record.get('name')}: {len(skills)} skills - "
                    f"{', '.join(skills)}"
                )
            else:
                print(f"Employee {record.get('name')}: skills not processed as array")

    # Example 5: Output Processed CSV to Different Formats
    print("\n=== Output Processed CSV to Different Formats ===")

    # Write to JSON
    json_dir = os.path.join(csv_processing_dir, "json")
    json_outputs = csv_result.write_all_json(base_path=json_dir)
    print(f"Wrote {len(json_outputs)} JSON files to {json_dir}")

    # Write to CSV (re-export)
    csv_dir = os.path.join(csv_processing_dir, "csv_export")
    csv_outputs = csv_result.write_all_csv(base_path=csv_dir)
    print(f"Wrote {len(csv_outputs)} CSV files to {csv_dir}")

    # Try to write to Parquet if available
    try:
        parquet_dir = os.path.join(csv_processing_dir, "parquet")
        parquet_outputs = csv_result.write_all_parquet(base_path=parquet_dir)
        print(f"Wrote {len(parquet_outputs)} Parquet files to {parquet_dir}")
    except ImportError:
        print("PyArrow not available. Parquet output skipped.")

    print(f"\nAll output files written to: {csv_processing_dir}")


if __name__ == "__main__":
    main()
