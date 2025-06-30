"""CSV Processing Example.

Demonstrates how to work with CSV files using Transmog.

Learning Objectives:
- Processing CSV files with automatic type inference
- Handling CSV-specific options
- Working with different output formats
"""

import os

import transmog as tm


def create_sample_csv(filepath):
    """Create a sample CSV file for testing."""
    csv_content = """id,name,department,salary,hire_date,address_street,address_city,address_state,skills
1,John Doe,Engineering,75000,2020-01-15,123 Tech St,San Francisco,CA,"Python,JavaScript,SQL"
2,Jane Smith,Marketing,65000,2019-03-22,456 Market Ave,New York,NY,"Marketing,Analytics,Excel"
3,Bob Johnson,Engineering,80000,2018-11-08,789 Dev Rd,Seattle,WA,"Java,Python,Docker"
4,Alice Williams,HR,70000,2021-02-01,321 People Ln,Chicago,IL,"Recruiting,HRIS,Communication"
5,Charlie Brown,Sales,72000,2020-06-15,654 Sales Blvd,Boston,MA,"CRM,Negotiation,Presentation"
"""
    with open(filepath, "w") as f:
        f.write(csv_content)


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

    # Example 1: Basic CSV Processing
    print("\n=== Basic CSV Processing ===")

    # Process CSV file - it's this simple!
    result = tm.flatten(csv_filepath, name="employees")

    # Print the main table
    print("\nMain Table (First 3 rows):")
    for i, record in enumerate(result.main[:3]):
        print(
            f"Row {i + 1}: id={record.get('id', 'N/A')}, "
            f"name={record.get('name', 'N/A')}, "
            f"department={record.get('department', 'N/A')}"
        )

    print(f"\nTotal records processed: {len(result.main)}")
    print(f"Fields detected: {list(result.main[0].keys())[:5]}...")

    # Example 2: Processing with Type Preservation
    print("\n=== Processing with Type Preservation ===")

    # Process with original types preserved
    result = tm.flatten(csv_filepath, name="employees", preserve_types=True)

    # Show data types
    if result.main:
        record = result.main[0]
        print("\nData types:")
        print(f"  id: {type(record.get('id'))}")
        print(f"  name: {type(record.get('name'))}")
        print(f"  salary: {type(record.get('salary'))}")
        print(f"  hire_date: {type(record.get('hire_date'))}")

    # Example 3: Custom Separator for Nested Fields
    print("\n=== Custom Separator ===")

    # Process with dot notation
    result = tm.flatten(csv_filepath, name="employees", separator=".")

    # Show nested field names
    print("\nAddress fields with dot notation:")
    address_fields = [k for k in result.main[0].keys() if "address" in k]
    for field in address_fields:
        print(f"  {field}: {result.main[0][field]}")

    # Example 4: Handling Skills as Arrays
    print("\n=== Skills Array Handling ===")

    # Note: CSV doesn't have native array support, but we can demonstrate
    # how the data is handled
    print("\nSkills field handling:")
    for i, record in enumerate(result.main[:3]):
        skills = record.get("skills", "")
        print(f"Employee {record.get('name')}: skills = '{skills}'")

    # Example 5: Output to Different Formats
    print("\n=== Output to Different Formats ===")

    # Save to JSON
    json_path = os.path.join(csv_processing_dir, "employees.json")
    result.save(json_path)
    print(f"Saved to JSON: {json_path}")

    # Save back to CSV
    csv_output = os.path.join(csv_processing_dir, "employees_processed.csv")
    result.save(csv_output)
    print(f"Saved to CSV: {csv_output}")

    # Save to Parquet (if available)
    try:
        parquet_path = os.path.join(csv_processing_dir, "employees.parquet")
        result.save(parquet_path)
        print(f"Saved to Parquet: {parquet_path}")
    except ImportError:
        print("Parquet requires pyarrow: pip install pyarrow")

    # Example 6: Error Handling with Bad Data
    print("\n=== Error Handling ===")

    # Create a CSV with some problematic data
    bad_csv = os.path.join(csv_processing_dir, "bad_data.csv")
    with open(bad_csv, "w") as f:
        f.write("id,name,age\n")
        f.write("1,Alice,25\n")
        f.write("bad_id,Bob,thirty\n")  # Bad data
        f.write("3,Charlie,30\n")

    # Process with error skipping
    result = tm.flatten(bad_csv, name="people", errors="skip")
    print(f"\nWith errors='skip': Processed {len(result.main)} records")

    # Example 7: Performance Optimization
    print("\n=== Performance Optimization ===")

    # For large CSV files, use low memory mode
    result = tm.flatten(csv_filepath, name="employees", low_memory=True)
    print("\nProcessed in low memory mode")

    # Or use custom batch size
    result = tm.flatten(csv_filepath, name="employees", batch_size=2)
    print("Processed with custom batch size")

    print(f"\nAll output files written to: {csv_processing_dir}")


if __name__ == "__main__":
    main()
