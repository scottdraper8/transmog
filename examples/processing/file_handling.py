"""Example Name: File Handling.

Demonstrates: File input and output operations in Transmog.

Related Documentation:
- https://transmog.readthedocs.io/en/latest/user/processing/file-processing.html
- https://transmog.readthedocs.io/en/latest/user/processing/io.html

Learning Objectives:
- How to read data from files in different formats
- How to write processed data to different file formats
- How to work with file paths and directories
- How to handle file input/output configurations
"""

import csv
import json
import os

# Import from transmog package
import transmog as tm


def create_sample_files(base_dir):
    """Create sample files in various formats for demonstration."""
    # Create sample directories
    input_dir = os.path.join(base_dir, "input")
    os.makedirs(input_dir, exist_ok=True)

    # Sample JSON data
    json_data = {
        "company": {
            "id": 42,
            "name": "TechCorp",
            "founded": 2010,
            "departments": [
                {
                    "id": 1,
                    "name": "Engineering",
                    "employees": [
                        {"id": 101, "name": "Alice", "role": "Developer"},
                        {"id": 102, "name": "Bob", "role": "QA Engineer"},
                    ],
                },
                {
                    "id": 2,
                    "name": "Marketing",
                    "employees": [
                        {"id": 201, "name": "Charlie", "role": "Strategist"},
                        {"id": 202, "name": "Diana", "role": "Content Manager"},
                    ],
                },
            ],
        }
    }

    # Write JSON file
    json_path = os.path.join(input_dir, "company.json")
    with open(json_path, "w") as f:
        json.dump(json_data, f, indent=2)

    # Write CSV file
    csv_path = os.path.join(input_dir, "employees.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "department", "role", "hire_date"])
        writer.writerow(["101", "Alice", "Engineering", "Developer", "2020-01-15"])
        writer.writerow(["102", "Bob", "Engineering", "QA Engineer", "2020-02-20"])
        writer.writerow(["201", "Charlie", "Marketing", "Strategist", "2019-11-10"])
        writer.writerow(["202", "Diana", "Marketing", "Content Manager", "2021-03-05"])

    # Write a text file with JSON lines
    jsonl_path = os.path.join(input_dir, "events.jsonl")
    with open(jsonl_path, "w") as f:
        f.write(
            json.dumps(
                {"event": "login", "user_id": 101, "timestamp": "2023-01-01T10:00:00"}
            )
            + "\n"
        )
        f.write(
            json.dumps(
                {
                    "event": "page_view",
                    "user_id": 101,
                    "page": "home",
                    "timestamp": "2023-01-01T10:01:30",
                }
            )
            + "\n"
        )
        f.write(
            json.dumps(
                {"event": "login", "user_id": 202, "timestamp": "2023-01-01T11:15:00"}
            )
            + "\n"
        )
        f.write(
            json.dumps(
                {
                    "event": "page_view",
                    "user_id": 202,
                    "page": "products",
                    "timestamp": "2023-01-01T11:16:45",
                }
            )
            + "\n"
        )

    return {
        "input_dir": input_dir,
        "json_path": json_path,
        "csv_path": csv_path,
        "jsonl_path": jsonl_path,
    }


def main():
    """Run the file handling example."""
    # Create output directory
    output_dir = os.path.join(
        os.path.dirname(__file__), "..", "data", "output", "file_handling"
    )
    os.makedirs(output_dir, exist_ok=True)

    # Create sample files
    file_paths = create_sample_files(output_dir)
    input_dir = file_paths["input_dir"]
    json_path = file_paths["json_path"]
    csv_path = file_paths["csv_path"]
    jsonl_path = file_paths["jsonl_path"]

    print("=== File Handling Example ===")
    print(f"Created sample files in: {input_dir}")

    # Example 1: Reading from JSON File
    print("\n=== Reading from JSON File ===")

    # Create a processor with default configuration
    processor = tm.Processor()

    # Process the JSON file
    json_result = processor.process_file(
        file_path=json_path, entity_name="organization", file_format="json"
    )

    # Print information about the processed JSON
    print(f"Processed JSON file: {json_path}")
    print(f"Main table size: {len(json_result.get_main_table())} records")
    print("Child tables:")
    for table_name in json_result.get_table_names():
        table_data = json_result.get_child_table(table_name)
        print(f"- {table_name}: {len(table_data)} records")

    # Example 2: Reading from CSV File
    print("\n=== Reading from CSV File ===")

    # Process the CSV file
    csv_result = processor.process_file(
        file_path=csv_path, entity_name="staff", file_format="csv"
    )

    # Print information about the processed CSV
    print(f"Processed CSV file: {csv_path}")
    print(f"Main table size: {len(csv_result.get_main_table())} records")
    if csv_result.get_main_table():
        print("Sample fields from first record:")
        record = csv_result.get_main_table()[0]
        for key in list(record.keys())[:5]:  # Print first 5 fields
            print(f"  {key}: {record[key]}")

    # Example 3: Reading Multiple Files
    print("\n=== Reading Multiple Files ===")

    # Create configuration for batch file processing
    batch_config = tm.TransmogConfig.default().with_processing(
        processing_mode=tm.ProcessingMode.MEMORY_OPTIMIZED
    )

    batch_processor = tm.Processor(config=batch_config)

    # Process multiple files
    file_results = {}
    for file_path, entity_name, format_name in [
        (json_path, "organization", "json"),
        (csv_path, "staff", "csv"),
    ]:
        print(f"Processing {os.path.basename(file_path)} as {entity_name}...")
        result = batch_processor.process_file(
            file_path=file_path, entity_name=entity_name, file_format=format_name
        )
        file_results[entity_name] = result

    print(f"Processed {len(file_results)} files")

    # Example 4: Writing to Different File Formats
    print("\n=== Writing to Different File Formats ===")

    # Create subdirectories for different output formats
    formats_dir = os.path.join(output_dir, "formats")
    os.makedirs(formats_dir, exist_ok=True)

    # Use the JSON result to demonstrate different output formats
    result = json_result

    # Write to JSON
    json_output_dir = os.path.join(formats_dir, "json")
    json_files = result.write_all_json(base_path=json_output_dir, indent=2)
    print(f"Wrote {len(json_files)} JSON files to {json_output_dir}")

    # Write to CSV
    csv_output_dir = os.path.join(formats_dir, "csv")
    csv_files = result.write_all_csv(base_path=csv_output_dir)
    print(f"Wrote {len(csv_files)} CSV files to {csv_output_dir}")

    # Try to write to Parquet if available
    try:
        parquet_output_dir = os.path.join(formats_dir, "parquet")
        parquet_files = result.write_all_parquet(base_path=parquet_output_dir)
        print(f"Wrote {len(parquet_files)} Parquet files to {parquet_output_dir}")
    except ImportError:
        print("PyArrow not available, skipping Parquet output")

    # Example 5: Streaming File Processing
    print("\n=== Streaming File Processing ===")

    # Create directory for streaming output
    streaming_dir = os.path.join(output_dir, "streaming")
    os.makedirs(streaming_dir, exist_ok=True)

    # Process JSONL file with streaming
    processor = tm.Processor.memory_optimized()

    print(f"Processing JSONL file with streaming: {jsonl_path}")

    # Use stream_process_file to directly process and write to files
    processor.stream_process_file(
        file_path=jsonl_path,
        entity_name="events",
        file_format="jsonl",  # JSON Lines format
        output_format="json",
        output_destination=streaming_dir,
    )

    # Count the output files
    streaming_files = [f for f in os.listdir(streaming_dir) if f.endswith(".json")]
    print(f"Created {len(streaming_files)} files from streaming process")

    print(f"\nAll output files written to: {output_dir}")


if __name__ == "__main__":
    main()
