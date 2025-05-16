"""Advanced example demonstrating Transmog functionality.

This example shows more complex use cases including:
- Custom table naming
- Complex nested structures
- Array handling
- Custom ID generation
- Performance optimization
"""

import os
import sys

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from src package
from transmog import Processor


def main():
    """Run the example."""
    # Complex nested JSON data
    data = {
        "company_id": "COMP123",
        "name": "TechCorp Inc.",
        "founding_date": "2000-01-01",
        "financials": {
            "revenue": 1000000,
            "expenses": {
                "salaries": 500000,
                "office": 200000,
                "marketing": 100000,
            },
            "profit": 200000,
        },
        "departments": [
            {
                "dept_id": "DEPT001",
                "name": "Engineering",
                "manager": {
                    "emp_id": "EMP001",
                    "name": "Alice Smith",
                    "title": "CTO",
                },
                "projects": [
                    {
                        "project_id": "PROJ001",
                        "name": "Alpha",
                        "status": "active",
                        "team": [
                            {"emp_id": "EMP002", "name": "Bob Jones", "role": "Lead"},
                            {"emp_id": "EMP003", "name": "Carol White", "role": "Dev"},
                        ],
                    },
                    {
                        "project_id": "PROJ002",
                        "name": "Beta",
                        "status": "completed",
                        "team": [
                            {"emp_id": "EMP004", "name": "Dave Brown", "role": "Lead"},
                            {"emp_id": "EMP005", "name": "Eve Green", "role": "Dev"},
                        ],
                    },
                ],
            },
            {
                "dept_id": "DEPT002",
                "name": "Marketing",
                "manager": {
                    "emp_id": "EMP006",
                    "name": "Frank Wilson",
                    "title": "CMO",
                },
                "campaigns": [
                    {
                        "campaign_id": "CAMP001",
                        "name": "Summer Sale",
                        "budget": 50000,
                        "metrics": {
                            "impressions": 1000000,
                            "clicks": 50000,
                            "conversions": 5000,
                        },
                    },
                    {
                        "campaign_id": "CAMP002",
                        "name": "Holiday Special",
                        "budget": 75000,
                        "metrics": {
                            "impressions": 1500000,
                            "clicks": 75000,
                            "conversions": 7500,
                        },
                    },
                ],
            },
        ],
    }

    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)

    # Example 1: Custom table naming and processing
    print("\n=== Example 1: Custom Configuration ===")
    processor = (
        Processor()
        .with_naming(
            separator="_",
            abbreviate_table_names=True,
            custom_names={
                "departments": "dept",
                "departments_projects": "proj",
                "departments_projects_team": "team",
                "departments_campaigns": "camp",
            },
        )
        .with_processing(cast_to_string=False, skip_null=True, max_array_size=1000)
        .with_metadata(
            id_field="record_id",
            parent_field="parent_id",
            timestamp_field="processed_at",
        )
    )
    result = processor.process(data=data, entity_name="company")
    print("Processed with custom configuration")

    # Example 2: Deterministic IDs with custom fields
    print("\n=== Example 2: Deterministic IDs ===")
    processor = Processor.with_deterministic_ids(
        {
            "": "company_id",
            "departments": "dept_id",
            "departments_projects": "project_id",
            "departments_projects_team": "emp_id",
            "departments_campaigns": "campaign_id",
        }
    )
    result = processor.process(data=data, entity_name="company")
    print("Processed with deterministic IDs")

    # Example 3: Memory-optimized processing with custom configuration
    print("\n=== Example 3: Memory-Optimized Processing ===")
    processor = (
        Processor.memory_optimized()
        .with_naming(separator=".", abbreviate_table_names=False)
        .with_processing(cast_to_string=True, skip_null=False)
    )
    result = processor.process(data=data, entity_name="company")
    print("Processed with memory optimization")

    # Example 4: Performance-optimized processing with custom configuration
    print("\n=== Example 4: Performance-Optimized Processing ===")
    processor = (
        Processor.performance_optimized()
        .with_naming(separator="_", abbreviate_table_names=True)
        .with_processing(cast_to_string=False, skip_null=True)
    )
    result = processor.process(data=data, entity_name="company")
    print("Processed with performance optimization")

    # Write to Parquet with custom compression
    print("\n=== Writing to Parquet ===")
    outputs = result.write_all_parquet(
        base_path=output_dir, compression="zstd", partition_cols=["processed_at"]
    )

    for table_name, file_path in outputs.items():
        print(f"Wrote {table_name} to {file_path}")


if __name__ == "__main__":
    main()
