"""Partial Recovery Example for Transmog.

This example demonstrates practical real-world use cases for the
PartialProcessingRecovery strategy and shows how it can save valuable data
that would otherwise be lost with stricter error handling approaches.
"""

import logging
import os
import sys
import tempfile

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from src package
from transmog import Processor, TransmogConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("partial_recovery_example")


def print_header(title):
    """Print a section header."""
    print("\n" + "=" * 80)
    print(f" {title} ".center(80, "="))
    print("=" * 80)


# Example 1: Malformed JSON data in an array
def example_malformed_array():
    """Process a dataset with some malformed array elements.

    Real-world scenario: Extract as much valid data as possible from a dataset
    where some array elements are malformed.

    This is common when:
    - Working with user-generated content
    - Processing data from external APIs
    - Handling legacy data migrations
    """
    print_header("Example 1: Recovering from Malformed Array Elements")

    # Data with a mix of valid and problematic elements
    data = {
        "company": "Acme Inc.",
        "employees": [
            {"id": 1, "name": "Alice", "department": "Engineering", "salary": 120000},
            {
                "id": 2,
                "name": "Bob",
                "department": "Sales",
                "salary": float("inf"),
            },  # Invalid salary
            {"id": 3, "name": "Charlie", "department": "Marketing", "salary": 90000},
            {
                "id": 4,
                "name": None,
                "department": "Product",
                "salary": 110000,
            },  # Missing name
            {"id": 5, "name": "Eve", "department": "Engineering", "salary": 125000},
        ],
    }

    # Process with different recovery strategies
    comparison_results = {}

    # 1. Strict recovery (will fail)
    try:
        strict_processor = Processor(
            config=TransmogConfig.default().with_error_handling(
                recovery_strategy="strict"
            )
        )
        strict_result = strict_processor.process(data, entity_name="company")
        comparison_results["strict"] = {
            "success": True,
            "employees": len(strict_result.get_child_table("company_employees")),
            "result": strict_result.to_dict(),
        }
    except Exception as e:
        comparison_results["strict"] = {"success": False, "error": str(e)}
        print(f"Strict recovery failed: {str(e)}")

    # 2. Skip and log (will skip problematic records)
    skip_processor = Processor(
        config=TransmogConfig.default().with_error_handling(recovery_strategy="skip")
    )
    skip_result = skip_processor.process(data, entity_name="company")
    comparison_results["skip"] = {
        "success": True,
        "employees": len(skip_result.get_child_table("company_employees")),
        "result": skip_result.to_dict(),
    }
    print(
        f"Skip recovery processed: {
            len(skip_result.get_child_table('company_employees'))
        } employees"
    )

    # 3. Partial recovery (will preserve most data)
    partial_processor = Processor(
        config=TransmogConfig.default().with_error_handling(recovery_strategy="partial")
    )
    partial_result = partial_processor.process(data, entity_name="company")
    comparison_results["partial"] = {
        "success": True,
        "employees": len(partial_result.get_child_table("company_employees")),
        "result": partial_result.to_dict(),
    }
    print(
        f"Partial recovery processed: {
            len(partial_result.get_child_table('company_employees'))
        } employees"
    )

    # Compare recoverable data
    print("\nComparison of employee record counts:")
    strict_result = (
        "Failed"
        if not comparison_results["strict"]["success"]
        else comparison_results["strict"]["employees"]
    )
    print(f"  Strict recovery: {strict_result}")
    print(f"  Skip recovery: {comparison_results['skip']['employees']}")
    print(f"  Partial recovery: {comparison_results['partial']['employees']}")

    # Show the recovered data
    print("\nPartially recovered employee records:")
    for employee in partial_result.get_child_table("company_employees"):
        error_marker = " [has errors]" if "_error" in employee else ""
        name = employee.get("name", "[missing]")
        print(f"  Employee {employee['id']}: {name}{error_marker}")
        if "_error" in employee:
            print(f"    Error: {employee['_error']}")


# Example 2: API response with deeply nested structures
def example_deeply_nested_structures():
    """Handle API responses with deeply nested structures.

    Real-world scenario: Processing API responses or exported data that may
    contain excessively deep nested structures.

    This is common when:
    - Working with complex hierarchical data structures
    - Processing verbose API responses
    - Handling nested JSON configurations
    """
    print_header("Example 2: Handling Deeply Nested Structures")

    # Create data with excessive nesting
    # This simulates an API response with an excessive hierarchy
    def create_nested_object(level, max_level=100):
        """Create a deeply nested object."""
        if level >= max_level:
            return {"value": f"level-{level}"}

        return {
            "id": f"level-{level}",
            "name": f"Level {level}",
            "metadata": {
                "created_at": "2023-01-01",
                "version": level,
                "child": create_nested_object(level + 1, max_level),
            },
        }

    # Create a deeply nested department structure
    department = {
        "id": "dept-eng",
        "name": "Engineering",
        "manager": {
            "id": "emp-1",
            "name": "Alice",
            "title": "Engineering Director",
            "profile": create_nested_object(1, 50),  # This creates excessive nesting
        },
        "employees": [
            {
                "id": "emp-1",
                "name": "Alice",
                "title": "Engineering Director",
                "skills": [
                    {"name": "Leadership", "level": 9},
                    {"name": "Architecture", "level": 8},
                    {
                        "name": "Programming",
                        "level": 7,
                        "details": create_nested_object(1, 30),
                    },
                ],
            },
            {
                "id": "emp-2",
                "name": "Bob",
                "title": "Senior Engineer",
                "skills": [
                    {"name": "Programming", "level": 9},
                    {"name": "Testing", "level": 8},
                    {"name": "DevOps", "level": 7},
                ],
            },
        ],
    }

    # Prepare data for processing
    organization = {"name": "Acme Inc.", "departments": [department]}

    # Process with different recovery strategies
    comparison_results = {}

    # 1. Try strict recovery (will fail due to max recursion depth)
    try:
        strict_processor = Processor(
            config=TransmogConfig.default().with_error_handling(
                recovery_strategy="strict"
            )
        )
        strict_result = strict_processor.process(
            organization, entity_name="organization"
        )
        comparison_results["strict"] = {
            "success": True,
            "result": strict_result.to_dict(),
        }
    except Exception as e:
        comparison_results["strict"] = {"success": False, "error": str(e)}
        print(f"Strict recovery failed: {str(e)}")

    # 2. Partial recovery
    partial_processor = Processor(
        config=TransmogConfig.default().with_error_handling(recovery_strategy="partial")
    )
    partial_result = partial_processor.process(organization, entity_name="organization")

    # Show the tables created
    print("\nPartial recovery created these tables:")
    for table_name in partial_result.get_table_names():
        table = partial_result.get_child_table(table_name)
        print(f"  {table_name}: {len(table)} records")

    # Show how deep nesting was handled
    print("\nDeep nesting issues were handled by marking problematic parts:")
    marked_records_found = False
    for table_name in partial_result.get_table_names():
        table = partial_result.get_child_table(table_name)
        for record in table:
            # Look for error indicators in any field
            error_fields = [field for field in record if field.startswith("_error")]
            if error_fields:
                marked_records_found = True
                print(f"  Table '{table_name}', record {record.get('id', 'unknown')}:")
                for field in error_fields:
                    print(f"    Error in {field}: {record[field]}")

    if not marked_records_found:
        print("  No error markers found in processed records")


# Example 3: Partial JSON file recovery
def example_malformed_json_file():
    """Recover data from malformed JSON files.

    Real-world scenario: Processing a JSON file that has syntax errors or
    malformed content but still contains valuable data.

    This is common when:
    - Dealing with corrupted data files
    - Processing manually edited JSON
    - Working with truncated or incomplete data dumps
    """
    print_header("Example 3: Recovering Data from Malformed JSON Files")

    # Create a temporary file with valid and invalid JSON
    with tempfile.NamedTemporaryFile(
        mode="w+", suffix=".json", delete=False
    ) as temp_file:
        # Start with valid structure, but introduce errors
        temp_file.write("""
        {
            "records": [
                {"id": 1, "name": "Valid Record 1", "value": 100},
                {"id": 2, "name": "Valid Record 2", "value": 200},
                {"id": 3, "name": "Incomplete Record",
                {"id": 4, "name": "Valid Record 4", "value": 400}
            ]
        """)  # Missing closing bracket and deliberately malformed JSON
        temp_file.flush()
        file_path = temp_file.name

    # Process with different recovery strategies
    for strategy_name, strategy in [
        ("strict", "strict"),
        ("skip", "skip"),
        ("partial", "partial"),
    ]:
        try:
            # Configure processor with the current strategy
            processor = Processor(
                config=TransmogConfig.default().with_error_handling(
                    recovery_strategy=strategy
                )
            )

            # Try to process the file
            result = processor.process_file(file_path, entity_name="data")

            # Report success
            print(
                f"\n{strategy_name.capitalize()} recovery successfully processed "
                f"the file"
            )
            print(f"  Main table has {len(result.get_main_table())} records")
            for key in result.get_table_names():
                if key != "main":
                    print(
                        f"  Child table '{key}' has "
                        f"{len(result.get_child_table(key))} records"
                    )

        except Exception as e:
            print(f"\n{strategy_name.capitalize()} recovery failed: {e}")

    # Clean up temporary file if it exists
    try:
        os.unlink(file_path)
    except OSError as e:
        print(f"Warning: Could not remove temporary file {file_path}: {e}")


# Example 4: Data migration with schema inconsistencies
def example_schema_inconsistencies():
    """Migrate data with schema inconsistencies.

    Real-world scenario: Migrating data from a source where the schema has
    evolved over time, resulting in inconsistent field names and structures.

    This is common when:
    - Migrating legacy databases
    - Processing data spanning multiple versions of an application
    - Consolidating data from different sources
    """
    print_header("Example 4: Migrating Data with Schema Inconsistencies")

    # Create a dataset with inconsistent schemas
    legacy_records = [
        # Old schema
        {"customer_id": 1, "name": "Acme Inc.", "contact": "555-1234", "active": True},
        {"customer_id": 2, "name": "XYZ Corp", "contact": "555-5678", "active": False},
        # Transitional schema - contact split into phone/email
        {
            "customer_id": 3,
            "name": "ABC Ltd",
            "phone": "555-9012",
            "email": "contact@abc.com",
            "active": True,
        },
        # New schema with additional nested information, missing legacy fields
        {
            "id": 4,
            "name": "New Company",
            "contact_info": {"phone": "555-3456", "email": "info@new.com"},
            "status": "active",
        },
        # Malformed record
        {
            "customer_id": 5,
            "name": None,
            "contact": {"value": float("nan")},
            "active": "invalid",
        },
    ]

    # Process with different strategies
    for strategy_name, strategy in [
        ("strict", "strict"),
        ("skip", "skip"),
        ("partial", "partial"),
    ]:
        try:
            # Configure processor
            processor = Processor(
                config=TransmogConfig.default().with_error_handling(
                    recovery_strategy=strategy
                )
            )

            # Process the data
            result = processor.process_batch(legacy_records, entity_name="customers")

            # Report results
            print(
                f"\n{strategy_name.capitalize()} recovery processed "
                f"{len(result.get_main_table())} customer records"
            )

            # Show the combined schema
            if strategy_name == "partial":
                print("\nFields present in the partial recovery results:")
                # Get all unique field names
                all_fields = set()
                for record in result.get_main_table():
                    all_fields.update(record.keys())

                print(
                    "  "
                    + ", ".join(
                        sorted([f for f in all_fields if not f.startswith("__")])
                    )
                )

                # Show which records were recovered with errors
                print("\nRecovered records:")
                for _i, record in enumerate(result.get_main_table()):
                    id_value = record.get("customer_id") or record.get("id")
                    has_error = "_error" in record
                    print(
                        f"  Record {id_value}: "
                        f"{'Partial (has errors)' if has_error else 'Complete'}"
                    )

        except Exception as e:
            print(f"\n{strategy_name.capitalize()} recovery failed: {e}")


def main():
    """Run all examples."""
    print_header("Transmog Partial Recovery Examples")

    print("""
This example demonstrates how the PartialProcessingRecovery strategy can help:
1. Recover usable data from malformed array elements
2. Process corrupted or malformed JSON files
3. Handle schema inconsistencies during data migrations

Each example compares the results between strict, skip, and partial recovery strategies.
    """)

    # Run all examples
    example_malformed_array()
    example_deeply_nested_structures()
    example_malformed_json_file()
    example_schema_inconsistencies()

    print("""
Key takeaways:
- Partial recovery can significantly increase data yield when processing problematic
  datasets
- Strict recovery fails completely on encountering any errors
- Skip recovery loses entire records even when only parts are problematic
- Partial recovery preserves data structure and relationships even with problematic
  elements
- Error markers make it easy to identify and handle partially recovered data
    """)


if __name__ == "__main__":
    main()
