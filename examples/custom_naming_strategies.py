"""Example demonstrating custom naming strategies in Transmog.

This example shows how to implement and use custom naming strategies
for table and field names when processing complex JSON structures.
The domain-specific table naming strategy demonstrates how to create
more intuitive table names for business users.
"""

import json
import os
import re
import sys
from typing import Any, Optional

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from transmog package
from transmog import Processor, TransmogConfig
from transmog.naming import register_field_name_strategy, register_table_name_strategy

# Sample complex nested JSON with deeply nested fields
COMPLEX_DATA = {
    "organization": {
        "id": "org123",
        "name": "Example Corporation",
        "metadata": {
            "established": "1995-03-15",
            "headquartersLocation": {
                "address": "123 Corporate Drive",
                "city": "Metropolis",
                "state": "CA",
                "postalCode": "90210",
            },
        },
        "departments": [
            {
                "id": "dept1",
                "name": "Engineering",
                "headCount": 150,
                "subDepartments": [
                    {
                        "id": "subdept1",
                        "name": "Frontend Development",
                        "headCount": 45,
                        "technologies": ["JavaScript", "React", "CSS"],
                    },
                    {
                        "id": "subdept2",
                        "name": "Backend Development",
                        "headCount": 55,
                        "technologies": ["Python", "Java", "Go"],
                    },
                    {
                        "id": "subdept3",
                        "name": "DevOps",
                        "headCount": 25,
                        "technologies": ["Kubernetes", "Docker", "Terraform"],
                    },
                ],
            },
            {
                "id": "dept2",
                "name": "Marketing",
                "headCount": 75,
                "campaigns": [
                    {
                        "id": "campaign1",
                        "name": "Q1 Product Launch",
                        "budget": 250000,
                        "channels": [
                            {"name": "Social Media", "allocation": 0.4},
                            {"name": "Email", "allocation": 0.3},
                            {"name": "Print", "allocation": 0.2},
                            {"name": "TV", "allocation": 0.1},
                        ],
                    }
                ],
            },
        ],
    }
}


def camel_case_strategy(components: list[str]) -> str:
    """Convert path components to camelCase.

    Args:
        components: List of path components

    Returns:
        camelCase string
    """
    if not components:
        return ""

    # First component starts with lowercase
    result = components[0].lower()

    # Subsequent components are capitalized
    for comp in components[1:]:
        result += comp[0].upper() + comp[1:] if comp else ""

    return result


def snake_case_strategy(components: list[str]) -> str:
    """Convert path components to snake_case.

    Args:
        components: List of path components

    Returns:
        snake_case string
    """
    # Convert CamelCase to snake_case first
    processed_components = []

    for comp in components:
        # Insert underscore before uppercase letters and convert to lowercase
        snake = re.sub(r"([A-Z])", r"_\1", comp).lower()
        # Remove leading underscore if present
        snake = snake.lstrip("_")
        processed_components.append(snake)

    return "_".join(processed_components)


def domain_specific_table_strategy(
    components: list[str], entity_name: str, path: str
) -> Optional[str]:
    """Create domain-specific table names based on component path.

    This strategy creates more intuitive table names by mapping technical path names
    to business-friendly domain names:
    - Departments become organization_departments
    - SubDepartments become organization_teams
    - Campaigns become organization_marketing_campaigns
    - Channels become organization_marketing_channels

    Args:
        components: Path components
        entity_name: Entity name
        path: Original path string

    Returns:
        Domain-specific table name or None to use default
    """
    path_lower = path.lower()

    # Apply specific domain rules
    if "departments" in path_lower and "subdepartments" not in path_lower:
        return f"{entity_name}_departments"

    if "subdepartment" in path_lower:
        return f"{entity_name}_teams"

    if "campaign" in path_lower:
        return f"{entity_name}_marketing_campaigns"

    if "channel" in path_lower:
        return f"{entity_name}_marketing_channels"

    # Return None to use the default name
    return None


def print_mapping(table_mapping: dict[str, str]) -> None:
    """Print table path to table name mapping."""
    print("\nTable Path to Name Mapping:")
    print("-" * 80)
    for original_path, table_name in sorted(table_mapping.items()):
        print(f"{original_path:40} -> {table_name}")


def process_with_camel_case() -> dict[str, Any]:
    """Process the data with camel case naming strategy."""
    print("\n=== Processing with camelCase naming strategy ===")

    # Define the camelCase field name function
    def camel_case_field_name(path: str, separator: str = ".") -> str:
        """Convert dot-separated path to camelCase."""
        if not path:
            return ""

        # Split by separator
        components = path.split(separator)
        return camel_case_strategy(components)

    # Create configuration with camelCase field naming
    config = TransmogConfig.default().with_custom_naming(
        field_name_strategy=camel_case_field_name
    )

    # Create processor and process the data
    processor = Processor(config)
    result = processor.process(COMPLEX_DATA, entity_name="organization")

    # Print sample of the field names
    tables = result.to_dict()
    print("\nSample camelCase field names:")
    print("-" * 80)
    for name, values in tables.items():
        if values:
            print(f"Table: {name}")
            print("Fields:", ", ".join(sorted(values[0].keys())[:5]) + "...")
            break

    return tables


def process_with_snake_case() -> dict[str, Any]:
    """Process the data with snake case naming strategy."""
    print("\n=== Processing with snake_case naming strategy ===")

    # Define the snake_case field name function
    def snake_case_field_name(path: str, separator: str = "_") -> str:
        """Convert underscore-separated path to snake_case."""
        if not path:
            return ""

        # Split by separator
        components = path.split(separator)
        return snake_case_strategy(components)

    # Create configuration with snake_case field naming
    config = TransmogConfig.default().with_custom_naming(
        field_name_strategy=snake_case_field_name
    )

    # Create processor and process the data
    processor = Processor(config)
    result = processor.process(COMPLEX_DATA, entity_name="organization")

    # Print sample of the field names
    tables = result.to_dict()
    print("\nSample snake_case field names:")
    print("-" * 80)
    for name, values in tables.items():
        if values:
            print(f"Table: {name}")
            print("Fields:", ", ".join(sorted(values[0].keys())[:5]) + "...")
            break

    return tables


def process_with_domain_tables() -> dict[str, Any]:
    """Process the data with domain-specific table naming."""
    print("\n=== Processing with domain-specific table naming ===")

    # Register the domain-specific table naming strategy
    register_table_name_strategy(domain_specific_table_strategy)

    # Create a processor with default configuration
    processor = Processor()

    # Process the data
    result = processor.process(COMPLEX_DATA, entity_name="organization")

    # Get the tables
    tables = result.to_dict()

    # Print the tables and their row counts
    print("\nDomain-specific tables:")
    print("-" * 80)
    for table_name, rows in sorted(tables.items()):
        print(f"{table_name:40} ({len(rows)} rows)")

    return tables


def process_with_combined_strategies() -> dict[str, Any]:
    """Process the data with combined naming strategies."""
    print("\n=== Processing with combined naming strategies ===")

    # Register both strategies
    register_table_name_strategy(domain_specific_table_strategy)
    register_field_name_strategy(camel_case_strategy)

    # Create a processor with custom configuration
    config = TransmogConfig.default().with_naming(
        # Setting other naming options
        abbreviate_field_names=True,
        max_field_component_length=8,
        preserve_root_component=True,
        preserve_leaf_component=True,
        # Adding custom abbreviations
        custom_abbreviations={
            "organization": "org",
            "headquarters": "hq",
            "location": "loc",
            "department": "dept",
            "marketing": "mkt",
        },
    )

    processor = Processor(config)

    # Process the data
    result = processor.process(COMPLEX_DATA, entity_name="organization")

    # Get the tables
    tables = result.to_dict()

    # Print the tables and a sample of their fields
    print("\nCombined naming strategies result:")
    print("-" * 80)
    for table_name, rows in sorted(tables.items()):
        if rows:
            print(f"Table: {table_name} ({len(rows)} rows)")
            print("Sample fields:")
            for field_name in sorted(rows[0].keys())[:5]:
                print(f"  - {field_name}")
            print("")

    return tables


def main():
    """Run the naming strategies example."""
    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "output", "naming_strategies")
    os.makedirs(output_dir, exist_ok=True)

    print("Custom Naming Strategies Example")
    print("================================")
    print(
        "\nThis example demonstrates different naming strategies for tables and fields."
    )

    # Process with camelCase field names
    camel_case_tables = process_with_camel_case()

    # Process with snake_case field names
    snake_case_tables = process_with_snake_case()

    # Process with domain-specific table names
    domain_tables = process_with_domain_tables()

    # Process with combined strategies
    combined_tables = process_with_combined_strategies()

    # Save results to files for inspection
    # CamelCase
    with open(os.path.join(output_dir, "camel_case_output.json"), "w") as f:
        json.dump(camel_case_tables, f, indent=2)

    # Snake case
    with open(os.path.join(output_dir, "snake_case_output.json"), "w") as f:
        json.dump(snake_case_tables, f, indent=2)

    # Domain-specific
    with open(os.path.join(output_dir, "domain_specific_output.json"), "w") as f:
        json.dump(domain_tables, f, indent=2)

    # Combined strategies
    with open(os.path.join(output_dir, "combined_strategies_output.json"), "w") as f:
        json.dump(combined_tables, f, indent=2)

    print(f"\nAll results written to: {output_dir}")

    print("\nSummary of Naming Strategies:")
    print("-" * 80)
    print("1. Field Name Strategies:")
    print("   - camelCase: Converts 'first_name' to 'firstName'")
    print("   - snake_case: Converts 'firstName' to 'first_name'")

    print("\n2. Table Name Strategies:")
    print("   - Domain-specific: Maps technical paths to business domain names")
    print(
        "     e.g., 'organization_departments_subDepartments' -> 'organization_teams'"
    )

    print("\n3. Combined Strategies:")
    print("   - Leverage both table and field naming together")
    print("   - Add abbreviations and other naming options for complete customization")

    print("\nImplementation Approaches:")
    print("1. Function-based: Simple functions that map components to names")
    print("2. Registration: Register strategies with the transmog naming system")
    print("3. Configuration: Configure naming options through TransmogConfig")


if __name__ == "__main__":
    main()
