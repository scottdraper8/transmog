"""
Example demonstrating custom naming strategies in Transmog.

This example shows how to implement and use custom naming strategies
for table and field names when processing complex JSON structures.
The domain-specific table naming strategy demonstrates how to create
more intuitive table names for business users.
"""

import json
import os
import sys
import re
from typing import Any, Dict, List, Tuple, Optional

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from src package
from transmog import Processor
from transmog.config import extensions
from transmog.config.settings import settings
from transmog.config import configure


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


def camel_case_strategy(components: List[str]) -> str:
    """
    Convert path components to camelCase.

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


def snake_case_strategy(components: List[str]) -> str:
    """
    Convert path components to snake_case.

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


class DomainSpecificTableStrategy:
    """
    A class-based approach to domain-specific table naming.

    This strategy creates more intuitive table names by mapping technical path names
    to business-friendly domain names:
    - Departments become organization_departments
    - SubDepartments become organization_teams
    - Campaigns become organization_marketing_campaigns
    - Channels become organization_marketing_channels

    This approach is suitable for production environments where table naming
    needs to be customized according to business domains.
    """

    def __init__(self):
        """Initialize the strategy."""
        self.original_extract_arrays = None
        self.created_tables = {}

    def get_domain_table_name(
        self, path: str, entity_name: str, separator: str
    ) -> Optional[str]:
        """
        Get a domain-specific table name based on the path.

        Args:
            path: Original path
            entity_name: Entity name
            separator: Separator character

        Returns:
            Domain-specific table name or None to use the default
        """
        path_lower = path.lower()

        # Apply specific domain rules
        if path_lower.endswith("departments"):
            return f"{entity_name}_departments"

        if "subdepartment" in path_lower:
            return f"{entity_name}_teams"

        if "campaign" in path_lower:
            return f"{entity_name}_marketing_campaigns"

        if "channel" in path_lower:
            return f"{entity_name}_marketing_channels"

        # Return None to use the default name
        return None

    def extract_arrays_wrapper(self, *args, **kwargs):
        """
        Wrapper for extract_arrays that applies domain-specific naming.

        This is the function that will replace the original extract_arrays.
        """
        # Call the original function
        result = self.original_extract_arrays(*args, **kwargs)

        # Apply domain-specific naming to the result keys
        entity_name = kwargs.get("entity_name", "root")
        separator = kwargs.get("separator", "_")

        # Only apply for our specific entity type
        if entity_name == "organization":
            new_result = {}

            for path, items in result.items():
                # Try to get a domain-specific name
                domain_name = self.get_domain_table_name(path, entity_name, separator)

                if domain_name:
                    # Use the domain-specific name
                    self.created_tables[path] = domain_name

                    # If we already have items for this domain name, extend them
                    if domain_name in new_result:
                        new_result[domain_name].extend(items)
                    else:
                        new_result[domain_name] = items
                else:
                    # Keep the original key
                    new_result[path] = items

            return new_result

        # For other entity types, return the original result
        return result

    def apply(self):
        """
        Apply this strategy by patching the extract_arrays function.

        Returns:
            The strategy instance for method chaining
        """
        from transmog.core import extractor

        # Store the original function
        self.original_extract_arrays = extractor.extract_arrays

        # Replace with our wrapper
        extractor.extract_arrays = self.extract_arrays_wrapper

        return self

    def restore(self):
        """
        Restore the original extract_arrays function.

        Returns:
            The strategy instance for method chaining
        """
        if self.original_extract_arrays:
            from transmog.core import extractor

            extractor.extract_arrays = self.original_extract_arrays

        return self

    def print_mapping(self):
        """Print the mapping of original to domain-specific table names."""
        if self.created_tables:
            print("\nDomain-specific table mappings:")
            for original, renamed in self.created_tables.items():
                print(f"  {original} -> {renamed}")


def register_custom_strategies() -> None:
    """Register custom naming strategies with Transmog."""
    # Register field naming strategies
    extensions.register_naming_strategy("camelCase", camel_case_strategy)
    extensions.register_naming_strategy("snake_case", snake_case_strategy)

    # Note: The domain table strategy is implemented through the
    # DomainSpecificTableStrategy class which directly patches the
    # extract_arrays function, since the current extension API doesn't
    # fully support custom table naming.


def process_with_camel_case() -> Dict[str, Any]:
    """
    Process data using camelCase field naming.

    Returns:
        Processing result
    """
    # Set the naming strategy - create a monkey-patched function that uses camelCase
    from transmog.naming import conventions

    # Store the original function if we haven't already
    if not hasattr(settings, "_original_field_name"):
        settings._original_field_name = conventions.get_standard_field_name

    # Create a camelCase version
    def camel_case_field_name(path: str, separator: str = ".") -> str:
        components = path.split(separator) if path else []
        return camel_case_strategy(components)

    # Replace the function
    conventions.get_standard_field_name = camel_case_field_name

    # Create processor with camelCase for field names
    processor = Processor(
        cast_to_string=True,
        include_empty=False,
        separator=".",  # Using dot notation for paths
    )

    # Process the data
    result = processor.process(data=COMPLEX_DATA, entity_name="organization")

    # Restore original function
    if hasattr(settings, "_original_field_name"):
        conventions.get_standard_field_name = settings._original_field_name

    return result


def process_with_snake_case() -> Dict[str, Any]:
    """
    Process data using snake_case field naming.

    Returns:
        Processing result
    """
    # Set the naming strategy - create a monkey-patched function that uses snake_case
    from transmog.naming import conventions

    # Store the original function if we haven't already
    if not hasattr(settings, "_original_field_name"):
        settings._original_field_name = conventions.get_standard_field_name

    # Create a snake_case version
    def snake_case_field_name(path: str, separator: str = "_") -> str:
        components = path.split(separator) if path else []
        return snake_case_strategy(components)

    # Replace the function
    conventions.get_standard_field_name = snake_case_field_name

    # Create processor with snake_case for field names
    processor = Processor(
        cast_to_string=True,
        include_empty=False,
        separator="_",  # Using underscore for paths
    )

    # Process the data
    result = processor.process(data=COMPLEX_DATA, entity_name="organization")

    # Restore original function
    if hasattr(settings, "_original_field_name"):
        conventions.get_standard_field_name = settings._original_field_name

    return result


def process_with_domain_tables() -> Dict[str, Any]:
    """
    Process data using domain-specific table naming.

    This approach uses a class-based strategy to create more intuitive,
    business-friendly table names from technical paths:
    - Departments -> organization_departments
    - SubDepartments -> organization_teams
    - Campaigns -> organization_marketing_campaigns
    - Channels -> organization_marketing_channels

    Returns:
        Processing result with domain-specific table names
    """
    # Create and apply the strategy
    strategy = DomainSpecificTableStrategy().apply()

    try:
        # Create processor
        processor = Processor(
            cast_to_string=True,
            include_empty=False,
            separator=".",  # Using dot notation for paths
        )

        # Process the data
        result = processor.process(data=COMPLEX_DATA, entity_name="organization")

        # Print debug info
        strategy.print_mapping()

        return result
    finally:
        # Always restore the original function
        strategy.restore()


def main():
    """Run the example."""
    # Register custom strategies
    register_custom_strategies()

    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)

    # Process with camelCase fields
    print("\n=== Processing with camelCase field names ===")
    camel_result = process_with_camel_case()

    # Write camelCase results
    camel_out_path = os.path.join(output_dir, "camel_case_main.json")
    with open(camel_out_path, "w") as f:
        json.dump(camel_result.get_main_table(), f, indent=2)

    print(f"Main table written to {camel_out_path}")
    print(f"Child tables: {camel_result.get_table_names()}")

    # Process with snake_case fields
    print("\n=== Processing with snake_case field names ===")
    snake_result = process_with_snake_case()

    # Write snake_case results
    snake_out_path = os.path.join(output_dir, "snake_case_main.json")
    with open(snake_out_path, "w") as f:
        json.dump(snake_result.get_main_table(), f, indent=2)

    print(f"Main table written to {snake_out_path}")
    print(f"Child tables: {snake_result.get_table_names()}")

    # Process with domain-specific table names
    print("\n=== Processing with domain-specific table names ===")
    domain_result = process_with_domain_tables()

    # Write domain-specific results
    domain_out_path = os.path.join(output_dir, "domain_tables_main.json")
    with open(domain_out_path, "w") as f:
        json.dump(domain_result.get_main_table(), f, indent=2)

    print(f"Main table written to {domain_out_path}")
    print(f"Child tables: {domain_result.get_table_names()}")

    # Compare the table names from different strategies
    print("\n=== Table Name Comparison ===")
    camel_tables = camel_result.get_table_names()
    domain_tables = domain_result.get_table_names()

    print("Default naming strategy tables:")
    for table in camel_tables:
        print(f"  - {table}")

    print("\nDomain-specific naming strategy tables:")
    for table in domain_tables:
        print(f"  - {table}")


if __name__ == "__main__":
    main()
