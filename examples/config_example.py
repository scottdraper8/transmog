"""
Example demonstrating Transmogrify configuration functionality.

This example shows how to use profiles, configuration files, and
direct setting configuration with Transmogrify.
"""

import json
import os
import sys
from pprint import pprint

# Add parent directory to path to import transmogrify without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from src package
from src.transmogrify import Processor, ProcessingResult
from src.transmogrify.config import settings, load_profile, configure, extensions


def create_sample_config_file():
    """Create a sample configuration file."""
    config = {
        "separator": ".",
        "cast_to_string": True,
        "include_empty": True,
        "batch_size": 500,
        "log_level": 20,  # INFO level
    }

    # Create directory if it doesn't exist
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)

    # Write config to file
    config_path = os.path.join(output_dir, "transmogrify_config.json")
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)

    return config_path


def register_custom_extensions():
    """Register custom extensions for demonstration."""

    # Register a custom type handler for datetime values
    def handle_datetime(value):
        """Convert datetime values to ISO format strings."""
        import datetime

        if isinstance(value, datetime.datetime):
            return value.isoformat()
        return value

    extensions.register_type_handler("datetime", handle_datetime)

    # Register a custom naming strategy
    def camel_case_strategy(components):
        """Convert components to camelCase."""
        if not components:
            return ""
        result = components[0].lower()
        for comp in components[1:]:
            result += comp.capitalize()
        return result

    extensions.register_naming_strategy("camelCase", camel_case_strategy)


def main():
    """Run the example."""
    # Sample nested JSON data
    data = {
        "id": 123,
        "name": "Example Company",
        "address": {
            "street": "123 Main St",
            "city": "Anytown",
            "state": "CA",
            "zip": "12345",
        },
        "contacts": [
            {
                "type": "primary",
                "name": "John Doe",
                "phone": "555-1234",
                "details": {"department": "Sales", "position": "Manager"},
            },
            {
                "type": "secondary",
                "name": "Jane Smith",
                "phone": "555-5678",
                "details": {"department": "Support", "position": "Director"},
            },
        ],
    }

    # Example 1: Use default settings
    print("\n=== Example 1: Default Settings ===")
    processor = Processor()
    result = processor.process(data=data, entity_name="company")
    print(f"Main table record count: {len(result.get_main_table())}")
    print(f"Child tables: {result.get_table_names()}")
    print(
        f"Settings used: separator='{settings.separator}', batch_size={settings.batch_size}"
    )

    # Example 2: Use a predefined profile
    print("\n=== Example 2: Memory-Efficient Profile ===")
    load_profile("memory_efficient")
    processor = Processor()  # Will use memory_efficient profile settings
    result = processor.process(data=data, entity_name="company")
    print(f"Main table record count: {len(result.get_main_table())}")
    print(
        f"Settings used: optimize_for_memory={settings.optimize_for_memory}, batch_size={settings.batch_size}"
    )

    # Example 3: Load from config file
    print("\n=== Example 3: Config File ===")
    config_file = create_sample_config_file()
    from src.transmogrify.config import load_config

    load_config(config_file)
    processor = Processor()  # Will use settings from config file
    result = processor.process(data=data, entity_name="company")
    print(f"Main table record count: {len(result.get_main_table())}")
    print(
        f"Settings used: separator='{settings.separator}', include_empty={settings.include_empty}"
    )

    # Example 4: Direct configuration
    print("\n=== Example 4: Direct Configuration ===")
    configure(
        separator="/",
        cast_to_string=False,
        log_level=10,  # DEBUG level
    )
    processor = Processor()  # Will use directly configured settings
    result = processor.process(data=data, entity_name="company")
    print(f"Main table record count: {len(result.get_main_table())}")
    print(
        f"Settings used: separator='{settings.separator}', cast_to_string={settings.cast_to_string}"
    )

    # Example 5: Environment variables (demonstrating how they would be used)
    print("\n=== Example 5: Environment Variables (demonstration) ===")
    print(
        "To use environment variables, you would set them before running your script:"
    )
    print("export TRANSMOGRIFY_SEPARATOR='::'")
    print("export TRANSMOGRIFY_BATCH_SIZE=250")
    print("export TRANSMOGRIFY_OPTIMIZE_FOR_MEMORY=true")

    # Example 6: Custom extensions
    print("\n=== Example 6: Custom Extensions ===")
    register_custom_extensions()
    print(
        f"Registered type handlers: {list(extensions.get_all_type_handlers().keys())}"
    )
    print(
        f"Registered naming strategies: {list(extensions.get_all_naming_strategies().keys())}"
    )


if __name__ == "__main__":
    main()
