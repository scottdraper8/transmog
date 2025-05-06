"""
Transmog Unified Debugger - A comprehensive debugging tool for Transmog components.

This tool provides debugging capabilities for core Transmog components:
1. Flatten functionality - Test and verify the flattener
2. Processor functionality - Test the full processing pipeline
3. Output validation - Compare results with expected outputs
4. Advanced feature testing - Test specific behaviors and edge cases

Usage:
    python tests/transmog_debugger.py [component] [options]

    Components:
        flatten - Test the flattener
        process - Test the processor
        nulls - Test null and empty value handling
        depth - Test max depth behavior
        arrays - Test array handling behavior
        inplace - Test in-place modifications

    Options:
        --verbose - Show detailed debug output
        --data-type TYPE - Specify the test data type (simple, array, scalar, complex, circular)
"""

import sys
import os
import json
from copy import deepcopy
import argparse
from typing import Any, Dict, List, Optional, Set, Tuple, Callable

# Import components directly from the package
from transmog.core.flattener import flatten_json
from transmog.config.settings import settings
from transmog import Processor, TransmogConfig


# Define utility functions
def print_header(title):
    """Print a formatted header."""
    print("\n" + "=" * 80)
    print(f" {title} ".center(80, "="))
    print("=" * 80)


def print_subheader(title):
    """Print a formatted subheader."""
    print("\n" + "-" * 80)
    print(f" {title} ".center(80, "-"))
    print("-" * 80)


def print_dict(data, indent=2):
    """Pretty print a dictionary."""

    # Handle circular references
    class CircularRefEncoder(json.JSONEncoder):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.visited = []

        def default(self, obj):
            obj_id = id(obj)
            if any(obj_id == id(visited_obj) for visited_obj in self.visited):
                return "<circular reference>"

            if isinstance(obj, dict) or isinstance(obj, list):
                self.visited.append(obj)
                result = super().default(obj)
                self.visited.pop()
                return result

            return str(obj)

    try:
        print(json.dumps(data, indent=indent, cls=CircularRefEncoder))
    except Exception as e:
        print(f"Could not serialize data: {str(e)}")
        print(f"Data type: {type(data)}")
        if isinstance(data, dict):
            print(f"Keys: {list(data.keys())}")
        elif isinstance(data, list):
            print(f"Length: {len(data)}")
        print("Using string representation instead:")
        print(str(data))


def compare_dicts(dict1, dict2):
    """Compare two dictionaries and return the differences."""
    result = {
        "keys_only_in_first": [],
        "keys_only_in_second": [],
        "different_values": {},
    }

    # Find keys in first but not in second
    for key in dict1:
        if key not in dict2:
            result["keys_only_in_first"].append(key)

    # Find keys in second but not in first
    for key in dict2:
        if key not in dict1:
            result["keys_only_in_second"].append(key)

    # Find different values for common keys
    for key in dict1:
        if key in dict2 and dict1[key] != dict2[key]:
            result["different_values"][key] = (dict1[key], dict2[key])

    return result


def print_comparison(comparison, title="Comparison Results"):
    """Print a dictionary comparison result."""
    print_subheader(title)

    if (
        not comparison["keys_only_in_first"]
        and not comparison["keys_only_in_second"]
        and not comparison["different_values"]
    ):
        print("The dictionaries are identical!")
        return

    if comparison["keys_only_in_first"]:
        print(
            f"Keys only in first dictionary: {sorted(comparison['keys_only_in_first'])}"
        )

    if comparison["keys_only_in_second"]:
        print(
            f"Keys only in second dictionary: {sorted(comparison['keys_only_in_second'])}"
        )

    if comparison["different_values"]:
        print(f"Keys with different values: {len(comparison['different_values'])}")
        for k, (v1, v2) in comparison["different_values"].items():
            print(f"  {k}:")
            print(f"    First:  {v1}")
            print(f"    Second: {v2}")


# Sample test data
def get_test_data(data_type="simple"):
    """Get test data of different types."""
    data = {
        "simple": {
            "id": 1,
            "name": "Test",
            "address": {
                "street": "123 Main St",
                "city": "Anytown",
                "state": "CA",
            },
        },
        "array": {
            "id": 3,
            "items": [
                {"id": 1, "value": "one"},
                {"id": 2, "value": "two"},
            ],
        },
        "scalar": {"id": 4, "name": "Test scalar", "price": 99.99, "is_active": True},
        "complex": {
            "id": 5,
            "name": "Complex",
            "metadata": {
                "created": "2023-01-01",
                "modified": "2023-02-01",
                "tags": ["important", "featured"],
            },
            "items": [
                {
                    "id": "item1",
                    "name": "Item 1",
                    "properties": {"color": "red", "size": "large"},
                },
                {
                    "id": "item2",
                    "name": "Item 2",
                    "properties": {"color": "blue", "size": "medium"},
                },
            ],
        },
        "circular": {
            "id": 6,
            "name": "Circular",
            # Self-reference will be added programmatically
        },
        "deep": {
            "level1": {
                "level2": {
                    "level3": {"level4": {"level5": {"data": "Deep nested value"}}}
                }
            }
        },
        "null_values": {
            "id": 7,
            "name": "Test with nulls",
            "description": None,
            "tags": ["tag1", None, "tag3"],
            "metadata": {
                "created": "2023-01-01",
                "updated": None,
                "empty_string": "",
            },
        },
    }

    # Add circular reference
    if data_type == "circular":
        data["circular"]["self"] = data["circular"]

    return data.get(data_type, data["simple"])


def debug_flattener(args):
    """Debug the flattener component."""
    print_header("Flattener Debugger")

    # Get appropriate test data
    data_type = args.data_type if hasattr(args, "data_type") else "simple"
    test_data = get_test_data(data_type)
    print_subheader(f"Input Data: {data_type}")
    print_dict(test_data)

    # Default configuration test
    print_subheader("Default Configuration")
    try:
        verbose = args.verbose if hasattr(args, "verbose") else False
        if verbose:
            print("[DEBUG] Using default configuration")

        default_result = flatten_json(test_data)
        print_dict(default_result)
    except Exception as e:
        print(f"Error with default configuration: {e}")

    # Test with explicit parameters
    print_subheader("Custom Configuration")
    try:
        if verbose:
            print("[DEBUG] Using custom configuration:")
            print("[DEBUG] - cast_to_string=False")
            print("[DEBUG] - include_empty=True")
            print("[DEBUG] - skip_null=False")

        custom_result = flatten_json(
            test_data,
            separator="_",
            cast_to_string=False,
            include_empty=True,
            skip_null=False,
        )
        print_dict(custom_result)
    except Exception as e:
        print(f"Error with custom configuration: {e}")

    # Test abbreviation system with better data for clear demonstration
    print_subheader("Abbreviation System")
    abbrev_data = {"rootcomp": {"middlesection": {"leafvalue": "test"}}}

    max_len = 4  # Max component length for clear truncation

    try:
        # Default behavior (preserve root and leaf)
        default_abbrev = flatten_json(
            abbrev_data,
            abbreviate_field_names=True,
            max_field_component_length=max_len,
        )
        print("Default abbreviation (preserve root and leaf):")
        print_dict(default_abbrev)

        # Preserve only root
        root_only_abbrev = flatten_json(
            abbrev_data,
            abbreviate_field_names=True,
            max_field_component_length=max_len,
            preserve_root_component=True,
            preserve_leaf_component=False,
        )
        print("\nPreserve root only:")
        print_dict(root_only_abbrev)

        # Preserve only leaf
        leaf_only_abbrev = flatten_json(
            abbrev_data,
            abbreviate_field_names=True,
            max_field_component_length=max_len,
            preserve_root_component=False,
            preserve_leaf_component=True,
        )
        print("\nPreserve leaf only:")
        print_dict(leaf_only_abbrev)

        # No preservation
        no_preserve_abbrev = flatten_json(
            abbrev_data,
            abbreviate_field_names=True,
            max_field_component_length=max_len,
            preserve_root_component=False,
            preserve_leaf_component=False,
        )
        print("\nNo preservation (abbreviate all):")
        print_dict(no_preserve_abbrev)

        # Custom abbreviations
        custom_abbrevs = {
            "middlesection": "mid",
            "leafvalue": "leaf",
        }
        custom_abbrev_result = flatten_json(
            abbrev_data,
            abbreviate_field_names=True,
            custom_abbreviations=custom_abbrevs,
        )
        print("\nCustom abbreviations:")
        print_dict(custom_abbrev_result)

    except Exception as e:
        print(f"Error with abbreviation testing: {e}")


def debug_processor(args):
    """Debug the processor component."""
    print_header("Processor Debugger")

    # Get appropriate test data
    data_type = args.data_type if hasattr(args, "data_type") else "simple"
    test_data = get_test_data(data_type)
    print_subheader(f"Input Data: {data_type}")
    print_dict(test_data)

    # Default processor
    print_subheader("Default Processor")
    try:
        processor = Processor()
        result = processor.process(test_data, entity_name="test")

        # Print main table
        print("\nMain Table:")
        print_dict(result.get_main_table())

        # Print table names
        table_names = result.get_table_names()
        print(f"\nTable Names: {table_names}")

        # Print child tables if any
        for table_name in table_names:
            if table_name != "main":
                print(f"\nChild Table: {table_name}")
                child_table = result.get_child_table(table_name)
                if child_table and len(child_table) > 0:
                    print_dict(child_table[0])
                    if len(child_table) > 1:
                        print(f"...and {len(child_table) - 1} more records")
    except Exception as e:
        print(f"Error with default processor: {e}")

    # Custom processor with different configurations
    print_subheader("Custom Processor Configurations")
    try:
        # Test different configuration options in a single session
        configs = [
            ("Default Configuration", TransmogConfig.default()),
            ("Memory Optimized Configuration", TransmogConfig.memory_optimized()),
            (
                "Performance Optimized Configuration",
                TransmogConfig.performance_optimized(),
            ),
            (
                "Custom Naming Configuration",
                TransmogConfig.default().with_naming(
                    separator=".",
                    abbreviate_table_names=True,
                    max_table_component_length=3,
                ),
            ),
        ]

        for config_name, config in configs:
            print(f"\n{config_name}:")
            processor = Processor(config=config)
            result = processor.process(test_data, entity_name="test")
            table_names = result.get_table_names()
            print(f"Table Names: {table_names}")
    except Exception as e:
        print(f"Error with custom processor configurations: {e}")


def debug_null_handling(args):
    """Debug null and empty value handling."""
    print_header("Null and Empty Value Handling")

    # Use test data with null and empty values
    test_data = get_test_data("null_values")
    print_subheader("Input Data with Null and Empty Values")
    print_dict(test_data)

    # Test different configurations for null and empty handling
    configs = [
        (
            "Default Configuration (skip_null=True, include_empty=False)",
            {"skip_null": True, "include_empty": False},
        ),
        (
            "Include Nulls (skip_null=False)",
            {"skip_null": False, "include_empty": False},
        ),
        (
            "Include Empty Strings (include_empty=True)",
            {"skip_null": True, "include_empty": True},
        ),
        (
            "Include Both Nulls and Empty Strings",
            {"skip_null": False, "include_empty": True},
        ),
    ]

    for config_name, params in configs:
        print_subheader(config_name)
        try:
            result = flatten_json(test_data, **params)
            print_dict(result)

            # Count null and empty values
            null_values = sum(1 for v in result.values() if v is None)
            empty_strings = sum(1 for v in result.values() if v == "")

            print(f"\nStatistics:")
            print(f"Total fields: {len(result)}")
            print(f"Null values: {null_values}")
            print(f"Empty strings: {empty_strings}")
        except Exception as e:
            print(f"Error: {e}")


def debug_max_depth(args):
    """Debug max depth behavior."""
    print_header("Max Depth Behavior")

    # Use deeply nested test data
    test_data = get_test_data("deep")
    print_subheader("Deep Nested Input Data")
    print_dict(test_data)

    # Test different max_depth settings
    for depth in [1, 2, 3, None]:
        print_subheader(f"Max Depth: {depth if depth is not None else 'Unlimited'}")
        try:
            result = flatten_json(test_data, max_depth=depth)
            print_dict(result)

            # Count the number of levels in the keys
            max_levels = (
                max(key.count("_") + 1 for key in result.keys()) if result else 0
            )
            print(f"\nDeepest key has {max_levels} levels")
        except Exception as e:
            print(f"Error: {e}")


def debug_array_handling(args):
    """Debug array handling behavior."""
    print_header("Array Handling Behavior")

    # Use test data with arrays
    data_type = "complex"  # Use complex data with nested arrays
    test_data = get_test_data(data_type)
    print_subheader(f"Input Data with Arrays: {data_type}")
    print_dict(test_data)

    # Test different array handling configurations
    configs = [
        (
            "Default (skip_arrays=True, visit_arrays=False)",
            {"skip_arrays": True, "visit_arrays": False},
        ),
        (
            "Process Arrays (skip_arrays=False)",
            {"skip_arrays": False, "visit_arrays": False},
        ),
        (
            "Visit Arrays (visit_arrays=True)",
            {"skip_arrays": True, "visit_arrays": True},
        ),
        ("Process and Visit Arrays", {"skip_arrays": False, "visit_arrays": True}),
    ]

    for config_name, params in configs:
        print_subheader(config_name)
        try:
            result = flatten_json(test_data, **params)
            print_dict(result)

            # Analyze array-related keys
            array_keys = [k for k in result.keys() if isinstance(result[k], list)]
            print(f"\nArray fields: {len(array_keys)}")
            if array_keys:
                print(f"Array keys: {array_keys}")
        except Exception as e:
            print(f"Error: {e}")

    # Demonstrate array extraction using Processor
    print_subheader("Processor Array Extraction")
    try:
        # Default processor
        processor = Processor()
        result = processor.process(test_data, entity_name="test")

        # Print table names and row counts
        table_names = result.get_table_names()
        print(f"Extracted tables: {table_names}")

        for table_name in table_names:
            if table_name == "main":
                continue
            child_table = result.get_child_table(table_name)
            print(f"\nTable '{table_name}': {len(child_table)} rows")
            if child_table:
                print_dict(child_table[0])
    except Exception as e:
        print(f"Error processing arrays: {e}")


def debug_inplace_modification(args):
    """Debug in-place dictionary modification."""
    print_header("In-Place Dictionary Modification")

    # Use test data
    data_type = args.data_type if hasattr(args, "data_type") else "complex"
    test_data = get_test_data(data_type)

    print_subheader("Original Input Data")
    print_dict(test_data)

    # Make a copy of the data for in-place modification
    data_copy = deepcopy(test_data)

    print_subheader("Standard Flattening (Non-In-Place)")
    try:
        # First test standard non-in-place flattening
        standard_result = flatten_json(test_data)
        print_dict(standard_result)

        # Original data should not be modified
        print("\nOriginal data after standard flattening (should be unchanged):")
        print_dict(test_data)
    except Exception as e:
        print(f"Error with standard flattening: {e}")

    print_subheader("In-Place Flattening")
    try:
        # Test in-place flattening
        inplace_result = flatten_json(data_copy, in_place=True)
        print("Flattened result:")
        print_dict(inplace_result)

        # Data should be modified in-place
        print("\nOriginal data after in-place flattening (should be modified):")
        print_dict(data_copy)

        # Compare the in-place modified data with the flattened result
        diff = compare_dicts(inplace_result, data_copy)
        print_comparison(diff, "Comparing in-place result with modified input")
    except Exception as e:
        print(f"Error with in-place flattening: {e}")


def main():
    """Main entry point for the debugger."""
    parser = argparse.ArgumentParser(description="Transmog Debugger")

    # Add component selection
    parser.add_argument(
        "component",
        choices=["flatten", "process", "nulls", "depth", "arrays", "inplace"],
        help="Component or feature to debug",
    )

    # Add options
    parser.add_argument(
        "--verbose", action="store_true", help="Show detailed debug output"
    )
    parser.add_argument(
        "--data-type",
        choices=[
            "simple",
            "array",
            "scalar",
            "complex",
            "circular",
            "deep",
            "null_values",
        ],
        default="simple",
        help="Type of test data to use",
    )

    args = parser.parse_args()

    # Run appropriate debug function
    if args.component == "flatten":
        debug_flattener(args)
    elif args.component == "process":
        debug_processor(args)
    elif args.component == "nulls":
        debug_null_handling(args)
    elif args.component == "depth":
        debug_max_depth(args)
    elif args.component == "arrays":
        debug_array_handling(args)
    elif args.component == "inplace":
        debug_inplace_modification(args)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main()
    else:
        # If no arguments, run basic tests
        class Args:
            verbose = False
            data_type = "simple"

        print("Running basic tests (use --help for more options)")
        debug_flattener(Args())
