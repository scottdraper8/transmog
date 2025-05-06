#!/usr/bin/env python
"""
Debug script for testing circular reference handling in Transmog.

This script tests how the Transmog package handles circular references
in nested JSON data using different recovery strategies.
"""

import sys
import os
from copy import deepcopy
import json
from typing import Dict, Any, List, Optional

# Import from transmog package
from transmog import Processor, TransmogConfig
from transmog.core.flattener import flatten_json
from transmog.error import CircularReferenceError, PartialProcessingRecovery, LENIENT


def create_circular_data():
    """Create test data with circular references."""
    # Create a simple object that will contain a circular reference
    data = {
        "id": 1,
        "name": "Circular Test",
        "metadata": {
            "created_by": "debug_script",
            "created_at": "2023-01-01",
        },
        "child": {
            "id": 2,
            "name": "Child Object",
            "parent_ref": None,  # Will be set to parent (creating circular reference)
        },
    }

    # Create circular reference
    data["child"]["parent_ref"] = data

    return data


def test_direct_recovery():
    """Test direct recovery with flatten_json."""
    print("\n--- Testing Direct Recovery with flatten_json ---")

    # Create circular data
    data = create_circular_data()

    # Create a recovery strategy
    class CircularRefRecovery(PartialProcessingRecovery):
        def handle_circular_reference(self, error, path=None):
            print(f"Recovery detected circular reference at path: {path}")
            # Replace circular reference with simple marker
            return "[CIRCULAR]"

    recovery = CircularRefRecovery()

    print("Attempting to flatten with recovery strategy...")
    try:
        # Try flattening with recovery
        result = flatten_json(data, recovery_strategy=recovery)
        print("Success! Flattening with recovery completed.")
        print("Result:", json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error even with recovery: {type(e).__name__}: {str(e)}")


def test_processor_recovery():
    """Test recovery through the Processor class."""
    print("\n--- Testing Processor with Circular References ---")

    # Create circular data
    data = create_circular_data()

    # First try without recovery strategy
    print("Attempting to process without recovery strategy...")
    try:
        processor = Processor()
        result = processor.process(data, entity_name="test")
        print("Success! (Unexpected: processed without error)")
        print("Main table:", json.dumps(result.get_main_table(), indent=2))
    except Exception as e:
        print(f"Error as expected: {type(e).__name__}: {str(e)}")

    # Now try with lenient recovery strategy
    print("\nAttempting to process with lenient recovery strategy...")
    try:
        # Create processor with lenient recovery
        processor = Processor.with_partial_recovery()
        result = processor.process(data, entity_name="test")
        print("Success! Processing with recovery completed.")
        print("Main table:", json.dumps(result.get_main_table(), indent=2))
    except Exception as e:
        print(f"Error with recovery: {type(e).__name__}: {str(e)}")

    # Try with custom config
    print("\nAttempting to process with custom recovery strategy...")
    try:
        # Create a custom recovery strategy
        class CircularRefRecovery(PartialProcessingRecovery):
            def handle_circular_reference(self, error, path=None):
                print(f"Custom recovery detected circular reference at path: {path}")
                # Return replacement object
                return {"_circular_reference": True, "path": path}

        custom_config = TransmogConfig.default().with_error_handling(
            recovery_strategy=CircularRefRecovery()
        )
        processor = Processor(config=custom_config)
        result = processor.process(data, entity_name="test")
        print("Success! Processing with custom recovery completed.")
        print("Main table:", json.dumps(result.get_main_table(), indent=2))
    except Exception as e:
        print(f"Error with custom recovery: {type(e).__name__}: {str(e)}")


def test_nested_arrays_with_circular_refs():
    """Test processing arrays containing circular references."""
    print("\n--- Testing Arrays with Circular References ---")

    # Create test data with circular references in arrays
    data = {
        "id": 1,
        "name": "Array Test",
        "items": [
            {"id": 1, "name": "Item 1"},
            {"id": 2, "name": "Item 2", "parent": None},  # Will be circular
        ],
    }

    # Create circular reference
    data["items"][1]["parent"] = data

    # Try with lenient recovery strategy
    print("Attempting to process array with lenient recovery strategy...")
    try:
        processor = Processor.with_partial_recovery()
        result = processor.process(data, entity_name="test")
        print("Success! Processing array with recovery completed.")
        print("Main table:", json.dumps(result.get_main_table(), indent=2))

        # Check if child tables were extracted
        table_names = result.get_table_names()
        print(f"Extracted tables: {table_names}")

        for table_name in table_names:
            if table_name != "main":
                child_table = result.get_child_table(table_name)
                if child_table:
                    print(
                        f"Child table '{table_name}':",
                        json.dumps(child_table[:1], indent=2),
                    )
                    print(f"Total records: {len(child_table)}")
    except Exception as e:
        print(f"Error with array recovery: {type(e).__name__}: {str(e)}")


def test_deeply_nested_circular_refs():
    """Test processing deeply nested circular references."""
    print("\n--- Testing Deeply Nested Circular References ---")

    # Create deeply nested data with circular references
    data = {
        "id": 1,
        "name": "Deep Test",
        "level1": {
            "level2": {
                "level3": {
                    "level4": {
                        "level5": {
                            "data": "Very deep",
                            "root_ref": None,  # Will point back to root
                        }
                    }
                }
            }
        },
    }

    # Create circular reference
    data["level1"]["level2"]["level3"]["level4"]["level5"]["root_ref"] = data

    # Try with lenient recovery strategy
    print("Attempting to process deeply nested data with recovery...")
    try:
        processor = Processor.with_partial_recovery()
        result = processor.process(data, entity_name="test")
        print("Success! Processing deeply nested data completed.")
        print("Main table:", json.dumps(result.get_main_table(), indent=2))
    except Exception as e:
        print(f"Error with deep nesting: {type(e).__name__}: {str(e)}")


if __name__ == "__main__":
    print("=== Transmog Circular Reference Debugging ===")

    # Run all tests
    test_direct_recovery()
    test_processor_recovery()
    test_nested_arrays_with_circular_refs()
    test_deeply_nested_circular_refs()
