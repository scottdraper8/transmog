#!/usr/bin/env python3
"""
Examples of deterministic ID generation in Transmog.

This script demonstrates the different approaches to ID generation:
1. Default random UUIDs
2. Field-based deterministic IDs at different path levels
3. Custom ID generation with a custom function

Each example processes the same data twice to demonstrate the
consistency (or lack thereof) of IDs across multiple runs.
"""

import uuid
import json
from datetime import datetime
import pandas as pd
from src.transmog import Processor


def print_header(title):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def print_ids(result, label=""):
    """Print IDs from a processing result."""
    tables = result.to_dict()

    # Print with label if provided
    if label:
        print(f"\n{label}:")

    # Print IDs from each table
    for table_name, records in tables.items():
        print(f"\n- Table: {table_name}")
        for i, record in enumerate(records):
            # Extract key fields for display alongside ID
            display_fields = []
            for field in ["id", "customer_id", "order_id", "sku", "name"]:
                if field in record:
                    display_fields.append(f"{field}={record[field]}")

            fields_str = ", ".join(display_fields)
            print(f"  Record {i + 1}: {record['__extract_id']} ({fields_str})")


def example_random_uuids():
    """Example of default random UUID generation."""
    print_header("Example 1: Default Random UUIDs")

    # Create test data
    data = {
        "id": "ROOT123",
        "name": "Example Root",
        "customers": [{"customer_id": "CUST001", "name": "Customer 1"}],
        "products": [{"sku": "PROD001", "name": "Product 1"}],
    }

    # Create processor with default settings (random UUIDs)
    processor = Processor()

    # Process the same data twice
    print("\nProcessing the same data twice with random UUIDs...")
    result1 = processor.process(data, entity_name="store")
    result2 = processor.process(data, entity_name="store")

    # Print IDs from both runs
    print_ids(result1, "First Run")
    print_ids(result2, "Second Run")

    print(
        "\nNotice that the IDs are different in each run, even though the data is the same."
    )


def example_root_deterministic_ids():
    """Example of deterministic IDs at the root level only."""
    print_header("Example 2: Root-level Deterministic IDs")

    # Create test data
    data = {
        "id": "ROOT123",
        "name": "Example Root",
        "customers": [{"customer_id": "CUST001", "name": "Customer 1"}],
        "products": [{"sku": "PROD001", "name": "Product 1"}],
    }

    # Create processor with deterministic ID for root level only
    processor = Processor(
        deterministic_id_fields={
            "": "id"  # Root level uses "id" field
        }
    )

    # Process the same data twice
    print("\nProcessing the same data twice with root-level deterministic IDs...")
    result1 = processor.process(data, entity_name="store")
    result2 = processor.process(data, entity_name="store")

    # Print IDs from both runs
    print_ids(result1, "First Run")
    print_ids(result2, "Second Run")

    print("\nNotice that the root record has the same ID in both runs,")
    print("but child records still have different random IDs.")


def example_multi_level_deterministic_ids():
    """Example of deterministic IDs at multiple levels."""
    print_header("Example 3: Multi-level Deterministic IDs")

    # Create test data with nested structure
    data = {
        "id": "ROOT123",
        "name": "Example Root",
        "customers": [
            {
                "customer_id": "CUST001",
                "name": "Customer 1",
                "orders": [
                    {"order_id": "ORD001", "total": 100.50},
                    {"order_id": "ORD002", "total": 75.25},
                ],
            },
            {"customer_id": "CUST002", "name": "Customer 2", "orders": []},
        ],
        "products": [
            {"sku": "PROD001", "name": "Product 1", "price": 25.00},
            {"sku": "PROD002", "name": "Product 2", "price": 50.00},
        ],
    }

    # Create processor with deterministic IDs at multiple levels
    processor = Processor(
        deterministic_id_fields={
            "": "id",  # Root level uses "id" field
            "customers": "customer_id",  # Customers use "customer_id" field
            "customers_orders": "order_id",  # Orders use "order_id" field
            "products": "sku",  # Products use "sku" field
        }
    )

    # Process the same data twice
    print("\nProcessing the same data twice with multi-level deterministic IDs...")
    result1 = processor.process(data, entity_name="store")
    result2 = processor.process(data, entity_name="store")

    # Print IDs from both runs
    print_ids(result1, "First Run")
    print_ids(result2, "Second Run")

    print("\nNotice that all records have the same IDs in both runs,")
    print("ensuring consistency across processing.")


def example_wildcard_pattern():
    """Example of using wildcard patterns for deterministic IDs."""
    print_header("Example 4: Wildcard Pattern for Deterministic IDs")

    # Create test data
    data = {
        "id": "ROOT123",
        "name": "Example Root",
        "customers": [{"id": "CUST001", "name": "Customer 1"}],
        "products": [{"id": "PROD001", "name": "Product 1"}],
        "events": [
            {"id": "EVENT001", "type": "sale"},
            {"id": "EVENT002", "type": "return"},
        ],
    }

    # Create processor with wildcard pattern
    processor = Processor(
        deterministic_id_fields={
            "*": "id"  # Use "id" field at all paths
        }
    )

    # Process the same data twice
    print("\nProcessing the same data twice with wildcard pattern...")
    result1 = processor.process(data, entity_name="store")
    result2 = processor.process(data, entity_name="store")

    # Print IDs from both runs
    print_ids(result1, "First Run")
    print_ids(result2, "Second Run")

    print("\nNotice that all records with an 'id' field have consistent IDs.")


def example_custom_id_generation():
    """Example of custom ID generation function."""
    print_header("Example 5: Custom ID Generation")

    # Create test data
    data = {
        "id": "ROOT123",
        "type": "store",
        "name": "Example Store",
        "customers": [
            {"customer_id": "CUST001", "type": "retail", "name": "Customer 1"},
            {"customer_id": "CUST002", "type": "wholesale", "name": "Customer 2"},
        ],
        "products": [
            {"sku": "PROD001", "category": "electronics", "name": "Product 1"},
            {"sku": "PROD002", "category": "furniture", "name": "Product 2"},
        ],
    }

    # Define custom ID generation function
    def custom_id_generator(record):
        """Generate custom IDs based on record contents."""
        # Root record
        if "id" in record and "type" in record and record.get("type") == "store":
            return f"STORE-{record['id']}"

        # Customer records
        elif "customer_id" in record:
            customer_type = record.get("type", "unknown")
            return f"CUSTOMER-{customer_type.upper()}-{record['customer_id']}"

        # Product records
        elif "sku" in record and "category" in record:
            return f"PRODUCT-{record['category'].upper()}-{record['sku']}"

        # Fall back to random UUID for any other records
        return str(uuid.uuid4())

    # Create processor with custom ID generation
    processor = Processor(id_generation_strategy=custom_id_generator)

    # Process the same data twice
    print("\nProcessing the same data twice with custom ID generation...")
    result1 = processor.process(data, entity_name="store")
    result2 = processor.process(data, entity_name="store")

    # Print IDs from both runs
    print_ids(result1, "First Run")
    print_ids(result2, "Second Run")

    print("\nNotice the custom ID format that includes record type information,")
    print("while still maintaining consistency across runs.")


def example_incremental_loading():
    """Example demonstrating incremental loading with deterministic IDs."""
    print_header("Example 6: Incremental Loading with Deterministic IDs")

    # Initial data set
    initial_data = {
        "id": "STORE001",
        "name": "Main Store",
        "customers": [
            {"customer_id": "CUST001", "name": "Customer 1"},
            {"customer_id": "CUST002", "name": "Customer 2"},
        ],
        "products": [
            {"sku": "PROD001", "name": "Product 1", "price": 25.00},
            {"sku": "PROD002", "name": "Product 2", "price": 50.00},
        ],
    }

    # Additional data (incremental update)
    additional_data = {
        "id": "STORE001",
        "name": "Main Store Updated",  # Name updated
        "customers": [
            {
                "customer_id": "CUST002",
                "name": "Customer 2 Updated",
            },  # Updated customer
            {"customer_id": "CUST003", "name": "Customer 3"},  # New customer
        ],
        "products": [
            {"sku": "PROD003", "name": "Product 3", "price": 75.00}  # New product
        ],
    }

    # Create processor with deterministic IDs
    processor = Processor(
        deterministic_id_fields={
            "": "id",
            "customers": "customer_id",
            "products": "sku",
        }
    )

    # Process initial data
    print("\nProcessing initial data...")
    initial_result = processor.process(initial_data, entity_name="store")
    print_ids(initial_result, "Initial Data")

    # Process additional data (incremental update)
    print("\nProcessing additional data (incremental update)...")
    additional_result = processor.process(additional_data, entity_name="store")
    print_ids(additional_result, "Additional Data")

    # Show how to identify new vs. updated records
    print("\nDemonstrating record matching across batches:")

    # Extract IDs from both runs
    initial_tables = initial_result.to_dict()
    additional_tables = additional_result.to_dict()

    # Compare customer IDs
    print("\nCustomer Records Analysis:")
    initial_customer_ids = {
        r["customer_id"]: r["__extract_id"]
        for r in initial_tables.get("store_customers", [])
    }
    additional_customer_ids = {
        r["customer_id"]: r["__extract_id"]
        for r in additional_tables.get("store_customers", [])
    }

    # Check for matching IDs (updated records)
    for customer_id, extract_id in additional_customer_ids.items():
        if customer_id in initial_customer_ids:
            match = (
                "MATCH (updated)"
                if extract_id == initial_customer_ids[customer_id]
                else "ID MISMATCH"
            )
            print(f"  Customer {customer_id}: {match}")
        else:
            print(f"  Customer {customer_id}: NEW")

    print("\nThis demonstrates how deterministic IDs allow you to match records across")
    print(
        "incremental data loads, making it easier to identify new vs. updated records."
    )


def example_comparison_table():
    """Create a comparison table of the different ID generation approaches."""
    print_header("Comparison of ID Generation Approaches")

    # Create simple test data
    data = {
        "id": "RECORD123",
        "name": "Test Record",
        "items": [
            {"item_id": "ITEM001", "name": "Item 1"},
            {"item_id": "ITEM002", "name": "Item 2"},
        ],
    }

    # Define processors with different strategies
    processors = {
        "Random UUIDs (Default)": Processor(),
        "Root Deterministic": Processor(deterministic_id_fields={"": "id"}),
        "Multi-level Deterministic": Processor(
            deterministic_id_fields={"": "id", "items": "item_id"}
        ),
        "Custom Function": Processor(
            id_generation_strategy=lambda r: f"CUSTOM-{r.get('id', r.get('item_id', 'UNKNOWN'))}"
        ),
    }

    # Process data with each strategy twice
    results = {}
    for name, processor in processors.items():
        # Process twice to show consistency (or lack thereof)
        run1 = processor.process(data, entity_name="test")
        run2 = processor.process(data, entity_name="test")

        # Store results
        results[name] = {"Run 1": run1, "Run 2": run2}

    # Create comparison table
    print("\nID Consistency Across Multiple Runs:\n")

    # Table headers
    headers = [
        "Strategy",
        "Root ID (Run 1)",
        "Root ID (Run 2)",
        "Same?",
        "Item IDs Same?",
        "Notes",
    ]

    # Table rows
    rows = []
    for name, runs in results.items():
        root_id1 = runs["Run 1"].to_dict()["main"][0]["__extract_id"]
        root_id2 = runs["Run 2"].to_dict()["main"][0]["__extract_id"]

        # Check if item IDs are the same across runs
        items1 = runs["Run 1"].to_dict().get("test_items", [])
        items2 = runs["Run 2"].to_dict().get("test_items", [])

        if items1 and items2:
            # Sort by item_id for consistent comparison
            items1.sort(key=lambda x: x.get("item_id", ""))
            items2.sort(key=lambda x: x.get("item_id", ""))

            items_same = all(
                items1[i]["__extract_id"] == items2[i]["__extract_id"]
                for i in range(min(len(items1), len(items2)))
            )
        else:
            items_same = "N/A"

        # Add notes based on strategy
        if name == "Random UUIDs (Default)":
            notes = "Different IDs each run"
        elif name == "Root Deterministic":
            notes = "Consistent root ID, random item IDs"
        elif name == "Multi-level Deterministic":
            notes = "Consistent IDs at all levels"
        else:
            notes = "Custom formatting with consistency"

        # Add row
        rows.append(
            [
                name,
                root_id1[:8] + "...",  # Truncate for display
                root_id2[:8] + "...",
                "Yes" if root_id1 == root_id2 else "No",
                "Yes"
                if items_same == True
                else "No"
                if items_same == False
                else items_same,
                notes,
            ]
        )

    # Print as markdown table
    print(f"| {' | '.join(headers)} |")
    print(f"| {' | '.join(['---' for _ in headers])} |")
    for row in rows:
        print(f"| {' | '.join(str(cell) for cell in row)} |")


def main():
    """Run all examples."""
    print("\nDeterministic ID Generation Examples")
    print(
        "\nThis script demonstrates different approaches to ID generation in Transmog,"
    )
    print("including random UUIDs, deterministic IDs, and custom ID generation.")

    # Run individual examples
    example_random_uuids()
    example_root_deterministic_ids()
    example_multi_level_deterministic_ids()
    example_wildcard_pattern()
    example_custom_id_generation()
    example_incremental_loading()
    example_comparison_table()

    print("\nEnd of examples.")


if __name__ == "__main__":
    main()
