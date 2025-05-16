#!/usr/bin/env python3
"""Examples of deterministic ID generation in Transmog.

This script demonstrates different approaches to generating
deterministic IDs in Transmog, which ensures consistent IDs
across multiple processing runs with the same data.

Each example processes the same data twice to demonstrate the
consistency (or lack thereof) of IDs across multiple runs.
"""

import os
import sys
import uuid

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from transmog package
from transmog import Processor


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
        "\nNotice that the IDs are different in each run, "
        "even though the data is the same."
    )


def example_root_deterministic_ids():
    """Example of deterministic IDs at root level only."""
    print_header("Example 2: Root-Level Deterministic IDs")

    # Create test data
    data = {
        "id": "ROOT123",
        "name": "Example Root",
        "customers": [{"customer_id": "CUST001", "name": "Customer 1"}],
        "products": [{"sku": "PROD001", "name": "Product 1"}],
    }

    # Create processor with deterministic ID for root level only
    # Use the with_deterministic_ids factory method with table-prefixed mapping
    processor = Processor.with_deterministic_ids(
        {
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

    # Create processor with deterministic IDs at multiple levels using table names
    processor = Processor.with_deterministic_ids(
        {
            "": "id",  # Root level uses "id" field
            "store_customers": "customer_id",  # Customers use "customer_id" field
            "store_customers_orders": "order_id",  # Orders use "order_id" field
            "store_products": "sku",  # Products use "sku" field
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


def example_table_name_strategy():
    """Example of using table names for deterministic IDs."""
    print_header("Example 4: Table Name Strategy for Deterministic IDs")

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

    # Create processor with table name-based deterministic IDs
    # Note: This differs from the old wildcard pattern approach
    processor = Processor.with_deterministic_ids(
        {
            "": "id",  # Root level uses "id" field
            "store_customers": "id",  # Customers table uses "id" field
            "store_products": "id",  # Products table uses "id" field
            "store_events": "id",  # Events table uses "id" field
        }
    )

    # Process the same data twice
    print("\nProcessing the same data twice with table name strategy...")
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
    processor = Processor.with_custom_id_generation(custom_id_generator)

    # Process the same data twice
    print("\nProcessing the same data twice with custom ID generation...")
    result1 = processor.process(data, entity_name="store")
    result2 = processor.process(data, entity_name="store")

    # Print IDs from both runs
    print_ids(result1, "First Run")
    print_ids(result2, "Second Run")

    print("\nNotice the custom ID format that includes record type information,")
    print("while still maintaining consistency across runs.")


def example_comparison_table():
    """Create a comparison of different ID generation approaches."""
    print_header("Comparison of ID Generation Approaches")

    # Create test data
    data = {
        "id": "ROOT123",
        "customer_id": "ROOT_CUST",
        "name": "Example Root",
        "customers": [
            {
                "id": "CUST001_ID",
                "customer_id": "CUST001",
                "name": "Customer 1",
                "orders": [{"id": "ORD001_ID", "order_id": "ORD001", "total": 100.50}],
            }
        ],
    }

    # 1. Default random UUIDs
    random_processor = Processor.default()
    random_result = random_processor.process(data, entity_name="store")

    # 2. Root-level deterministic IDs
    root_processor = Processor.with_deterministic_ids({"": "id"})
    root_result = root_processor.process(data, entity_name="store")

    # 3. Multi-level with customer_id and order_id fields
    multilevel_processor = Processor.with_deterministic_ids(
        {
            "": "id",
            "store_customers": "customer_id",
            "store_customers_orders": "order_id",
        }
    )
    multilevel_result = multilevel_processor.process(data, entity_name="store")

    # 4. Multi-level with id field consistently
    consistent_id_processor = Processor.with_deterministic_ids(
        {
            "": "id",
            "store_customers": "id",
            "store_customers_orders": "id",
        }
    )
    consistent_id_result = consistent_id_processor.process(data, entity_name="store")

    # 5. Custom ID generation
    def prefix_id_generator(record):
        """Generate IDs with prefixes based on available fields."""
        if "id" in record:
            return f"PREFIX-{record['id']}"
        elif "customer_id" in record:
            return f"PREFIX-{record['customer_id']}"
        elif "order_id" in record:
            return f"PREFIX-{record['order_id']}"
        return str(uuid.uuid4())

    custom_processor = Processor.with_custom_id_generation(prefix_id_generator)
    custom_result = custom_processor.process(data, entity_name="store")

    # Print comparison table
    print("\nComparison of different ID generation approaches:\n")
    print(
        f"{'Approach':<30} | {'Root ID':<36} | {'Customer ID':<36} | {'Order ID':<36}"
    )
    print("-" * 140)

    def get_ids(result):
        """Extract IDs from tables in result."""
        tables = result.to_dict()
        root_id = tables["main"][0]["__extract_id"] if tables["main"] else "N/A"
        customer_id = (
            tables["store_customers"][0]["__extract_id"]
            if "store_customers" in tables and tables["store_customers"]
            else "N/A"
        )
        order_id = (
            tables["store_customers_orders"][0]["__extract_id"]
            if "store_customers_orders" in tables and tables["store_customers_orders"]
            else "N/A"
        )
        return root_id, customer_id, order_id

    random_ids = get_ids(random_result)
    root_ids = get_ids(root_result)
    multilevel_ids = get_ids(multilevel_result)
    consistent_ids = get_ids(consistent_id_result)
    custom_ids = get_ids(custom_result)

    print(
        f"{'1. Default Random UUIDs':<30} | {random_ids[0]:<36} | "
        f"{random_ids[1]:<36} | {random_ids[2]:<36}"
    )
    print(
        f"{'2. Root-level Deterministic':<30} | {root_ids[0]:<36} | "
        f"{root_ids[1]:<36} | {root_ids[2]:<36}"
    )
    print(
        f"{'3. Multi-level (different fields)':<30} | {multilevel_ids[0]:<36} | "
        f"{multilevel_ids[1]:<36} | {multilevel_ids[2]:<36}"
    )
    print(
        f"{'4. Multi-level (consistent field)':<30} | {consistent_ids[0]:<36} | "
        f"{consistent_ids[1]:<36} | {consistent_ids[2]:<36}"
    )
    print(
        f"{'5. Custom ID generator':<30} | {custom_ids[0]:<36} | "
        f"{custom_ids[1]:<36} | {custom_ids[2]:<36}"
    )

    print("\nKey observations:")
    print("- Default: All IDs are random UUIDs")
    print("- Root-level: Only the root record has a deterministic ID")
    print("- Multi-level (different fields): Each level uses a different source field")
    print(
        "- Multi-level (consistent field): All levels use the 'id' field consistently"
    )
    print("- Custom generator: Complete flexibility with custom prefixing and logic")


def main():
    """Run all examples."""
    print("\nTransmog Deterministic ID Examples")
    print("===================================")
    print("This script demonstrates various approaches to generating")
    print("deterministic IDs in Transmog, ensuring consistency across")
    print("multiple processing runs with the same data.")

    # Create output directory for any file outputs
    output_dir = os.path.join(os.path.dirname(__file__), "output", "deterministic_ids")
    os.makedirs(output_dir, exist_ok=True)

    # Example 1: Default Random UUIDs
    example_random_uuids()

    # Example 2: Root-Level Deterministic IDs
    example_root_deterministic_ids()

    # Example 3: Multi-level Deterministic IDs
    example_multi_level_deterministic_ids()

    # Example 4: Table Name Strategy
    example_table_name_strategy()

    # Example 5: Custom ID Generation
    example_custom_id_generation()

    # Additional: Comparison Table
    example_comparison_table()


if __name__ == "__main__":
    main()
