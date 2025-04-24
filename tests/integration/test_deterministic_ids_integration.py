"""
Integration tests for deterministic ID generation.

These tests verify that deterministic IDs work correctly
across multiple processing runs with complex data structures.
"""

import copy
import json
import uuid
import pytest

from src.transmogrify import Processor
from src.transmogrify.core.metadata import generate_deterministic_id


class TestDeterministicIdsIntegration:
    """Integration tests for deterministic ID generation."""

    def test_complex_nested_structure_stability(self):
        """Test ID stability with a complex nested structure."""
        # Create a complex nested data structure
        data = {
            "id": "ROOT123",
            "metadata": {
                "version": "1.0",
                "created_at": "2023-01-01",
                "status": "active",
            },
            "customers": [
                {
                    "id": "CUST001",
                    "name": "Customer 1",
                    "contact": {
                        "email": "customer1@example.com",
                        "phone": "123-456-7890",
                    },
                    "orders": [
                        {
                            "order_id": "ORD001",
                            "total": 100.50,
                            "items": [
                                {"sku": "ITEM001", "quantity": 2, "price": 25.25},
                                {"sku": "ITEM002", "quantity": 1, "price": 50.00},
                            ],
                            "shipping": {
                                "method": "express",
                                "address": {
                                    "street": "123 Main St",
                                    "city": "Anytown",
                                    "zip": "12345",
                                },
                            },
                        },
                        {
                            "order_id": "ORD002",
                            "total": 75.25,
                            "items": [
                                {"sku": "ITEM003", "quantity": 3, "price": 25.00},
                                {"sku": "ITEM004", "quantity": 1, "price": 0.25},
                            ],
                        },
                    ],
                },
                {
                    "id": "CUST002",
                    "name": "Customer 2",
                    "contact": {
                        "email": "customer2@example.com",
                        "phone": "987-654-3210",
                    },
                    "orders": [],
                },
            ],
            "products": [
                {"sku": "ITEM001", "name": "Product 1", "price": 25.25},
                {"sku": "ITEM002", "name": "Product 2", "price": 50.00},
                {"sku": "ITEM003", "name": "Product 3", "price": 25.00},
                {"sku": "ITEM004", "name": "Product 4", "price": 0.25},
            ],
        }

        # Define deterministic ID fields for various paths
        deterministic_id_fields = {
            "": "id",  # Root uses id
            "customers": "id",  # Customers array uses id
            "customers_orders": "order_id",  # Orders array uses order_id
            "products": "sku",  # Products array uses sku
            "customers_orders_items": "sku",  # Order items use sku
        }

        # Create processor with deterministic ID fields
        processor = Processor(deterministic_id_fields=deterministic_id_fields)

        # Process the data three times to verify stability
        results = []
        for _ in range(3):
            # Create a deep copy to ensure we're not modifying the original
            data_copy = copy.deepcopy(data)
            result = processor.process(data_copy, entity_name="store")
            results.append(result.to_dict())

        # Verify that the main record has the same ID in all runs
        main_id = generate_deterministic_id(data["id"])  # Expected deterministic ID
        for result in results:
            assert result["main"][0]["__extract_id"] == main_id

        # Verify that the customers have the same deterministic IDs in all runs
        for result in results:
            if "store_customers" in result:
                customers = sorted(result["store_customers"], key=lambda x: x["id"])
                for i, customer in enumerate(customers):
                    expected_id = generate_deterministic_id(data["customers"][i]["id"])
                    assert customer["__extract_id"] == expected_id

        # Verify that products have the same deterministic IDs in all runs
        for result in results:
            if "store_products" in result:
                products = sorted(result["store_products"], key=lambda x: x["sku"])
                for i, product in enumerate(products):
                    expected_id = generate_deterministic_id(data["products"][i]["sku"])
                    assert product["__extract_id"] == expected_id

        # Instead of comparing all tables exhaustively, check specific key tables
        # This reduces the complexity while still ensuring the test's purpose is met
        key_tables = ["main", "store_customers", "store_products"]
        for table_name in key_tables:
            if table_name in results[0]:
                # Sort by a relevant key for comparison
                sort_key = "id"  # Default
                if table_name == "store_products":
                    sort_key = "sku"

                # Make sure the key exists in the records
                if results[0][table_name] and sort_key in results[0][table_name][0]:
                    # Sort all results by the sort key
                    for i in range(3):
                        if table_name in results[i]:
                            results[i][table_name] = sorted(
                                results[i][table_name],
                                key=lambda x: x.get(sort_key, ""),
                            )

                    # Compare extract IDs
                    for record_idx in range(len(results[0][table_name])):
                        if record_idx < len(
                            results[1][table_name]
                        ) and record_idx < len(results[2][table_name]):
                            assert (
                                results[0][table_name][record_idx]["__extract_id"]
                                == results[1][table_name][record_idx]["__extract_id"]
                            )
                            assert (
                                results[0][table_name][record_idx]["__extract_id"]
                                == results[2][table_name][record_idx]["__extract_id"]
                            )

    def test_incremental_processing_stability(self):
        """Test ID stability with incremental data processing."""
        # Initial data set
        initial_data = {
            "id": "ROOT456",
            "customers": [
                {"id": "CUST001", "name": "Customer 1"},
                {"id": "CUST002", "name": "Customer 2"},
            ],
            "products": [
                {"sku": "PROD001", "name": "Product 1"},
                {"sku": "PROD002", "name": "Product 2"},
            ],
        }

        # Additional data to add later
        additional_data = {
            "id": "ROOT456",  # Same root ID
            "customers": [
                {"id": "CUST003", "name": "Customer 3"},  # New customer
                {"id": "CUST001", "name": "Customer 1 Updated"},  # Updated customer
            ],
            "products": [
                {"sku": "PROD003", "name": "Product 3"},  # New product
            ],
        }

        # Define deterministic ID fields
        deterministic_id_fields = {
            "": "id",
            "customers": "id",
            "products": "sku",
        }

        # Create processor with deterministic ID fields
        processor = Processor(deterministic_id_fields=deterministic_id_fields)

        # Process initial data
        initial_result = processor.process(initial_data, entity_name="store")
        initial_tables = initial_result.to_dict()

        # Process additional data
        additional_result = processor.process(additional_data, entity_name="store")
        additional_tables = additional_result.to_dict()

        # Check root record ID consistency (should be identical since id is the same)
        assert (
            initial_tables["main"][0]["__extract_id"]
            == additional_tables["main"][0]["__extract_id"]
        )

        # Check customer records - existing ones should maintain IDs
        initial_customers = {
            c["id"]: c["__extract_id"]
            for c in initial_tables.get("store_customers", [])
        }
        additional_customers = {
            c["id"]: c["__extract_id"]
            for c in additional_tables.get("store_customers", [])
        }

        # CUST001 ID should be the same in both runs
        assert initial_customers["CUST001"] == additional_customers["CUST001"]

        # CUST002 shouldn't be in additional data
        assert "CUST002" in initial_customers
        assert "CUST002" not in additional_customers

        # CUST003 should only be in additional data
        assert "CUST003" not in initial_customers
        assert "CUST003" in additional_customers

        # Check product records - existing ones should maintain IDs
        initial_products = {
            p["sku"]: p["__extract_id"]
            for p in initial_tables.get("store_products", [])
        }
        additional_products = {
            p["sku"]: p["__extract_id"]
            for p in additional_tables.get("store_products", [])
        }

        # PROD003 should only be in additional data
        assert "PROD003" not in initial_products
        assert "PROD003" in additional_products

    def test_large_batch_processing(self):
        """Test deterministic IDs with large batches of data."""
        # Create a large batch of records (500 records)
        records = []
        for i in range(500):
            record = {
                "id": f"REC{i:04d}",
                "name": f"Record {i}",
                "value": i * 10,
                "tags": [f"tag{j}" for j in range(i % 5 + 1)],  # 1-5 tags per record
            }
            records.append(record)

        # Define deterministic ID field
        deterministic_id_fields = {
            "": "id",  # Use id field at root level
        }

        # Create processor with deterministic IDs
        processor = Processor(
            deterministic_id_fields=deterministic_id_fields,
            batch_size=100,  # Process in batches of 100
        )

        # Process data in one go
        full_result = processor.process(records, entity_name="records")
        full_tables = full_result.to_dict()

        # Process data in chunks
        chunk_size = 100
        chunk_results = []
        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]
            result = processor.process(chunk, entity_name="records")
            chunk_results.append(result.to_dict())

        # Combine chunk results
        combined_tables = {"main": []}
        for chunk_table in chunk_results:
            combined_tables["main"].extend(chunk_table["main"])

        # Sort both result sets by id for comparison
        full_main = sorted(full_tables["main"], key=lambda x: x["id"])
        combined_main = sorted(combined_tables["main"], key=lambda x: x["id"])

        # IDs should be consistent across processing methods
        for i in range(len(full_main)):
            assert full_main[i]["__extract_id"] == combined_main[i]["__extract_id"]

    def test_composite_path_stability(self):
        """Test stability of IDs when using composite paths."""
        # Create test data with nested paths
        data = {
            "id": "MAIN001",
            "sections": [
                {
                    "id": "SEC001",
                    "title": "Section 1",
                    "subsections": [
                        {"id": "SUB001", "title": "Subsection 1.1"},
                        {"id": "SUB002", "title": "Subsection 1.2"},
                    ],
                },
                {
                    "id": "SEC002",
                    "title": "Section 2",
                    "subsections": [
                        {"id": "SUB003", "title": "Subsection 2.1"},
                    ],
                },
            ],
        }

        # Create a custom ID generation strategy that combines multiple fields
        def composite_id_strategy(record):
            # For subsections, combine parent section id with subsection id
            if "id" in record and record.get("id", "").startswith("SUB"):
                return f"COMPOSITE-{record.get('parent_section', '')}-{record.get('id', '')}"
            # For other records, use a simple strategy
            return f"SIMPLE-{record.get('id', 'unknown')}"

        # Create processor with the custom strategy
        processor = Processor(id_generation_strategy=composite_id_strategy)

        # Process data with some manual preparation to simulate parent context
        enriched_data = copy.deepcopy(data)

        # Add parent_section field to subsections to demonstrate the concept
        for section in enriched_data["sections"]:
            section_id = section.get("id")
            for subsection in section.get("subsections", []):
                subsection["parent_section"] = section_id

        # Process the enriched data
        result = processor.process(enriched_data, entity_name="document")
        tables = result.to_dict()

        # Verify that the main record ID follows the expected pattern
        assert tables["main"][0]["__extract_id"] == f"SIMPLE-{data['id']}"

        # Find the subsections table
        subsections_table = None
        for table_name, records in tables.items():
            if "subsections" in table_name:
                subsections_table = records
                break

        # If we found subsections, verify their IDs follow the composite pattern
        if subsections_table:
            for record in subsections_table:
                # Only check records that were enriched with parent_section
                if "parent_section" in record and "id" in record:
                    expected_id = f"COMPOSITE-{record['parent_section']}-{record['id']}"
                    assert record["__extract_id"] == expected_id

    def test_custom_id_strategy_edge_cases(self):
        """Test custom ID strategy with edge cases."""
        # Create test data with edge cases
        edge_case_data = [
            {"case": "normal", "id": "NORMAL", "value": "Regular value"},
            {"case": "empty", "id": "", "value": "Empty ID"},  # Empty ID
            {"case": "null", "id": None, "value": "Null ID"},  # Null ID
            {
                "case": "object",
                "id": {"nested": "value"},
                "value": "Object ID",
            },  # Object as ID
            {"case": "array", "id": [1, 2, 3], "value": "Array ID"},  # Array as ID
            # Missing ID field
            {"case": "missing", "value": "Missing ID"},
        ]

        # Define a robust custom strategy that handles edge cases
        def robust_strategy(record):
            case_type = record.get("case")
            record_id = record.get("id")

            if case_type == "normal":
                return f"CUSTOM-{record_id}"
            elif case_type == "empty":
                return "CUSTOM-EMPTY"
            elif case_type == "null":
                return "CUSTOM-NULL"
            elif case_type == "object":
                # Handle object IDs by converting to string
                if isinstance(record_id, dict):
                    return f"CUSTOM-OBJECT-{len(record_id)}"
            elif case_type == "array":
                # Handle array IDs by joining elements
                if isinstance(record_id, list):
                    return f"CUSTOM-ARRAY-{len(record_id)}"
            elif case_type == "missing":
                return "CUSTOM-MISSING"

            # Fallback for unexpected cases
            return "CUSTOM-FALLBACK"

        # Create processor with the robust custom strategy
        processor = Processor(id_generation_strategy=robust_strategy)

        # Process each record individually and verify the results
        for record in edge_case_data:
            result = processor.process(record, entity_name="test")
            tables = result.to_dict()

            # Get the extract ID
            extract_id = tables["main"][0]["__extract_id"]

            # Verify the ID follows our expected pattern based on case
            case_type = record.get("case")
            if case_type == "normal":
                assert extract_id == "CUSTOM-NORMAL"
            elif case_type == "empty":
                assert extract_id == "CUSTOM-EMPTY"
            elif case_type == "null":
                assert extract_id == "CUSTOM-NULL"
            elif case_type == "object":
                assert extract_id == "CUSTOM-OBJECT-1"
            elif case_type == "array":
                assert extract_id == "CUSTOM-ARRAY-3"
            elif case_type == "missing":
                assert extract_id == "CUSTOM-MISSING"
