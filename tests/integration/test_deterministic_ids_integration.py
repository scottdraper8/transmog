"""
Integration tests for deterministic ID generation.

These tests verify that deterministic IDs work correctly
across multiple processing runs with complex data structures.
"""

import copy
import uuid

from transmog import Processor
from transmog.core.metadata import generate_deterministic_id


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

        # Create processor with deterministic ID field
        processor = Processor.with_deterministic_ids("id")

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
            assert result["main_table"][0]["__extract_id"] == main_id

        # Verify that the customers have the same deterministic IDs in all runs
        for result in results:
            if "store_customers" in result:
                customers = sorted(result["store_customers"], key=lambda x: x["id"])
                for i, customer in enumerate(customers):
                    expected_id = generate_deterministic_id(data["customers"][i]["id"])
                    assert customer["__extract_id"] == expected_id

        # All other tables will use random IDs with this approach, so we don't test them

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

        # Create processor with deterministic ID field
        processor = Processor.with_deterministic_ids("id")

        # Process the initial data
        initial_result = processor.process(initial_data, entity_name="store")

        # Process the additional data
        additional_result = processor.process(additional_data, entity_name="store")

        # Verify root entity has the same ID
        initial_main_id = initial_result.get_main_table()[0]["__extract_id"]
        additional_main_id = additional_result.get_main_table()[0]["__extract_id"]
        assert initial_main_id == additional_main_id

        # Verify customers have consistent business keys across runs
        initial_customers = initial_result.get_child_table("store_customers")
        additional_customers = additional_result.get_child_table("store_customers")

        # Find Customer 1 in both results
        initial_cust1 = next(c for c in initial_customers if c["id"] == "CUST001")
        additional_cust1 = next(c for c in additional_customers if c["id"] == "CUST001")

        # Customer 1 should have the same business key (id) in both runs
        assert initial_cust1["id"] == additional_cust1["id"]

        # But the __extract_id may be different since deterministic IDs
        # are only applied at the main record level in the current implementation
        # In a future enhancement, we might want deterministic IDs to cascade
        # to child records based on their business keys

        # Customer 1 name should be updated in the second run
        assert initial_cust1["name"] == "Customer 1"
        assert additional_cust1["name"] == "Customer 1 Updated"

        # Verify that Customer 2 is not in the additional results
        additional_cust2_ids = [c["id"] for c in additional_customers]
        assert "CUST002" not in additional_cust2_ids

        # Verify that Customer 3 is new in the additional results
        additional_cust3 = next(c for c in additional_customers if c["id"] == "CUST003")
        assert additional_cust3 is not None

    def test_large_batch_processing(self):
        """Test deterministic ID generation with large batches."""
        # Create a large batch of records
        batch_size = 100
        base_data = []
        for i in range(batch_size):
            base_data.append(
                {
                    "record_id": f"RECORD_{i:03d}",
                    "value": f"Value {i}",
                    "timestamp": f"2023-01-{(i % 30) + 1:02d}",
                }
            )

        # Process the batch with deterministic IDs
        processor = Processor.with_deterministic_ids("record_id")
        result1 = processor.process(base_data, entity_name="records")

        # Process again to verify stability
        result2 = processor.process(base_data, entity_name="records")

        # Compare the IDs from both runs
        main_table1 = result1.get_main_table()
        main_table2 = result2.get_main_table()

        # Sort both tables by record_id for comparison
        main_table1 = sorted(main_table1, key=lambda x: x["record_id"])
        main_table2 = sorted(main_table2, key=lambda x: x["record_id"])

        # Compare IDs
        for i in range(batch_size):
            record1 = main_table1[i]
            record2 = main_table2[i]
            assert record1["record_id"] == record2["record_id"]
            assert record1["__extract_id"] == record2["__extract_id"]
            # Verify the ID is actually deterministic
            expected_id = generate_deterministic_id(base_data[i]["record_id"])
            assert record1["__extract_id"] == expected_id

    def test_custom_id_strategy(self):
        """Test custom ID generation strategy."""

        # Define a custom ID generation strategy
        def custom_id_strategy(record):
            # Combine multiple fields
            name = record.get("name", "")
            code = record.get("code", "")
            return f"CUSTOM_{name}_{code}".upper()

        # Create test data
        data = [
            {"name": "Item A", "code": "001", "description": "First item"},
            {"name": "Item B", "code": "002", "description": "Second item"},
            {"name": "Item C", "code": "003", "description": "Third item"},
        ]

        # Create processor with custom ID strategy
        processor = Processor.with_custom_id_generation(custom_id_strategy)
        result = processor.process(data, entity_name="items")

        # Verify custom IDs were generated
        main_table = result.get_main_table()
        assert main_table[0]["__extract_id"] == "CUSTOM_ITEM A_001"
        assert main_table[1]["__extract_id"] == "CUSTOM_ITEM B_002"
        assert main_table[2]["__extract_id"] == "CUSTOM_ITEM C_003"

    def test_custom_id_strategy_edge_cases(self):
        """Test edge cases with custom ID generation strategy."""

        # Define a robust strategy that handles missing fields
        def robust_strategy(record):
            try:
                # Try to get fields, use defaults if missing
                id_val = record.get("id", "UNKNOWN")
                type_val = record.get("type", "UNKNOWN")
                return f"{type_val}_{id_val}"
            except Exception:
                # Fall back to UUID if anything goes wrong
                return str(uuid.uuid4())

        # Create test data with edge cases
        data = [
            {"id": "001", "type": "normal"},  # Normal case
            {"type": "missing_id"},  # Missing id
            {"id": "003"},  # Missing type
            {},  # Empty record
            {"id": None, "type": "null_id"},  # Null ID
            None,  # None record (handled with a UUID)
        ]

        # Create processor with robust strategy
        processor = Processor.with_custom_id_generation(robust_strategy)

        # Process should not raise exceptions
        result = processor.process(data, entity_name="edge_cases")

        # Verify records were processed
        main_table = result.get_main_table()

        # The main table should have 6 records - verify that
        assert len(main_table) == 6, f"Expected 6 records, got {len(main_table)}"

        # Check each record's ID based on the strategy
        assert main_table[0]["__extract_id"] == "normal_001"
        assert main_table[1]["__extract_id"] == "missing_id_UNKNOWN"
        assert main_table[2]["__extract_id"] == "UNKNOWN_003"
        assert main_table[3]["__extract_id"] == "UNKNOWN_UNKNOWN"
        assert (
            main_table[4]["__extract_id"] == "null_id_UNKNOWN"
        )  # None is treated as missing/UNKNOWN

        # The None record should have been assigned a UUID, which is non-deterministic
        # Just verify it's a valid UUID
        assert main_table[5]["__extract_id"] is not None
        # Check if it's a valid UUID string
        try:
            uuid.UUID(main_table[5]["__extract_id"])
            is_uuid = True
        except ValueError:
            is_uuid = False
        assert is_uuid, f"Expected UUID format, got {main_table[5]['__extract_id']}"
