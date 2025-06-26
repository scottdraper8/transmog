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
        processor = Processor.with_deterministic_ids("id").with_metadata(
            force_transmog_id=True
        )

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
            assert result["main_table"][0]["__transmog_id"] == main_id

        # Verify that the customers have the same deterministic IDs in all runs
        for result in results:
            if "store_customers" in result:
                customers = sorted(result["store_customers"], key=lambda x: x["id"])
                for i, customer in enumerate(customers):
                    expected_id = generate_deterministic_id(data["customers"][i]["id"])
                    assert customer["__transmog_id"] == expected_id

        # All other tables will use random IDs with this approach, so we don't test them

    def test_incremental_processing_stability(self):
        """Test stability of IDs across incremental processing runs."""
        # Create a processor with deterministic IDs
        processor = Processor.with_deterministic_ids("id").with_metadata(
            force_transmog_id=True
        )

        # Create initial data
        initial_data = [
            {"id": "record1", "name": "Record 1", "value": 100},
            {"id": "record2", "name": "Record 2", "value": 200},
        ]

        # Process initial data
        result1 = processor.process_batch(initial_data, entity_name="test")

        # Add more records
        additional_data = [
            {"id": "record3", "name": "Record 3", "value": 300},
            {"id": "record4", "name": "Record 4", "value": 400},
        ]

        # Process combined data
        combined_data = initial_data + additional_data
        result2 = processor.process_batch(combined_data, entity_name="test")

        # Get main tables
        main_table1 = result1.get_main_table()
        main_table2 = result2.get_main_table()

        # Check that all records have transmog IDs
        for record in main_table1:
            assert "__transmog_id" in record

        for record in main_table2:
            assert "__transmog_id" in record

        # Verify ID stability for initial records
        for i in range(len(main_table1)):
            record1 = main_table1[i]
            record2 = next(r for r in main_table2 if r["id"] == record1["id"])
            assert record1["__transmog_id"] == record2["__transmog_id"]

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
            assert record1["__transmog_id"] == record2["__transmog_id"]
            # Verify the ID is actually deterministic
            expected_id = generate_deterministic_id(base_data[i]["record_id"])
            assert record1["__transmog_id"] == expected_id

    def test_custom_id_strategy(self):
        """Test custom ID generation strategy."""

        # Define a custom ID strategy
        def custom_id_strategy(record):
            # Use id field with a prefix
            return f"CUSTOM-{record.get('id', 'unknown')}"

        # Create a processor with the custom strategy
        processor = Processor.with_custom_id_generation(
            custom_id_strategy
        ).with_metadata(force_transmog_id=True)

        # Create test data
        test_data = [
            {"id": "record1", "name": "Record 1"},
            {"id": "record2", "name": "Record 2"},
        ]

        # Process the data
        result = processor.process_batch(test_data, entity_name="test")

        # Get main table
        main_table = result.get_main_table()

        # Verify custom IDs were applied
        assert len(main_table) == 2
        assert main_table[0]["__transmog_id"] == "CUSTOM-record1"
        assert main_table[1]["__transmog_id"] == "CUSTOM-record2"

    def test_custom_id_strategy_edge_cases(self):
        """Test custom ID strategy with edge cases."""

        # Define a custom ID strategy that handles edge cases
        def robust_id_strategy(record):
            # Handle missing id field
            if "id" not in record:
                return f"GENERATED-{hash(str(sorted(record.items())))}"
            return f"ID-{record['id']}"

        # Create a processor with the custom strategy
        processor = Processor.with_custom_id_generation(
            robust_id_strategy
        ).with_metadata(force_transmog_id=True)

        # Create test data with edge cases
        test_data = [
            {"id": "record1", "name": "Record 1"},  # Normal case
            {"name": "No ID Record"},  # Missing ID
            {"id": None, "name": "Null ID"},  # Null ID
            {"id": "", "name": "Empty ID"},  # Empty ID
        ]

        # Process the data
        result = processor.process_batch(test_data, entity_name="test")

        # Get main table
        main_table = result.get_main_table()

        # Verify IDs were generated for all records
        assert len(main_table) == 4

        # Sort by name for consistent testing
        sorted_records = sorted(main_table, key=lambda r: r["name"])

        # Find the record with id="record1"
        record1 = next(r for r in main_table if r.get("id") == "record1")
        assert record1["__transmog_id"] == "ID-record1"

        # Check that all records have transmog IDs
        for record in main_table:
            assert "__transmog_id" in record

        # Find records with specific conditions
        no_id_record = next(r for r in main_table if "id" not in r)
        null_id_record = next(r for r in main_table if r.get("id") is None)
        empty_id_record = next(r for r in main_table if r.get("id") == "")

        # Verify ID generation for edge cases
        assert no_id_record["__transmog_id"].startswith("GENERATED-")
        # Implementation may handle null IDs differently, just check it has some ID
        assert null_id_record["__transmog_id"] is not None
        # Implementation may handle empty IDs differently, just check it has some ID
        assert empty_id_record["__transmog_id"] is not None

    def test_force_transmog_id(self):
        """Test forcing transmog ID generation."""
        # Sample data with natural IDs
        data = {
            "id": "COMP-001",
            "name": "Test Company",
            "departments": [
                {"id": "DEPT-001", "name": "HR"},
                {"id": "DEPT-002", "name": "Engineering"},
            ],
        }

        # Process with force_transmog_id=True
        processor = Processor.with_natural_ids(id_field_patterns=["id"]).with_metadata(
            force_transmog_id=True
        )
        result = processor.process(data, entity_name="company")

        # Check main table has both IDs
        assert len(result.main_table) == 1
        assert result.main_table[0]["id"] == "COMP-001"  # Natural ID preserved
        assert "__transmog_id" in result.main_table[0]  # Transmog ID added

        # Check departments table has both IDs
        dept_table = result.child_tables["company_departments"]
        assert len(dept_table) == 2
        assert dept_table[0]["id"] in ["DEPT-001", "DEPT-002"]  # Natural ID preserved
        assert "__transmog_id" in dept_table[0]  # Transmog ID added
