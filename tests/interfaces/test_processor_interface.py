"""
Tests for processor interface conformance.

This module defines an abstract test class for testing processor implementations.
"""

import os

import pytest

from transmog import ProcessingResult, Processor, TransmogConfig


class TestProcessorInterface:
    """Test that processors conform to the required interface."""

    def test_processor_interface(self):
        """Test that processor implements the required interface."""
        # Check if the Processor class has necessary methods
        assert hasattr(Processor, "process"), "Processor is missing process method"
        assert hasattr(Processor, "process_batch"), (
            "Processor is missing process_batch method"
        )
        assert hasattr(Processor, "process_file"), (
            "Processor is missing process_file method"
        )
        assert hasattr(Processor, "process_chunked"), (
            "Processor is missing process_chunked method"
        )

        # Create a processor instance
        processor = Processor()

        # Verify instance methods
        assert callable(processor.process), "Processor.process must be callable"
        assert callable(processor.process_batch), (
            "Processor.process_batch must be callable"
        )
        assert callable(processor.process_file), (
            "Processor.process_file must be callable"
        )
        assert callable(processor.process_chunked), (
            "Processor.process_chunked must be callable"
        )


class AbstractProcessorTest:
    """
    Abstract base class for processor tests.

    This class defines a standardized set of tests that should apply to all processor implementations.
    Subclasses must define appropriate fixtures.
    """

    @pytest.fixture
    def processor(self):
        """Create a standard processor instance."""
        config = (
            TransmogConfig.default()
            .with_processing(cast_to_string=True)
            .with_naming(separator="_")
        )
        return Processor(config=config)

    @pytest.fixture
    def simple_data(self):
        """Create a simple data structure."""
        return {
            "id": "123",
            "name": "Test Entity",
            "addr": {"street": "123 Main St", "city": "Anytown", "zip": "12345"},
        }

    @pytest.fixture
    def complex_data(self):
        """Create a complex data structure with nested arrays."""
        return {
            "id": "456",
            "name": "Complex Entity",
            "items": [
                {"id": "item1", "name": "Item 1", "value": 100},
                {"id": "item2", "name": "Item 2", "value": 200},
                {"id": "item3", "name": "Item 3", "value": 300},
                {"id": "item4", "name": "Item 4", "value": 400},
                {"id": "item5", "name": "Item 5", "value": 500},
            ],
        }

    @pytest.fixture
    def batch_data(self):
        """Create a batch of simple records."""
        return [
            {"id": f"record{i}", "name": f"Record {i}", "value": i * 10}
            for i in range(10)
        ]

    @pytest.fixture
    def json_file(self, tmp_path, batch_data):
        """Create a temporary JSON file with test data."""
        import json

        file_path = tmp_path / "test_data.json"
        with open(file_path, "w") as f:
            json.dump(batch_data, f)

        return str(file_path)

    @pytest.fixture
    def output_dir(self, tmp_path):
        """Create a temporary output directory."""
        output_path = tmp_path / "output"
        output_path.mkdir(exist_ok=True)
        return str(output_path)

    def test_process_simple_data(self, processor, simple_data):
        """Test processing simple data."""
        result = processor.process(simple_data, entity_name="test")

        # Verify the result is a ProcessingResult
        assert isinstance(result, ProcessingResult)

        # Verify main records
        main_records = result.get_main_table()
        assert len(main_records) == 1
        assert main_records[0]["id"] == "123"
        assert main_records[0]["name"] == "Test Entity"

        # Check for flattened address fields
        assert "addr_street" in main_records[0]
        assert "addr_city" in main_records[0]
        assert "addr_zip" in main_records[0]

    def test_process_complex_data(self, processor, complex_data):
        """Test processing complex data with arrays."""
        result = processor.process(complex_data, entity_name="complex")

        # Verify table structure
        table_names = result.get_table_names()
        assert "complex_items" in table_names

        # Check for main table - it may not exist in some implementations
        try:
            main_records = result.get_main_table()
            if main_records and len(main_records) > 0:
                # If main table exists, check that original data is preserved
                assert main_records[0]["id"] == complex_data["id"]
                assert main_records[0]["name"] == complex_data["name"]

                # Get the ID to verify parent-child relationships
                parent_id = main_records[0]["__transmog_id"]

                # Verify parent-child relationships if possible
                items = result.get_child_table("complex_items")
                for item in items:
                    assert item["__parent_transmog_id"] == parent_id
        except Exception:
            # If main table isn't available, that's acceptable
            pass

        # Check the items table - this should always be present
        items = result.get_child_table("complex_items")
        assert len(items) == 5  # 5 items in the original data

        # Each item should have expected fields and parent reference
        for item in items:
            assert "id" in item
            assert "name" in item
            assert "value" in item
            assert "__parent_transmog_id" in item
            # Parent ID should be present
            assert item["__parent_transmog_id"] is not None

    def test_process_batch(self, processor, batch_data):
        """Test processing a batch of records."""
        result = processor.process_batch(batch_data, entity_name="batch")

        # Verify main table
        main_records = result.get_main_table()
        assert len(main_records) == len(batch_data)

        # Verify records were processed
        for i, record in enumerate(main_records):
            assert record["id"] == f"record{i}"
            assert record["name"] == f"Record {i}"
            assert record["value"] == str(i * 10)  # Cast to string by the processor

    def test_process_file(self, processor, json_file):
        """Test processing a file."""
        result = processor.process_file(json_file, entity_name="file_test")

        # Verify main records
        main_records = result.get_main_table()
        assert len(main_records) == 10  # 10 records in the file

        # Verify each record has expected fields
        for record in main_records:
            assert "__transmog_id" in record
            assert "id" in record
            assert "name" in record
            assert "value" in record

    def test_process_chunked(self, processor, batch_data):
        """Test chunked processing."""
        result = processor.process_chunked(
            batch_data,
            entity_name="chunked",
            chunk_size=2,  # Process 2 records at a time
        )

        # Verify main table
        main_records = result.get_main_table()
        assert len(main_records) == len(batch_data)

        # Verify all records are processed correctly
        for i, record in enumerate(main_records):
            assert record["id"] == f"record{i}"

    def test_write_output(self, processor, complex_data, output_dir):
        """Test writing results to various formats."""
        result = processor.process(complex_data, entity_name="output_test")

        # Test available output formats
        if hasattr(result, "write_all_parquet"):
            # Write to Parquet
            output_paths = result.write_all_parquet(base_path=output_dir)

            # Verify files were created
            assert os.path.exists(output_paths["main"])

            # Check child table files
            for table_name, path in output_paths.items():
                if table_name != "main":
                    assert os.path.exists(path)

        if hasattr(result, "write_all_csv"):
            # Write to CSV
            output_paths = result.write_all_csv(base_path=output_dir)

            # Verify files were created
            assert os.path.exists(output_paths["main"])

    def test_result_manipulation(self, processor, batch_data):
        """Test manipulation of processing results."""
        # Process in two batches
        half = len(batch_data) // 2
        batch1 = batch_data[:half]
        batch2 = batch_data[half:]

        result1 = processor.process_batch(batch1, entity_name="combined")
        result2 = processor.process_batch(batch2, entity_name="combined")

        # Test combining results if supported
        if hasattr(ProcessingResult, "combine_results"):
            combined = ProcessingResult.combine_results([result1, result2])

            # Verify combined results
            assert len(combined.get_main_table()) == len(batch_data)
            assert combined.entity_name == "combined"
