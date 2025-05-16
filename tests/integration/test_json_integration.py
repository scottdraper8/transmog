"""
Integration tests for JSON processing functionality.

These tests verify the end-to-end functionality of JSON processing,
including streaming JSON data into different output formats.
"""

import json
import os
import tempfile

import pytest

from transmog import Processor
from transmog.error import ProcessingError
from transmog.io.writers.json import JsonStreamingWriter


class TestJsonIntegration:
    """Integration tests for JSON processing."""

    def create_test_data(self):
        """Create test data for JSON processing."""
        # Setup test data
        return [
            {"id": 1, "name": "John Doe", "age": 30, "active": True, "score": 95.5},
            {"id": 2, "name": "Jane Smith", "age": 25, "active": True, "score": 98.3},
            {"id": 3, "name": "Bob Johnson", "age": 45, "active": False, "score": 82.1},
            {"id": 4, "name": "Alice Brown", "age": 35, "active": True, "score": 91.7},
            {
                "id": 5,
                "name": "Charlie Davis",
                "age": 50,
                "active": False,
                "score": 75.0,
            },
        ]

    def create_test_nested_data(self):
        """Create test data with nested structures for JSON processing."""
        return [
            {
                "id": 1,
                "name": "John Doe",
                "age": 30,
                "address": {
                    "street": "123 Main St",
                    "city": "Anytown",
                    "zipcode": "12345",
                },
                "contact": {"email": "john@example.com", "phone": "555-1234"},
                "scores": {"math": 95, "science": 88, "history": 91},
            },
            {
                "id": 2,
                "name": "Jane Smith",
                "age": 28,
                "address": {
                    "street": "456 Oak Ave",
                    "city": "Somecity",
                    "zipcode": "67890",
                },
                "contact": {"email": "jane@example.com", "phone": "555-5678"},
                "scores": {"math": 92, "science": 96, "history": 89},
            },
            {
                "id": 3,
                "name": "Bob Johnson",
                "age": 35,
                "address": {
                    "street": "789 Pine Rd",
                    "city": "Othertown",
                    "zipcode": "13579",
                },
                "contact": {"email": "bob@example.com", "phone": "555-9012"},
                "scores": {"math": 78, "science": 82, "history": 85},
            },
        ]

    def test_basic_json_streaming(self):
        """Test basic JSON streaming functionality."""
        data = self.create_test_data()

        # Create temporary directory for output
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a JSON streaming writer
            writer = JsonStreamingWriter(destination=temp_dir, entity_name="basic_test")

            # Write data in batches to simulate streaming
            writer.initialize_main_table()
            writer.write_main_records(data[:2])  # First batch
            writer.write_main_records(data[2:])  # Second batch
            writer.finalize()
            writer.close()

            # Verify output file exists
            output_file = os.path.join(temp_dir, "basic_test.json")
            assert os.path.exists(output_file)

            # Verify content
            with open(output_file) as f:
                result = json.load(f)
                assert result == data

    def test_json_streaming_with_child_tables(self):
        """Test JSON streaming with child tables."""
        main_data = self.create_test_data()
        child_data = [
            {"child_id": 101, "parent_id": 1, "name": "Child 1", "value": 10},
            {"child_id": 102, "parent_id": 1, "name": "Child 2", "value": 20},
            {"child_id": 103, "parent_id": 2, "name": "Child 3", "value": 30},
            {"child_id": 104, "parent_id": 3, "name": "Child 4", "value": 40},
        ]

        # Create temporary directory for output
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a JSON streaming writer
            writer = JsonStreamingWriter(destination=temp_dir, entity_name="parent")

            # Initialize tables
            writer.initialize_main_table()
            writer.initialize_child_table("children")

            # Write main data
            writer.write_main_records(main_data)

            # Write child data in batches
            writer.write_child_records("children", child_data[:2])
            writer.write_child_records("children", child_data[2:])

            writer.finalize()
            writer.close()

            # Verify files exist
            main_file = os.path.join(temp_dir, "parent.json")
            child_file = os.path.join(temp_dir, "children.json")

            assert os.path.exists(main_file)
            assert os.path.exists(child_file)

            # Verify content
            with open(main_file) as f:
                result = json.load(f)
                assert result == main_data

            with open(child_file) as f:
                result = json.load(f)
                assert result == child_data

    def test_json_streaming_with_processor(self):
        """Test JSON streaming with the Processor class."""
        test_data = self.create_test_data()

        # Create temporary input file
        with tempfile.NamedTemporaryFile(
            mode="w+", suffix=".json", delete=False
        ) as input_file:
            json.dump(test_data, input_file)
            input_path = input_file.name

        # Create temporary directory for output
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                # Initialize processor
                processor = Processor()

                # Process the file first
                result = processor.process_file(
                    file_path=input_path,
                    entity_name="processor_test",
                )

                # Then write the result to JSON files in the output directory
                output_paths = result.write_all_json(base_path=temp_dir)

                # Verify output was created
                assert len(output_paths) > 0
                assert os.path.exists(output_paths["main"])

            finally:
                # Clean up
                os.unlink(input_path)

    def test_error_handling(self):
        """Test error handling during JSON streaming."""
        # Test with non-existent file
        with tempfile.TemporaryDirectory() as temp_dir:
            non_existent_file = os.path.join(temp_dir, "does_not_exist.json")

            # Test with a file that doesn't exist
            # The error is wrapped in a ProcessingError by the error_context decorator
            with pytest.raises(ProcessingError) as exc_info:
                processor = Processor()
                processor.process_file(
                    file_path=non_existent_file, entity_name="error_test"
                )
            # Verify that the underlying cause was a file error
            assert "File not found" in str(exc_info.value)

            # Test with invalid JSON content
            invalid_json_path = os.path.join(temp_dir, "invalid.json")
            with open(invalid_json_path, "w") as f:
                f.write("{invalid json")

            with pytest.raises(ProcessingError) as exc_info:
                processor = Processor()
                processor.process_file(
                    file_path=invalid_json_path, entity_name="error_test"
                )
            # Verify that the error indicates a parsing problem
            assert "json" in str(exc_info.value).lower()

    def test_large_batch_processing(self):
        """Test processing large batches of data."""
        # Create a large dataset (1000 records)
        large_data = [
            {"id": i, "name": f"Person {i}", "value": i * 10} for i in range(1, 1001)
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a JSON streaming writer
            writer = JsonStreamingWriter(destination=temp_dir, entity_name="large_test")

            # Write data in multiple batches
            writer.initialize_main_table()

            # Write in batches of 100
            batch_size = 100
            for i in range(0, len(large_data), batch_size):
                batch = large_data[i : i + batch_size]
                writer.write_main_records(batch)

            writer.finalize()
            writer.close()

            # Verify output file exists
            output_file = os.path.join(temp_dir, "large_test.json")
            assert os.path.exists(output_file)

            # Verify content
            with open(output_file) as f:
                result = json.load(f)
                assert len(result) == len(large_data)

                # Verify first and last records
                assert result[0]["id"] == 1
                assert result[-1]["id"] == 1000
