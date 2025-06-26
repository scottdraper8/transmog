"""
Tests for streaming functionality in ProcessingResult.

This module tests the streaming capabilities of the ProcessingResult class.
"""

import os
import tempfile

import pytest

from transmog import Processor, TransmogConfig
from transmog.process.result import ProcessingResult

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None


@pytest.fixture
def sample_data():
    """Generate sample nested data for testing."""
    return {
        "id": 1,
        "name": "Test Entity",
        "attributes": {
            "color": "red",
            "size": "large",
            "enabled": True,
        },
        "items": [
            {"id": 101, "name": "Item 1", "value": 10.5},
            {"id": 102, "name": "Item 2", "value": 20.5},
            {"id": 103, "name": "Item 3", "value": 30.5},
        ],
        "tags": ["tag1", "tag2", "tag3"],
    }


@pytest.fixture
def processor():
    """Create a processor for testing.

    Note: This processor is configured with cast_to_string=False, but type conversion
    might still happen during PyArrow table creation depending on the implementation.
    The tests accommodate both integer and string IDs accordingly.
    """
    config = TransmogConfig.default().with_processing(cast_to_string=False)
    return Processor(config=config)


@pytest.fixture
def processing_result(processor, sample_data):
    """Create a processing result with sample data."""
    return processor.process(sample_data, entity_name="test_entity")


class TestStreamingResult:
    """Tests for streaming functionality in the ProcessingResult class."""

    def test_stream_to_parquet(self, processing_result):
        """Test streaming to Parquet files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Stream the result to Parquet
            output_files = processing_result.stream_to_parquet(
                base_path=temp_dir, compression="snappy", row_group_size=100
            )

            # Verify output files were created
            assert "main" in output_files

            # Check that the main file exists and can be read
            main_path = output_files["main"]
            assert os.path.exists(main_path)
            main_table = pq.read_table(main_path)

            # Basic validations on the main table
            assert len(main_table) == 1  # One main record
            assert "id" in main_table.column_names
            assert "name" in main_table.column_names

            # Check for at least one child table
            child_tables = {k: v for k, v in output_files.items() if k != "main"}
            assert len(child_tables) > 0

            # Verify each child table exists and can be read
            for _table_name, file_path in child_tables.items():
                assert os.path.exists(file_path)
                child_table = pq.read_table(file_path)
                assert len(child_table) > 0
                assert "__parent_transmog_id" in child_table.column_names

    def test_stream_to_parquet_large_dataset(self):
        """Test streaming a large dataset to Parquet."""
        try:
            import pyarrow
        except ImportError:
            pytest.skip("pyarrow not installed, skipping Parquet test")

        # Create a large dataset
        large_dataset = [
            {"id": f"record{i}", "value": i, "name": f"Record {i}"} for i in range(1000)
        ]

        # Create a streaming result
        result = ProcessingResult(
            main_table=large_dataset[:10],  # Only store first 10 records in memory
            child_tables={},
            entity_name="large_test",
        )

        # Create a temporary directory for output
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = os.path.join(tmp_dir, "large_test.parquet")
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, "main.parquet")

            # Stream to Parquet - note that we don't need a record_generator in the updated API
            result.stream_to_parquet(
                base_path=output_dir,
                compression="snappy",
            )

            # Verify file was created
            assert os.path.exists(os.path.join(output_dir, "main.parquet"))

    def test_stream_to_parquet_schema_evolution(self, processor):
        """Test that schema evolution is handled correctly when streaming to Parquet."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create data with evolving schema
            data = []

            # First batch has field1
            for i in range(50):
                data.append({"id": i, "field1": f"value_{i}"})

            # Second batch has field2
            for i in range(50, 100):
                data.append({"id": i, "field2": f"value_{i}"})

            # Process the data
            result = processor.process_batch(data, entity_name="evolving_schema")

            # Stream to Parquet
            output_files = result.stream_to_parquet(
                base_path=temp_dir,
                compression="snappy",
                row_group_size=25,  # Force multiple row groups
            )

            # Check the resulting file
            main_path = output_files["main"]
            table = pq.read_table(main_path)

            # Should have all fields from both batches
            assert "id" in table.column_names
            assert "field1" in table.column_names
            assert "field2" in table.column_names

            # First 50 records should have field1 but not field2
            for i in range(50):
                # Convert to int if id is stored as string
                id_value = table["id"][i].as_py()
                if isinstance(id_value, str):
                    id_value = int(id_value)
                assert id_value == i
                assert table["field1"][i].as_py() is not None
                assert table["field2"][i].as_py() is None

            # Next 50 records should have field2 but not field1
            for i in range(50, 100):
                # Convert to int if id is stored as string
                id_value = table["id"][i].as_py()
                if isinstance(id_value, str):
                    id_value = int(id_value)
                assert id_value == i
                assert table["field1"][i].as_py() is None
                assert table["field2"][i].as_py() is not None
