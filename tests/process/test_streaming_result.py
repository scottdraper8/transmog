"""
Tests for streaming functionality in ProcessingResult.

This module tests the streaming capabilities of the ProcessingResult class.
"""

import os
import tempfile

import pytest

# Skip tests if PyArrow is not available
pytest.importorskip("pyarrow")
import pyarrow.parquet as pq

from transmog import Processor, TransmogConfig


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
                assert "__parent_extract_id" in child_table.column_names

    def test_stream_to_parquet_large_dataset(self, processor):
        """Test streaming to Parquet with a larger synthetic dataset."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Generate a larger dataset (200 records)
            large_data = []
            for i in range(200):
                large_data.append(
                    {
                        "id": i,
                        "name": f"Record {i}",
                        "value": i * 10,
                        "nested": {
                            "field1": f"nested_{i}",
                            "field2": i % 10,
                        },
                        "items": [
                            {"item_id": j, "name": f"Item {i}-{j}", "value": j}
                            for j in range(5)  # 5 child items per record
                        ],
                    }
                )

            # Process the data
            result = processor.process_batch(large_data, entity_name="large_dataset")

            # Stream to Parquet with small row group size to force multiple row groups
            output_files = result.stream_to_parquet(
                base_path=temp_dir,
                compression="snappy",
                row_group_size=50,  # Small enough to create multiple row groups
            )

            # Verify the main table
            main_path = output_files["main"]
            main_parquet = pq.ParquetFile(main_path)

            # Should have at least one row group
            assert main_parquet.num_row_groups >= 1

            # Should have correct number of rows
            main_table = main_parquet.read()
            assert len(main_table) == 200

            # Verify the child table (items)
            items_table_name = next(
                name
                for name in output_files.keys()
                if name != "main" and "items" in name
            )
            items_path = output_files[items_table_name]
            items_parquet = pq.ParquetFile(items_path)

            # Should have at least one row group
            assert items_parquet.num_row_groups >= 1

            # Should have 1000 rows (200 records × 5 items)
            items_table = items_parquet.read()
            assert len(items_table) == 1000

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
