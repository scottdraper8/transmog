"""
Integration tests for the Transmog package.

These tests verify the end-to-end functionality of the package with
realistic scenarios.
"""

import os
import json
import tempfile
import pytest
from src.transmog import Processor

# Check if pyarrow is available
try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


class TestEndToEndScenarios:
    """End-to-end integration tests for Transmog."""

    @pytest.mark.skipif(
        not PYARROW_AVAILABLE, reason="PyArrow required for Parquet output"
    )
    def test_process_and_write_workflow(self, complex_data, test_output_dir):
        """Test the complete workflow from processing to writing files."""
        # Initialize processor
        processor = Processor(cast_to_string=True, visit_arrays=True, skip_null=True)

        # Process data
        result = processor.process(complex_data, entity_name="integration_test")

        # Write to parquet
        output_paths = result.write_all_parquet(base_path=test_output_dir)

        # Verify file creation
        assert len(output_paths) >= 1  # At least main table should exist

        # Verify files exist
        for path in output_paths.values():
            assert os.path.exists(path)

        # If PyArrow is available, also verify content
        if PYARROW_AVAILABLE:
            main_table = pq.read_table(output_paths["main"])
            assert main_table.num_rows > 0
            assert "__extract_id" in main_table.column_names

    def test_file_processing_workflow(self, test_output_dir):
        """Test processing files in different formats."""
        # Create test files
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as jsonl_file:
            # Write multiple JSON objects, one per line
            for i in range(3):
                data = {
                    "id": f"rec{i}",
                    "name": f"Record {i}",
                    "details": {"type": "test", "active": True, "score": i * 10},
                    "tags": ["test", "integration", f"tag{i}"],
                }
                jsonl_file.write((json.dumps(data) + "\n").encode())
            jsonl_path = jsonl_file.name

        try:
            # Initialize processor
            processor = Processor(cast_to_string=True)

            # Process JSONL file
            result = processor.process_file(jsonl_path, entity_name="file_integration")

            # Verify results
            main_table = result.get_main_table()
            assert len(main_table) == 3

            # Verify each record has the expected fields
            for record in main_table:
                assert "id" in record
                assert "name" in record
                assert "tags" in record  # As a string, not expanded
        finally:
            # Clean up
            os.unlink(jsonl_path)

    def test_large_batch_processing(self):
        """Test processing a larger batch of data."""
        # Create a larger batch of records
        large_batch = []
        for i in range(50):
            large_batch.append(
                {
                    "id": f"{i}",
                    "name": f"Record {i}",
                    "active": i % 2 == 0,
                    "metadata": {
                        "created": "2023-01-01",
                        "modified": "2023-01-02",
                        "version": i % 5,
                    },
                    "items": [{"item_id": f"item{j}", "value": j} for j in range(3)],
                }
            )

        # Process in chunks
        processor = Processor(cast_to_string=True)
        result = processor.process_chunked(
            large_batch, entity_name="large_batch", chunk_size=10
        )

        # Verify results
        main_table = result.get_main_table()
        assert len(main_table) == 50

        # Verify field values
        assert all("__extract_id" in record for record in main_table)
        assert all(
            "items" in record for record in main_table
        )  # As strings, not expanded

    def test_memory_efficiency(self):
        """Test memory-efficient processing with optimizations."""
        # Create test data
        test_data = []
        for i in range(25):
            record = {
                "id": f"{i}",
                "deep": {"nested": {"structure": {"with": {"many": {"levels": i}}}}},
                "array_field": [{"name": f"item{j}", "value": j} for j in range(5)],
            }
            test_data.append(record)

        # Use memory-optimized processor with explicitly defined abbreviation settings
        processor = Processor(
            cast_to_string=True,
            optimize_for_memory=True,
            path_parts_optimization=True,
            # Disable abbreviation for test consistency
            abbreviate_field_names=False,
            # Set explicit component lengths to prevent test failures when defaults change
            max_field_component_length=10,
            max_table_component_length=10,
        )

        # Process with small chunks
        result = processor.process_chunked(
            test_data, entity_name="memory_test", chunk_size=5
        )

        # Verify results
        main_table = result.get_main_table()
        assert len(main_table) == 25

        # Verify deep nesting was flattened - check that some deeply nested field exists
        assert any("deep_nested_structure" in key for key in main_table[0].keys())

        # Check child tables
        table_names = result.get_table_names()

        # Get the array table name
        array_table_name = next(name for name in table_names if "array" in name.lower())

        # Verify child table has data
        array_data = result.get_child_table(array_table_name)
        assert len(array_data) > 0  # Has some data
