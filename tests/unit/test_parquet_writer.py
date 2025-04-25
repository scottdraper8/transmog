"""
Tests for the Parquet writer component.

This file tests both basic functionality with mock objects (when PyArrow
is not available) and comprehensive functionality with real PyArrow integration.
"""

import os
import json
import tempfile
import shutil
import sys
from unittest import mock
from typing import Dict, List, Any

import pytest

# Import the writer module to access module-level variables
from transmog.io import parquet_writer
from transmog import Processor
from transmog.io.parquet_writer import ParquetWriter

# Try to import pyarrow for enhanced tests
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyarrow.lib import ArrowException

    PYARROW_AVAILABLE = True
except ImportError:
    pa = None
    pq = None
    ArrowException = Exception
    PYARROW_AVAILABLE = False


@pytest.fixture
def mock_pa_table(monkeypatch):
    """Fixture to mock PyArrow Table creation and writing."""
    # Create a mock Table class
    mock_table = mock.MagicMock()

    # Create mock modules
    mock_pa = mock.MagicMock()
    mock_pq = mock.MagicMock()

    # Set up the Table.from_pydict method
    mock_pa.Table = mock.MagicMock()
    mock_pa.Table.from_pydict = mock.MagicMock(return_value=mock_table)
    mock_pa.table = mock.MagicMock(return_value=mock_table)

    # Set up the write_table function
    mock_pq.write_table = mock.MagicMock()

    # Patch at module level
    monkeypatch.setattr(parquet_writer, "pa", mock_pa)
    monkeypatch.setattr(parquet_writer, "pq", mock_pq)

    # Also patch sys.modules to handle dynamic imports
    monkeypatch.setitem(sys.modules, "pyarrow", mock_pa)
    monkeypatch.setitem(sys.modules, "pyarrow.parquet", mock_pq)

    return mock_table


class TestParquetWriterBasic:
    """Basic tests for the Parquet Writer that work with or without PyArrow."""

    def test_initialization(self):
        """Test that the writer initializes correctly."""
        writer = ParquetWriter()
        assert writer is not None

    def test_write_single_table_mock(self, mock_pa_table):
        """Test writing a single table to a Parquet file with mocks."""
        # Skip if real PyArrow is available (we'll use real tests instead)
        if ParquetWriter.is_available() and parquet_writer.pa is not mock.MagicMock:
            pytest.skip("Using real PyArrow tests instead of mocks")

        # Setup test data
        test_data = [
            {"id": 1, "name": "Test1", "value": 100},
            {"id": 2, "name": "Test2", "value": 200},
        ]

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create writer
            writer = ParquetWriter()

            # Write to file
            file_path = writer.write_table(
                table_data=test_data,
                output_path=os.path.join(temp_dir, "test_table.parquet"),
                compression="snappy",
            )

            # Check the path is as expected
            assert file_path == os.path.join(temp_dir, "test_table.parquet")

            # Get the mocked modules through our fixture system
            pa = parquet_writer.pa
            pq = parquet_writer.pq

            # Verify calls to PyArrow
            pa.Table.from_pydict.assert_called_once()
            pq.write_table.assert_called_once()

    def test_write_all_tables_mock(self, mock_pa_table):
        """Test writing multiple tables to Parquet files with mocks."""
        # Skip if real PyArrow is available (we'll use real tests instead)
        if ParquetWriter.is_available() and parquet_writer.pa is not mock.MagicMock:
            pytest.skip("Using real PyArrow tests instead of mocks")

        # Setup test data
        main_table = [{"id": 1, "name": "Main", "type": "Parent"}]
        child_tables = {
            "child1": [{"id": 1, "parent_id": 1, "name": "Child1"}],
            "child2": [{"id": 2, "parent_id": 1, "name": "Child2"}],
        }

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create writer
            writer = ParquetWriter()

            # Write all tables
            result = writer.write_all_tables(
                main_table=main_table,
                child_tables=child_tables,
                base_path=temp_dir,
                entity_name="test_entity",
                compression="snappy",
            )

            # Check results
            assert "main" in result
            assert "child1" in result
            assert "child2" in result

            # Get the mocked modules through our fixture system
            pa = parquet_writer.pa
            pq = parquet_writer.pq

            # PyArrow Table.from_pydict should have been called three times (once per table)
            assert pa.Table.from_pydict.call_count == 3

            # pq.write_table should have been called three times (once per table)
            assert pq.write_table.call_count == 3

    def test_handle_empty_data_mock(self, mock_pa_table):
        """Test writing empty data with mocks."""
        # Skip if real PyArrow is available (we'll use real tests instead)
        if ParquetWriter.is_available() and parquet_writer.pa is not mock.MagicMock:
            pytest.skip("Using real PyArrow tests instead of mocks")

        # Setup empty test data
        test_data = []

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create writer
            writer = ParquetWriter()

            # Get the mocked modules through our fixture system
            pa = parquet_writer.pa
            pq = parquet_writer.pq

            # Reset mock call counts
            pa.Table.from_pydict.reset_mock()
            pq.write_table.reset_mock()

            # Write to file
            file_path = writer.write_table(
                table_data=test_data,
                output_path=os.path.join(temp_dir, "empty_table.parquet"),
            )

            # For empty data, we should still get a path
            assert file_path == os.path.join(temp_dir, "empty_table.parquet")

            # For empty data, we should use pa.table() instead of from_pydict
            assert not pa.Table.from_pydict.called
            assert pa.table.called
            assert pq.write_table.called

    def test_parquet_unavailable(self):
        """Test behavior when PyArrow is not available."""
        # Temporarily override is_available
        with mock.patch.object(ParquetWriter, "is_available", return_value=False):
            # Create writer
            writer = ParquetWriter()

            # Test that exception is raised when writing
            with pytest.raises(ImportError):
                writer.write_table(
                    table_data=[{"id": 1}], output_path="/tmp/error_table.parquet"
                )


# Run enhanced tests only when PyArrow is available
@pytest.mark.skipif(
    not ParquetWriter.is_available(), reason="PyArrow is required for these tests"
)
class TestParquetWriterEnhanced:
    """Enhanced tests for the ParquetWriter class with real PyArrow."""

    def setup_method(self):
        """Set up test data."""
        # Only run if PyArrow is available
        if not PYARROW_AVAILABLE:
            pytest.skip("PyArrow is required for these tests")

        # Create test data
        self.main_table = [
            {"id": "1", "name": "Record 1", "value": "100", "__extract_id": "main1"},
            {"id": "2", "name": "Record 2", "value": "200", "__extract_id": "main2"},
        ]

        self.child_tables = {
            "test_items": [
                {
                    "id": "101",
                    "parent_id": "1",
                    "name": "Item 1",
                    "__extract_id": "item1",
                    "__parent_extract_id": "main1",
                },
                {
                    "id": "102",
                    "parent_id": "2",
                    "name": "Item 2",
                    "__extract_id": "item2",
                    "__parent_extract_id": "main2",
                },
            ]
        }

        # Create a temporary directory for file outputs
        self.temp_dir = tempfile.mkdtemp()

        # Create writer
        self.writer = ParquetWriter()

    def teardown_method(self):
        """Clean up after tests."""
        # Remove the temporary directory
        shutil.rmtree(self.temp_dir)

    def test_basic_parquet_writing(self):
        """Test basic Parquet writing functionality."""
        # Write to Parquet
        output_paths = self.writer.write_all_tables(
            main_table=self.main_table,
            child_tables=self.child_tables,
            base_path=self.temp_dir,
            entity_name="test_entity",
        )

        # Verify output paths
        assert "main" in output_paths
        assert "test_items" in output_paths

        # Check files exist
        assert os.path.exists(output_paths["main"])
        assert os.path.exists(output_paths["test_items"])

        # Read back with pyarrow to verify data
        main_table = pq.read_table(output_paths["main"])
        child_table = pq.read_table(output_paths["test_items"])

        # Verify content
        assert main_table.num_rows == 2
        assert child_table.num_rows == 2
        assert "name" in main_table.column_names
        assert "id" in child_table.column_names
        assert main_table.column("id").to_pylist() == ["1", "2"]
        assert child_table.column("id").to_pylist() == ["101", "102"]

    def test_compression_options(self):
        """Test different compression options."""
        # Test different compressions
        compressions = ["snappy", "gzip", "brotli", None]

        for compression in compressions:
            file_path = self.writer.write_table(
                table_data=self.main_table,
                output_path=os.path.join(
                    self.temp_dir, f"main_{compression or 'none'}.parquet"
                ),
                compression=compression,
            )

            # Verify file was created
            assert os.path.exists(file_path)

            # Read the file back
            table = pq.read_table(file_path)

            # Verify content is correct regardless of compression
            assert table.num_rows == 2

    def test_empty_tables(self):
        """Test writing empty tables."""
        # Write empty main table
        empty_main_path = self.writer.write_table(
            table_data=[], output_path=os.path.join(self.temp_dir, "empty_main.parquet")
        )

        # Empty tables are written as empty PyArrow tables
        assert os.path.exists(empty_main_path)

        # Write empty child tables
        result = self.writer.write_all_tables(
            main_table=self.main_table,
            child_tables={"empty_child": []},
            base_path=self.temp_dir,
            entity_name="test_entity_empty",
        )

        # Verify main file was created and empty child file should also be created
        assert os.path.exists(result["main"])
        assert "empty_child" in result
        assert os.path.exists(result["empty_child"])

    def test_schema_preservation(self):
        """Test schema preservation with different data types."""
        # Create test data with various types
        complex_data = [
            {
                "int_val": 123,
                "float_val": 123.456,
                "str_val": "test",
                "bool_val": True,
                "null_val": None,
                "list_val_json": json.dumps([1, 2, 3]),
                "dict_val_json": json.dumps({"key": "value"}),
            }
        ]

        # Write to file
        file_path = self.writer.write_table(
            table_data=complex_data,
            output_path=os.path.join(self.temp_dir, "complex.parquet"),
        )

        # Verify file was created
        assert os.path.exists(file_path)

        # Read the file back
        table = pq.read_table(file_path)

        # Check data types - with the new implementation, data types are preserved
        # rather than everything being converted to strings
        assert table.column("int_val")[0].as_py() == 123
        assert table.column("float_val")[0].as_py() == 123.456
        assert table.column("str_val")[0].as_py() == "test"
        assert table.column("bool_val")[0].as_py() is True
        assert table.column("null_val")[0].as_py() is None

        # String-serialized values stay as strings
        assert table.column("list_val_json")[0].as_py() == "[1, 2, 3]"
        assert table.column("dict_val_json")[0].as_py() == '{"key": "value"}'

    def test_integration_with_processor(self):
        """Test integration with a Processor."""

        # Create a simple processor that uses ParquetWriter
        class TestProcessor(Processor):
            def process(
                self, input_data: Dict[str, Any], params: Dict[str, Any] = None
            ):
                writer = ParquetWriter()
                base_path = params.get("output_path", self.temp_dir)
                entity_name = params.get("entity_name", "test_processor")

                # Process and write data
                result = writer.write_all_tables(
                    main_table=input_data.get("main", []),
                    child_tables=input_data.get("children", {}),
                    base_path=base_path,
                    entity_name=entity_name,
                )

                return {"written_files": result}

        # Initialize processor with the test directory
        processor = TestProcessor()
        processor.temp_dir = self.temp_dir

        # Create test data
        input_data = {
            "main": self.main_table,
            "children": self.child_tables,
        }

        # Process data
        result = processor.process(
            input_data=input_data,
            params={"entity_name": "processor_test", "output_path": self.temp_dir},
        )

        # Verify results
        assert "written_files" in result
        assert "main" in result["written_files"]
        assert "test_items" in result["written_files"]

        # Verify files exist
        assert os.path.exists(result["written_files"]["main"])
        assert os.path.exists(result["written_files"]["test_items"])


# Run integration tests that require PyArrow
@pytest.mark.skipif(
    not ParquetWriter.is_available(), reason="PyArrow is required for these tests"
)
def test_parquet_table_from_empty_list():
    """Test creating a Parquet table from an empty list of records."""
    # Create a record with schema for empty table
    schema_record = {"id": "", "name": "", "value": ""}

    # Create empty list
    empty_data = []

    # This test previously expected an error, but our implementation handles empty lists gracefully
    # So instead we verify it works correctly
    writer = ParquetWriter()

    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # The writer should handle empty tables
        path = writer.write_table(
            table_data=empty_data, output_path=os.path.join(temp_dir, "empty.parquet")
        )

        # File should be created for empty data with an empty schema
        assert os.path.exists(path)

        # Read back the empty table
        table = pq.read_table(path)
        assert table.num_rows == 0


@pytest.mark.skipif(
    not ParquetWriter.is_available(), reason="PyArrow is required for these tests"
)
def test_parquet_error_handling():
    """Test error handling in ParquetWriter."""
    # Create writer
    writer = ParquetWriter()

    # Test with invalid path
    with tempfile.TemporaryDirectory() as temp_dir:
        # Test with invalid compression option instead of directory issues
        # since our implementation creates parent directories automatically
        with pytest.raises(ArrowException):
            writer.write_table(
                table_data=[{"id": "1"}],
                output_path=os.path.join(temp_dir, "test.parquet"),
                compression="invalid_compression",
            )

        # Ensure writing with valid compression works
        path = writer.write_table(
            table_data=[{"id": "1"}],
            output_path=os.path.join(temp_dir, "test.parquet"),
            compression="snappy",
        )
        assert os.path.exists(path)
