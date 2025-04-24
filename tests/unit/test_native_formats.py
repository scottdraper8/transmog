"""
Tests for native format output methods in ProcessingResult.

This module tests the native format output methods added to ProcessingResult:
- to_dict()
- to_json_objects()
- to_pyarrow_tables()
- to_parquet_bytes()
- to_csv_bytes()
- to_json_bytes()
"""

import pytest
from typing import Dict, List, Any
import io

from src.transmog import Processor
from src.transmog.core.processing_result import ProcessingResult

# Sample test data
SAMPLE_DATA = {
    "id": "rec123",
    "name": "Test Record",
    "details": {"created": "2023-01-01", "active": True, "score": 95.5},
    "items": [{"item_id": "i1", "value": 10}, {"item_id": "i2", "value": 20}],
}


@pytest.fixture
def processing_result() -> ProcessingResult:
    """Create a ProcessingResult with test data."""
    # Process sample data
    processor = Processor(visit_arrays=True)
    return processor.process(SAMPLE_DATA, entity_name="test")


class TestNativeFormatMethods:
    """Test native format output methods in ProcessingResult."""

    def test_to_dict(self, processing_result: ProcessingResult):
        """Test to_dict() method."""
        # Get dictionary representation
        result_dict = processing_result.to_dict()

        # Basic validation
        assert isinstance(result_dict, dict)
        assert "main" in result_dict
        assert len(result_dict) > 1  # Should have main and at least one child table

        # Verify main table structure
        main_table = result_dict["main"]
        assert isinstance(main_table, list)
        assert len(main_table) == 1
        assert "id" in main_table[0]
        assert "name" in main_table[0]
        assert "__extract_id" in main_table[0]  # Should have metadata fields

        # Verify child tables
        items_table = None
        for table_name, table_data in result_dict.items():
            if table_name != "main" and "items" in table_name:
                items_table = table_data
                break

        assert items_table is not None
        assert len(items_table) == 2  # Should have two items

    def test_to_json_objects(self, processing_result: ProcessingResult):
        """Test to_json_objects() method."""
        # Get JSON-serializable objects
        json_obj = processing_result.to_json_objects()

        # Basic validation
        assert isinstance(json_obj, dict)
        assert "main" in json_obj

        # Verify all values are JSON serializable
        import json

        # This should not raise any exception
        json_str = json.dumps(json_obj)
        assert isinstance(json_str, str)

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow", reason="PyArrow not available"),
        reason="PyArrow required for this test",
    )
    def test_to_pyarrow_tables(self, processing_result: ProcessingResult):
        """Test to_pyarrow_tables() method."""
        import pyarrow as pa

        # Get PyArrow tables
        tables = processing_result.to_pyarrow_tables()

        # Basic validation
        assert isinstance(tables, dict)
        assert "main" in tables
        assert isinstance(tables["main"], pa.Table)

        # Verify main table structure
        main_table = tables["main"]
        assert main_table.num_rows > 0
        assert "__extract_id" in main_table.column_names
        assert "id" in main_table.column_names
        assert "name" in main_table.column_names

        # Verify child tables
        items_table = None
        for table_name, table in tables.items():
            if table_name != "main" and "items" in table_name:
                items_table = table
                break

        assert items_table is not None
        assert items_table.num_rows == 2  # Should have two items
        assert "item_id" in items_table.column_names
        assert "value" in items_table.column_names

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow", reason="PyArrow not available"),
        reason="PyArrow required for this test",
    )
    def test_to_parquet_bytes(self, processing_result: ProcessingResult):
        """Test to_parquet_bytes() method."""
        import pyarrow.parquet as pq

        # Get Parquet bytes
        parquet_bytes = processing_result.to_parquet_bytes()

        # Basic validation
        assert isinstance(parquet_bytes, dict)
        assert "main" in parquet_bytes
        assert isinstance(parquet_bytes["main"], bytes)
        assert len(parquet_bytes["main"]) > 0

        # Verify bytes can be read as Parquet
        main_buffer = io.BytesIO(parquet_bytes["main"])
        table = pq.read_table(main_buffer)
        assert table.num_rows > 0
        assert "__extract_id" in table.column_names

        # Verify child tables
        for table_name, data in parquet_bytes.items():
            if table_name != "main":
                buffer = io.BytesIO(data)
                child_table = pq.read_table(buffer)
                assert child_table.num_rows > 0
                assert "__extract_id" in child_table.column_names
                assert "__parent_extract_id" in child_table.column_names

    def test_to_csv_bytes(self, processing_result: ProcessingResult):
        """Test to_csv_bytes() method."""
        # Get CSV bytes
        csv_bytes = processing_result.to_csv_bytes()

        # Basic validation
        assert isinstance(csv_bytes, dict)
        assert "main" in csv_bytes
        assert isinstance(csv_bytes["main"], bytes)
        assert len(csv_bytes["main"]) > 0

        # Verify bytes look like CSV format (headers + data)
        main_csv = csv_bytes["main"].decode("utf-8")
        assert "id" in main_csv
        assert "name" in main_csv
        assert "__extract_id" in main_csv
        assert "\n" in main_csv  # Should have at least one newline

        # Verify child tables
        for table_name, data in csv_bytes.items():
            if table_name != "main":
                child_csv = data.decode("utf-8")
                assert "__extract_id" in child_csv
                assert "__parent_extract_id" in child_csv
                assert "\n" in child_csv

    def test_to_json_bytes(self, processing_result: ProcessingResult):
        """Test to_json_bytes() method."""
        # Get JSON bytes
        json_bytes = processing_result.to_json_bytes()

        # Basic validation
        assert isinstance(json_bytes, dict)
        assert "main" in json_bytes
        assert isinstance(json_bytes["main"], bytes)
        assert len(json_bytes["main"]) > 0

        # Verify bytes can be decoded as JSON
        import json

        main_json = json.loads(json_bytes["main"])
        assert isinstance(main_json, list)
        assert len(main_json) > 0
        assert "id" in main_json[0]
        assert "name" in main_json[0]
        assert "__extract_id" in main_json[0]

        # Verify child tables
        for table_name, data in json_bytes.items():
            if table_name != "main":
                child_json = json.loads(data)
                assert isinstance(child_json, list)
                assert "__extract_id" in child_json[0]
                assert "__parent_extract_id" in child_json[0]


class TestFormatRoundtrips:
    """Test round-trip conversions between formats."""

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow", reason="PyArrow not available"),
        reason="PyArrow required for this test",
    )
    def test_dict_to_pyarrow_roundtrip(self, processing_result: ProcessingResult):
        """Test dict → PyArrow → dict round-trip."""
        import pyarrow as pa

        # Get original dict
        original_dict = processing_result.to_dict()

        # Convert to PyArrow
        tables = processing_result.to_pyarrow_tables()

        # Convert back to dict
        roundtrip_dict = {}
        for table_name, table in tables.items():
            roundtrip_dict[table_name] = table.to_pydict()
            # Convert from column-based to row-based format
            rows = []
            if table.num_rows > 0:
                for i in range(table.num_rows):
                    row = {
                        col: roundtrip_dict[table_name][col][i]
                        for col in roundtrip_dict[table_name]
                    }
                    rows.append(row)
                roundtrip_dict[table_name] = rows

        # Verify main table structure (should match original)
        assert len(roundtrip_dict["main"]) == len(original_dict["main"])
        for key in original_dict["main"][0].keys():
            assert key in roundtrip_dict["main"][0]

    def test_json_bytes_roundtrip(self, processing_result: ProcessingResult):
        """Test JSON bytes round-trip."""
        import json

        # Get original dict
        original_dict = processing_result.to_json_objects()

        # Convert to JSON bytes
        json_bytes = processing_result.to_json_bytes()

        # Convert back to dict
        roundtrip_dict = {}
        for table_name, data in json_bytes.items():
            roundtrip_dict[table_name] = json.loads(data)

        # Verify structure matches
        assert len(roundtrip_dict["main"]) == len(original_dict["main"])
        for key in original_dict["main"][0].keys():
            assert key in roundtrip_dict["main"][0]

        # Verify child tables
        for table_name in original_dict:
            if table_name != "main":
                assert table_name in roundtrip_dict
                assert len(roundtrip_dict[table_name]) == len(original_dict[table_name])
