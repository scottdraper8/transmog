"""
Format conversion interface tests.

This module defines abstract test classes for format conversion interfaces
that all implementations must satisfy.
"""

from typing import Any

import pytest


class AbstractFormatConversionTest:
    """
    Abstract test class for format conversion.

    All format conversion implementations must pass these tests to ensure
    consistent behavior across the system.
    """

    @pytest.fixture
    def sample_data(self) -> dict[str, Any]:
        """
        Fixture to provide sample test data.
        """
        return {
            "id": "rec123",
            "name": "Test Record",
            "details": {"created": "2023-01-01", "active": True, "score": 95.5},
            "items": [{"item_id": "i1", "value": 10}, {"item_id": "i2", "value": 20}],
        }

    @pytest.fixture
    def processor_factory(self):
        """
        Fixture to provide a function that creates a processor.

        Implementations must override this to provide the actual factory function.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def processing_result_factory(self, processor_factory, sample_data):
        """
        Fixture to provide a function that creates a processing result.
        """
        processor = processor_factory(cast_to_string=True, visit_arrays=True)
        return processor.process(sample_data, entity_name="test")

    def test_to_dict(self, processing_result_factory):
        """Test to_dict() method."""
        # Get processing result
        result = processing_result_factory

        # Get dictionary representation
        result_dict = result.to_dict()

        # Basic validation
        assert isinstance(result_dict, dict)
        assert "main_table" in result_dict
        assert "child_tables" in result_dict
        assert "entity_name" in result_dict
        assert "source_info" in result_dict

        # Verify main table structure
        main_table = result_dict["main_table"]
        assert isinstance(main_table, list)
        assert len(main_table) == 1
        assert "id" in main_table[0]
        assert "name" in main_table[0]
        assert "__extract_id" in main_table[0]  # Should have metadata fields

        # Verify child tables
        child_tables = result_dict["child_tables"]
        assert isinstance(child_tables, dict)

        # Find items table in child tables
        items_table = None
        for table_name, table_data in child_tables.items():
            if "items" in table_name:
                items_table = table_data
                break

        assert items_table is not None
        assert len(items_table) == 2  # Should have two items

    def test_to_json_objects(self, processing_result_factory):
        """Test to_json_objects() method."""
        # Get processing result
        result = processing_result_factory

        # Get JSON-serializable objects
        json_obj = result.to_json_objects()

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
    def test_to_pyarrow_tables(self, processing_result_factory):
        """Test to_pyarrow_tables() method."""
        import pyarrow as pa

        # Get processing result
        result = processing_result_factory

        # Get PyArrow tables
        tables = result.to_pyarrow_tables()

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

        # Check for required item columns (might have different names due to sanitization)
        item_id_col = False
        value_col = False

        for col_name in items_table.column_names:
            if "item" in col_name.lower() and "id" in col_name.lower():
                item_id_col = True
            if "value" in col_name.lower():
                value_col = True

        assert item_id_col, "Missing item ID column"
        assert value_col, "Missing value column"

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow", reason="PyArrow not available"),
        reason="PyArrow required for this test",
    )
    def test_to_parquet_bytes(self, processing_result_factory):
        """Test to_parquet_bytes() method."""
        import io

        import pyarrow.parquet as pq

        # Get processing result
        result = processing_result_factory

        # Get Parquet bytes
        parquet_bytes = result.to_parquet_bytes()

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

    def test_to_csv_bytes(self, processing_result_factory):
        """Test to_csv_bytes() method."""
        # Get processing result
        result = processing_result_factory

        # Get CSV bytes
        csv_bytes = result.to_csv_bytes()

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

    def test_to_json_bytes(self, processing_result_factory):
        """Test to_json_bytes() method."""
        # Get processing result
        result = processing_result_factory

        # Get JSON bytes
        json_bytes = result.to_json_bytes()

        # Basic validation
        assert isinstance(json_bytes, dict)
        assert "main" in json_bytes
        assert isinstance(json_bytes["main"], bytes)
        assert len(json_bytes["main"]) > 0

        # Verify bytes look like JSON format
        main_json = json_bytes["main"].decode("utf-8")
        assert "[" in main_json  # Should be an array
        assert "{" in main_json  # Should contain objects
        assert "id" in main_json
        assert "name" in main_json
        assert "__extract_id" in main_json

        # Verify child tables
        for table_name, data in json_bytes.items():
            if table_name != "main":
                child_json = data.decode("utf-8")
                assert "[" in child_json
                assert "{" in child_json
                assert "__extract_id" in child_json
                assert "__parent_extract_id" in child_json

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow", reason="PyArrow not available"),
        reason="PyArrow required for this test",
    )
    def test_dict_to_pyarrow_roundtrip(self, processing_result_factory):
        """Test roundtrip conversion between dict and PyArrow tables."""
        import pyarrow as pa

        # Get processing result
        result = processing_result_factory

        # Start with dict representation
        dict_data = result.to_dict()
        assert isinstance(dict_data["main_table"], list)

        # Convert to PyArrow
        pa_tables = result.to_pyarrow_tables()
        assert isinstance(pa_tables["main"], pa.Table)

        # Get row count from both
        dict_row_count = len(dict_data["main_table"])
        pa_row_count = pa_tables["main"].num_rows
        assert dict_row_count == pa_row_count

        # Check field presence in both
        dict_fields = list(dict_data["main_table"][0].keys())
        pa_fields = pa_tables["main"].column_names

        # All dict fields should have corresponding PyArrow columns
        # (allowing for sanitized field names)
        for field in dict_fields:
            sanitized_field = field.replace("-", "").replace("_", "").lower()
            matching_pa_field = any(
                col.replace("-", "").replace("_", "").lower() == sanitized_field
                for col in pa_fields
            )
            assert matching_pa_field, f"Field {field} missing in PyArrow table"

    def test_json_bytes_roundtrip(self, processing_result_factory):
        """Test roundtrip conversion through JSON bytes."""
        import json

        # Get processing result
        result = processing_result_factory

        # Get JSON bytes
        json_bytes = result.to_json_bytes()

        # Parse JSON bytes back to Python
        parsed_data = {}
        for table_name, bytes_data in json_bytes.items():
            parsed_data[table_name] = json.loads(bytes_data.decode("utf-8"))

        # Verify main table
        assert len(parsed_data["main"]) > 0
        assert isinstance(parsed_data["main"], list)
        assert isinstance(parsed_data["main"][0], dict)

        # Check for expected fields
        assert "id" in parsed_data["main"][0]
        assert "__extract_id" in parsed_data["main"][0]

        # Find items table
        items_table = None
        for table_name, table_data in parsed_data.items():
            if table_name != "main" and "items" in table_name:
                items_table = table_data
                break

        assert items_table is not None
        assert len(items_table) == 2  # Should have two items
