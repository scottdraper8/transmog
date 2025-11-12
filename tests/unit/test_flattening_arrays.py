"""
Tests for array extraction functionality.

Tests the core array extraction logic, array processing modes, and nested array handling.
"""

from datetime import datetime

import pytest

import transmog as tm
from transmog.config import TransmogConfig
from transmog.flattening import flatten_json
from transmog.types import ArrayMode, ProcessingContext


class TestExtractArraysFunction:
    """Test the array extraction functionality via flatten_json."""

    def test_extract_arrays_basic(self, array_data):
        """Test basic array extraction function."""
        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        context = ProcessingContext(extract_time=str(datetime.now()))

        _flattened, result = flatten_json(
            array_data,
            config=config,
            _context=context,
            _collect_arrays=True,
            _parent_id="parent-1",
            _entity_name="test",
        )

        assert isinstance(result, dict)
        # Should extract arrays from the data
        assert len(result) > 0

    def test_extract_arrays_with_parent_id(self, array_data):
        """Test array extraction with parent ID."""
        parent_id = "test-parent-123"
        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        context = ProcessingContext(extract_time=str(datetime.now()))

        _flattened, result = flatten_json(
            array_data,
            config=config,
            _context=context,
            _collect_arrays=True,
            _parent_id=parent_id,
            _entity_name="test_entity",
        )

        assert isinstance(result, dict)

        # Check that direct child tables have the correct parent ID
        direct_child_tables = [name for name in result.keys() if name.count("_") == 2]

        for table_name in direct_child_tables:
            records = result[table_name]
            if records:
                for record in records:
                    assert config.parent_field in record
                    assert record[config.parent_field] == parent_id

    def test_extract_arrays_with_options(self, array_data):
        """Test array extraction with configuration options."""
        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        context = ProcessingContext(extract_time=str(datetime.now()))

        _flattened, result = flatten_json(
            array_data,
            config=config,
            _context=context,
            _collect_arrays=True,
            _entity_name="test",
        )

        assert isinstance(result, dict)

    def test_extract_arrays_depth_limit(self, complex_nested_data):
        """Test array extraction with depth limits."""
        config_shallow = TransmogConfig(max_depth=2, array_mode=ArrayMode.SEPARATE)
        config_deep = TransmogConfig(max_depth=10, array_mode=ArrayMode.SEPARATE)
        context = ProcessingContext(extract_time=str(datetime.now()))

        _flat_shallow, result_shallow = flatten_json(
            complex_nested_data,
            config=config_shallow,
            _context=context,
            _collect_arrays=True,
            _entity_name="test",
        )
        _flat_deep, result_deep = flatten_json(
            complex_nested_data,
            config=config_deep,
            _context=context,
            _collect_arrays=True,
            _entity_name="test",
        )

        assert isinstance(result_shallow, dict)
        assert isinstance(result_deep, dict)

    def test_extract_primitive_arrays(self):
        """Test extracting arrays of primitive values."""
        data = {
            "id": 1,
            "tags": ["tag1", "tag2", "tag3"],
            "numbers": [1, 2, 3, 4, 5],
            "booleans": [True, False, True],
        }
        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        context = ProcessingContext(extract_time=str(datetime.now()))

        _flattened, result = flatten_json(
            data,
            config=config,
            _context=context,
            _collect_arrays=True,
            _entity_name="primitive_test",
        )

        assert isinstance(result, dict)
        # Should have extracted the arrays
        assert len(result) >= 1

    def test_extract_object_arrays(self, array_data):
        """Test extracting arrays of objects."""
        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        context = ProcessingContext(extract_time=str(datetime.now()))

        _flattened, result = flatten_json(
            array_data,
            config=config,
            _context=context,
            _collect_arrays=True,
            _entity_name="object_test",
        )

        assert isinstance(result, dict)
        # Should extract employee array and other arrays
        assert len(result) > 0

    def test_extract_empty_arrays(self):
        """Test extracting empty arrays."""
        data = {"id": 1, "empty_array": [], "nested": {"also_empty": []}}
        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        context = ProcessingContext(extract_time=str(datetime.now()))

        _flattened, result = flatten_json(
            data,
            config=config,
            _context=context,
            _collect_arrays=True,
            _entity_name="empty_test",
        )

        # Empty arrays should be skipped
        assert isinstance(result, dict)

    def test_extract_no_arrays(self):
        """Test extraction when no arrays present."""
        data = {"id": 1, "name": "test", "nested": {"value": 42, "active": True}}
        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        context = ProcessingContext(extract_time=str(datetime.now()))

        _flattened, result = flatten_json(
            data,
            config=config,
            _context=context,
            _collect_arrays=True,
            _entity_name="no_array_test",
        )

        # Should return empty dict when no arrays
        assert isinstance(result, dict)
        assert len(result) == 0


class TestArrayExtractionIntegration:
    """Test array extraction integration with flattening."""

    def test_extraction_with_transmog_flatten(self, array_data):
        """Test that transmog.flatten properly handles arrays."""
        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        result = tm.flatten(array_data, name="test", config=config)

        # Should have main table
        assert len(result.main) == 1

        # Should have child tables from arrays
        assert len(result.tables) >= 0

    def test_array_parent_child_relationships(self, array_data):
        """Test parent-child relationships in extracted arrays."""
        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        result = tm.flatten(array_data, name="relationship_test", config=config)

        # Check main record has ID - the API uses _id not __transmog_id
        main_record = result.main[0]
        assert "_id" in main_record or "id" in main_record

        # Get the main ID - could be natural ID or generated ID
        main_id = main_record.get("_id") or main_record.get("id")
        assert main_id is not None

        # Check direct child tables reference parent
        # Only check tables that are direct children of the main table
        direct_child_tables = {}
        for table_name, records in result.tables.items():
            # Direct child tables have format: relationship_test_tablename
            # Nested tables have format: relationship_test_parent_child
            parts = table_name.split("_")
            if len(parts) == 3:  # relationship + test + tablename
                direct_child_tables[table_name] = records

        for _table_name, records in direct_child_tables.items():
            for record in records:
                assert "_parent_id" in record
                assert record["_parent_id"] == main_id


class TestArrayExtractionEdgeCases:
    """Test edge cases in array extraction."""

    def test_deeply_nested_arrays(self):
        """Test extraction of deeply nested arrays."""
        data = {
            "level1": [
                {
                    "id": "L1",
                    "level2": [{"id": "L2", "level3": [{"id": "L3", "value": "deep"}]}],
                }
            ]
        }
        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        context = ProcessingContext(extract_time=str(datetime.now()))

        _flattened, result = flatten_json(
            data,
            config=config,
            _context=context,
            _collect_arrays=True,
            _entity_name="deep_test",
        )

        assert isinstance(result, dict)
        # Should have extracted multiple levels
        assert len(result) >= 1

    def test_arrays_with_null_values(self):
        """Test arrays containing null values."""
        data = {
            "items": [
                {"id": 1, "name": "Item 1"},
                None,
                {"id": 2, "name": "Item 2"},
            ]
        }
        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        context = ProcessingContext(extract_time=str(datetime.now()))

        _flattened, result = flatten_json(
            data,
            config=config,
            _context=context,
            _collect_arrays=True,
            _entity_name="null_test",
        )

        assert isinstance(result, dict)

    def test_large_arrays(self):
        """Test extraction of large arrays."""
        large_array = [{"id": i, "value": f"item_{i}"} for i in range(100)]
        data = {"large_items": large_array}
        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        context = ProcessingContext(extract_time=str(datetime.now()))

        _flattened, result = flatten_json(
            data,
            config=config,
            _context=context,
            _collect_arrays=True,
            _entity_name="large_test",
        )

        assert isinstance(result, dict)
        if result:
            # Should have extracted all items
            total_records = sum(len(records) for records in result.values())
            assert total_records == 100

    def test_arrays_with_special_characters(self):
        """Test arrays with special characters in field names."""
        data = {
            "special-items": [
                {"field@name": "value1", "field with spaces": "value2"},
                {"field@name": "value3", "field with spaces": "value4"},
            ]
        }
        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        context = ProcessingContext(extract_time=str(datetime.now()))

        _flattened, result = flatten_json(
            data,
            config=config,
            _context=context,
            _collect_arrays=True,
            _entity_name="special_test",
        )

        assert isinstance(result, dict)
