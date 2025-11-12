"""
Tests for hierarchical data processing.

Tests the core hierarchy processing logic and structure processing functions.
"""

import pytest

import transmog as tm
from transmog.config import TransmogConfig
from transmog.flattening import (
    _process_structure,
    process_record_batch,
)
from transmog.types import ProcessingContext


class TestProcessStructure:
    """Test the process_structure function."""

    def test_process_simple_structure(self, simple_data):
        """Test processing simple hierarchical structure."""
        config = TransmogConfig()
        context = ProcessingContext()
        main_record, child_tables = _process_structure(
            simple_data, entity_name="simple_test", config=config, _context=context
        )

        assert main_record is not None
        assert isinstance(main_record, dict)
        assert isinstance(child_tables, dict)

        # Should have basic fields
        assert "name" in main_record
        assert main_record["name"] == "Test Entity"

    def test_process_nested_structure(self, complex_nested_data):
        """Test processing complex nested structure."""
        from transmog.types import ArrayMode

        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        context = ProcessingContext()
        main_record, child_tables = _process_structure(
            complex_nested_data,
            entity_name="complex_test",
            config=config,
            _context=context,
        )

        assert main_record is not None
        assert isinstance(child_tables, dict)

        # Should have flattened nested fields
        assert "name" in main_record
        assert "organization_name" in main_record
        assert main_record["organization_name"] == "Main Org"

        # Should have child tables from arrays
        assert len(child_tables) > 0

    def test_process_structure_with_arrays(self, array_data):
        """Test processing structure with arrays."""
        from transmog.types import ArrayMode

        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        context = ProcessingContext()
        main_record, child_tables = _process_structure(
            array_data, entity_name="array_test", config=config, _context=context
        )

        assert main_record is not None
        assert isinstance(child_tables, dict)

        # Should have main record
        assert "name" in main_record
        assert main_record["name"] == "Company"

        # Should have child tables
        assert len(child_tables) > 0

    def test_process_structure_skip_arrays(self, array_data):
        """Test processing structure with arrays skipped."""
        from transmog.types import ArrayMode

        config = TransmogConfig(array_mode=ArrayMode.SKIP)
        context = ProcessingContext()
        main_record, child_tables = _process_structure(
            array_data, entity_name="skip_test", config=config, _context=context
        )

        assert main_record is not None
        assert isinstance(child_tables, dict)

        # Should have main record
        assert "name" in main_record

        # Should have no child tables
        assert len(child_tables) == 0

    def test_process_structure_with_parent(self, simple_data):
        """Test processing structure with parent ID."""
        config = TransmogConfig()
        context = ProcessingContext()
        main_record, child_tables = _process_structure(
            simple_data,
            entity_name="child_test",
            config=config,
            _context=context,
            parent_id="parent_123",
        )

        assert main_record is not None

        # Should have parent reference
        parent_fields = [k for k in main_record.keys() if "parent" in k.lower()]
        assert len(parent_fields) > 0

    def test_process_empty_structure(self):
        """Test processing empty structure."""
        config = TransmogConfig()
        context = ProcessingContext()
        main_record, child_tables = _process_structure(
            {}, entity_name="empty_test", config=config, _context=context
        )

        assert main_record is not None
        assert isinstance(child_tables, dict)

    def test_process_null_structure(self):
        """Test processing null structure."""
        config = TransmogConfig()
        context = ProcessingContext()
        main_record, child_tables = _process_structure(
            None, entity_name="null_test", config=config, _context=context
        )

        assert main_record is not None
        assert isinstance(child_tables, dict)


class TestProcessRecordBatch:
    """Test the process_record_batch function."""

    def test_process_batch_simple(self, batch_data):
        """Test processing a batch of records."""
        config = TransmogConfig()
        context = ProcessingContext()
        main_records, child_tables = process_record_batch(
            batch_data, entity_name="batch_test", config=config, _context=context
        )

        assert main_records is not None
        assert isinstance(main_records, list)
        assert isinstance(child_tables, dict)

        # Should have all records
        assert len(main_records) == len(batch_data)

        # Check first record
        assert main_records[0]["name"] == "Record 1"

    def test_process_batch_with_arrays(self, array_data):
        """Test processing batch with arrays."""
        batch = [array_data] * 3

        from transmog.types import ArrayMode

        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        context = ProcessingContext()
        main_records, child_tables = process_record_batch(
            batch, entity_name="array_batch", config=config, _context=context
        )

        assert len(main_records) == 3
        assert len(child_tables) > 0

        # All records should have the same name
        for record in main_records:
            assert record["name"] == "Company"

    def test_process_batch_custom_batch_size(self, batch_data):
        """Test processing batch with custom batch size."""
        config = TransmogConfig(batch_size=3)
        context = ProcessingContext()
        main_records, child_tables = process_record_batch(
            batch_data, entity_name="custom_batch", config=config, _context=context
        )

        assert len(main_records) == len(batch_data)
        assert isinstance(child_tables, dict)

    def test_process_empty_batch(self):
        """Test processing empty batch."""
        config = TransmogConfig()
        context = ProcessingContext()
        main_records, child_tables = process_record_batch(
            [], entity_name="empty_batch", config=config, _context=context
        )

        assert isinstance(main_records, list)
        assert len(main_records) == 0
        assert isinstance(child_tables, dict)


class TestHierarchyIntegration:
    """Test hierarchy processing integration with transmog."""

    def test_hierarchy_with_transmog_flatten(self, complex_nested_data):
        """Test that transmog.flatten uses hierarchy processing."""
        result = tm.flatten(complex_nested_data, name="hierarchy_test")

        assert result is not None
        assert len(result.main) == 1
        assert len(result.tables) > 0

    def test_hierarchy_with_nested_paths(self, complex_nested_data):
        """Test hierarchy processing with nested paths."""
        config = TransmogConfig()
        result = tm.flatten(complex_nested_data, name="nested_test", config=config)

        assert len(result.main) == 1

    def test_hierarchy_with_streaming(self, batch_data):
        """Test hierarchy processing with streaming mode."""
        config = TransmogConfig(batch_size=100)
        result = tm.flatten(batch_data, name="stream_hierarchy", config=config)

        assert result is not None
        assert len(result.main) == len(batch_data)


class TestHierarchyEdgeCases:
    """Test edge cases in hierarchy processing."""

    def test_circular_reference_handling(self):
        """Test handling of circular references in hierarchy."""
        data = {"name": "test", "child": {}}
        data["child"]["parent"] = data

        config = TransmogConfig(max_depth=5)
        context = ProcessingContext()
        main_record, child_tables = _process_structure(
            data,
            entity_name="circular_test",
            config=config,
            _context=context,
        )

        # Should handle gracefully with skip strategy
        assert main_record is not None
        assert "name" in main_record

    def test_very_deep_hierarchy(self):
        """Test very deep hierarchical structures."""
        data = {"root": {}}
        current = data["root"]

        for i in range(20):
            current[f"level_{i}"] = {}
            current = current[f"level_{i}"]

        current["value"] = "deep"

        config = TransmogConfig(max_depth=25)
        context = ProcessingContext()
        main_record, child_tables = _process_structure(
            data, entity_name="deep_test", config=config, _context=context
        )

        assert main_record is not None
        assert (
            "root_level_0_level_1_level_2_level_3_level_4_level_5_level_6_level_7_level_8_level_9_level_10_level_11_level_12_level_13_level_14_level_15_level_16_level_17_level_18_level_19_value"
            in main_record
            or any("value" in k for k in main_record.keys())
        )
        deep_fields = [k for k in main_record.keys() if "level_" in k or "value" in k]
        assert len(deep_fields) >= 1

    def test_mixed_data_types_hierarchy(self, mixed_types_data):
        """Test hierarchy processing with mixed data types."""
        config = TransmogConfig()
        context = ProcessingContext()
        main_record, child_tables = _process_structure(
            mixed_types_data, entity_name="mixed_test", config=config, _context=context
        )

        assert main_record is not None
        assert main_record["name"] == "Mixed Types Test"

    def test_large_batch_hierarchy(self):
        """Test hierarchy processing with large batches."""
        large_batch = [
            {"id": i, "name": f"Record {i}", "value": i * 10} for i in range(1000)
        ]

        config = TransmogConfig(batch_size=100)
        context = ProcessingContext()
        main_records, child_tables = process_record_batch(
            large_batch, entity_name="large_test", config=config, _context=context
        )

        assert len(main_records) == 1000
        assert main_records[0]["name"] == "Record 0"
        assert main_records[-1]["name"] == "Record 999"
