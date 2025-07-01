"""
Tests for hierarchical data processing.

Tests the core hierarchy processing logic and structure processing functions.
"""

import pytest

import transmog as tm
from transmog.core.hierarchy import (
    process_record_batch,
    process_structure,
    stream_process_records,
)


class TestProcessStructure:
    """Test the process_structure function."""

    def test_process_simple_structure(self, simple_data):
        """Test processing simple hierarchical structure."""
        main_record, child_tables = process_structure(
            simple_data, entity_name="simple_test"
        )

        assert main_record is not None
        assert isinstance(main_record, dict)
        assert isinstance(child_tables, dict)

        # Should have basic fields
        assert "name" in main_record
        assert main_record["name"] == "Test Entity"

    def test_process_nested_structure(self, complex_nested_data):
        """Test processing complex nested structure."""
        main_record, child_tables = process_structure(
            complex_nested_data, entity_name="complex_test", visit_arrays=True
        )

        assert main_record is not None
        assert isinstance(child_tables, dict)

        # Should have flattened nested fields
        assert "name" in main_record
        assert main_record["name"] == "Complex Entity"

        # Should have child tables from arrays
        assert len(child_tables) > 0

    def test_process_structure_with_arrays(self, array_data):
        """Test processing structure with arrays."""
        main_record, child_tables = process_structure(
            array_data, entity_name="array_test", visit_arrays=True
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
        main_record, child_tables = process_structure(
            array_data, entity_name="skip_test", visit_arrays=False
        )

        assert main_record is not None
        assert isinstance(child_tables, dict)

        # Should have main record
        assert "name" in main_record

        # Should have no child tables
        assert len(child_tables) == 0

    def test_process_structure_with_parent(self, simple_data):
        """Test processing structure with parent ID."""
        main_record, child_tables = process_structure(
            simple_data, entity_name="child_test", parent_id="parent_123"
        )

        assert main_record is not None

        # Should have parent reference
        parent_fields = [k for k in main_record.keys() if "parent" in k.lower()]
        assert len(parent_fields) > 0

    def test_process_structure_custom_separator(self, simple_data):
        """Test processing structure with custom separator."""
        main_record, child_tables = process_structure(
            simple_data, entity_name="sep_test", separator="."
        )

        assert main_record is not None

        # Should have dot-separated fields
        dot_fields = [k for k in main_record.keys() if "." in k]
        assert len(dot_fields) > 0

    def test_process_empty_structure(self):
        """Test processing empty structure."""
        main_record, child_tables = process_structure({}, entity_name="empty_test")

        assert main_record is not None
        assert isinstance(child_tables, dict)

    def test_process_null_structure(self):
        """Test processing null structure."""
        main_record, child_tables = process_structure(None, entity_name="null_test")

        assert main_record is not None
        assert isinstance(child_tables, dict)


class TestProcessRecordBatch:
    """Test the process_record_batch function."""

    def test_process_batch_simple(self, batch_data):
        """Test processing a batch of records."""
        main_records, child_tables = process_record_batch(
            batch_data, entity_name="batch_test"
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
        batch = [array_data] * 3  # Create batch of 3 identical records

        main_records, child_tables = process_record_batch(
            batch, entity_name="array_batch", visit_arrays=True
        )

        assert len(main_records) == 3
        assert len(child_tables) > 0

        # All records should have the same name
        for record in main_records:
            assert record["name"] == "Company"

    def test_process_batch_custom_batch_size(self, batch_data):
        """Test processing batch with custom batch size."""
        main_records, child_tables = process_record_batch(
            batch_data, entity_name="custom_batch", batch_size=3
        )

        assert len(main_records) == len(batch_data)
        assert isinstance(child_tables, dict)

    def test_process_empty_batch(self):
        """Test processing empty batch."""
        main_records, child_tables = process_record_batch([], entity_name="empty_batch")

        assert isinstance(main_records, list)
        assert len(main_records) == 0
        assert isinstance(child_tables, dict)


class TestStreamProcessRecords:
    """Test the stream_process_records function."""

    def test_stream_process_simple(self, batch_data):
        """Test streaming processing of records."""
        main_records, child_generator = stream_process_records(
            batch_data, entity_name="stream_test"
        )

        assert main_records is not None
        assert isinstance(main_records, list)
        assert len(main_records) == len(batch_data)

        # Child generator should be iterable
        child_tables = {}
        for table_name, table_data in child_generator:
            if table_name not in child_tables:
                child_tables[table_name] = []
            child_tables[table_name].extend(
                table_data if isinstance(table_data, list) else [table_data]
            )

        assert isinstance(child_tables, dict)

    def test_stream_process_with_arrays(self, array_data):
        """Test streaming processing with arrays."""
        batch = [array_data] * 2

        main_records, child_generator = stream_process_records(
            batch, entity_name="stream_array", visit_arrays=True
        )

        assert len(main_records) == 2

        # Consume the generator
        child_tables = {}
        for table_name, table_data in child_generator:
            if table_name not in child_tables:
                child_tables[table_name] = []
            child_tables[table_name].extend(
                table_data if isinstance(table_data, list) else [table_data]
            )

        # Should have child tables from arrays
        assert len(child_tables) > 0

    def test_stream_process_deterministic_ids(self, simple_data):
        """Test streaming processing with deterministic IDs."""
        batch = [simple_data] * 2

        main_records, child_generator = stream_process_records(
            batch, entity_name="deterministic_test", use_deterministic_ids=True
        )

        assert len(main_records) == 2

        # Consume generator
        list(child_generator)


class TestHierarchyIntegration:
    """Test hierarchy processing integration with transmog."""

    def test_hierarchy_with_transmog_flatten(self, complex_nested_data):
        """Test that transmog.flatten uses hierarchy processing."""
        result = tm.flatten(complex_nested_data, name="hierarchy_test")

        assert result is not None
        assert len(result.main) == 1
        assert len(result.tables) > 0

    def test_hierarchy_with_different_thresholds(self, complex_nested_data):
        """Test hierarchy processing with different nesting thresholds."""
        # Low threshold - more flattening
        result_low = tm.flatten(
            complex_nested_data, name="low_threshold", nested_threshold=2
        )

        # High threshold - less flattening
        result_high = tm.flatten(
            complex_nested_data, name="high_threshold", nested_threshold=10
        )

        assert len(result_low.main) == 1
        assert len(result_high.main) == 1

        # Both should work, potentially with different structures

    def test_hierarchy_with_streaming(self, batch_data):
        """Test hierarchy processing with streaming mode."""
        # This tests the integration between hierarchy and streaming
        result = tm.flatten(batch_data, name="stream_hierarchy", low_memory=True)

        assert result is not None
        assert len(result.main) == len(batch_data)


class TestHierarchyEdgeCases:
    """Test edge cases in hierarchy processing."""

    def test_circular_reference_handling(self):
        """Test handling of circular references in hierarchy."""
        data = {"name": "test", "child": {}}
        data["child"]["parent"] = data  # Create circular reference

        # Test that circular references are handled with skip recovery strategy
        main_record, child_tables = process_structure(
            data,
            entity_name="circular_test",
            max_depth=5,  # Limit depth to prevent infinite recursion
            recovery_strategy="skip",  # Skip problematic fields
        )

        # Should handle gracefully with skip strategy
        assert main_record is not None
        assert "name" in main_record  # Should have processed the simple field

    def test_very_deep_hierarchy(self):
        """Test very deep hierarchical structures."""
        # Create deeply nested structure
        data = {"root": {}}
        current = data["root"]

        for i in range(20):  # 20 levels deep
            current[f"level_{i}"] = {}
            current = current[f"level_{i}"]

        current["value"] = "deep"

        main_record, child_tables = process_structure(
            data, entity_name="deep_test", max_depth=25, nested_threshold=25
        )

        assert main_record is not None
        # Should have flattened deep fields - look for the deep value
        assert (
            "root_level_0_level_1_level_2_level_3_level_4_level_5_level_6_level_7_level_8_level_9_level_10_level_11_level_12_level_13_level_14_level_15_level_16_level_17_level_18_level_19_value"
            in main_record
            or any("value" in k for k in main_record.keys())
        )
        # Should have processed the deep structure
        deep_fields = [k for k in main_record.keys() if "level_" in k or "value" in k]
        assert len(deep_fields) >= 1  # At least the final value should be present

    def test_mixed_data_types_hierarchy(self, mixed_types_data):
        """Test hierarchy processing with mixed data types."""
        main_record, child_tables = process_structure(
            mixed_types_data, entity_name="mixed_test"
        )

        assert main_record is not None
        assert main_record["name"] == "Mixed Types Test"

    def test_large_batch_hierarchy(self):
        """Test hierarchy processing with large batches."""
        # Create large batch
        large_batch = [
            {"id": i, "name": f"Record {i}", "value": i * 10} for i in range(1000)
        ]

        main_records, child_tables = process_record_batch(
            large_batch, entity_name="large_test", batch_size=100
        )

        assert len(main_records) == 1000
        assert main_records[0]["name"] == "Record 0"
        assert main_records[-1]["name"] == "Record 999"
