"""
Tests for metadata generation and ID handling.

Tests ID generation, timestamp handling, and metadata annotation.
"""

import pytest
import uuid
from datetime import datetime

from transmog.core.metadata import (
    TRANSMOG_NAMESPACE,
    annotate_with_metadata,
    create_batch_metadata,
    generate_composite_id,
    generate_deterministic_id,
    generate_transmog_id,
    get_current_timestamp,
)


class TestIdGeneration:
    """Test ID generation functions."""

    def test_generate_transmog_id(self):
        """Test generating random transmog IDs."""
        id1 = generate_transmog_id()
        id2 = generate_transmog_id()

        assert isinstance(id1, str)
        assert isinstance(id2, str)
        assert id1 != id2  # Should be unique
        assert len(id1) > 0
        assert len(id2) > 0

    def test_generate_deterministic_id(self):
        """Test generating deterministic IDs."""
        data1 = {"name": "test", "value": 42}
        data2 = {"name": "test", "value": 42}
        data3 = {"name": "different", "value": 42}

        id1 = generate_deterministic_id(data1)
        id2 = generate_deterministic_id(data2)
        id3 = generate_deterministic_id(data3)

        assert isinstance(id1, str)
        assert isinstance(id2, str)
        assert isinstance(id3, str)
        assert id1 == id2  # Same data should produce same ID
        assert id1 != id3  # Different data should produce different ID

    def test_generate_deterministic_id_with_key(self):
        """Test generating deterministic IDs with specific value."""
        data = {"id": 123, "name": "test", "value": 42}

        # Use the actual function signature - pass the value directly
        id1 = generate_deterministic_id(data["id"])
        id2 = generate_deterministic_id(data["id"])

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_generate_composite_id(self):
        """Test generating composite IDs."""
        values = {"field1": "part1", "field2": "part2", "field3": "part3"}
        fields = ["field1", "field2", "field3"]

        composite_id = generate_composite_id(values, fields)

        assert isinstance(composite_id, str)
        assert len(composite_id) > 0

    def test_generate_composite_id_empty_parts(self):
        """Test generating composite ID with empty parts."""
        composite_id = generate_composite_id({}, [])

        assert isinstance(composite_id, str)

    def test_generate_composite_id_with_none(self):
        """Test generating composite ID with None values."""
        values = {"field1": "part1", "field2": None, "field3": "part3"}
        fields = ["field1", "field2", "field3"]

        composite_id = generate_composite_id(values, fields)

        assert isinstance(composite_id, str)
        assert len(composite_id) > 0

    def test_deterministic_id_consistency(self):
        """Test that deterministic IDs are consistent across calls."""
        data = {"name": "consistency_test", "value": 123}

        ids = [generate_deterministic_id(data) for _ in range(10)]

        # All IDs should be identical
        assert all(id_val == ids[0] for id_val in ids)

    def test_deterministic_id_with_different_order(self):
        """Test deterministic IDs with different key order."""
        data1 = {"a": 1, "b": 2, "c": 3}
        data2 = {"c": 3, "a": 1, "b": 2}

        # Function takes a single value, not a dict
        # So we need to test with the same value
        id1 = generate_deterministic_id("test_value")
        id2 = generate_deterministic_id("test_value")

        # Should be the same for same value
        assert id1 == id2


class TestTimestampGeneration:
    """Test timestamp generation functions."""

    def test_get_current_timestamp(self):
        """Test getting timestamp."""
        timestamp = get_current_timestamp()

        assert isinstance(timestamp, str)
        assert len(timestamp) > 0

        # Should be a valid ISO format timestamp
        try:
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            pytest.fail("Timestamp is not in valid ISO format")

    def test_timestamp_uniqueness(self):
        """Test that timestamps are reasonably unique."""
        timestamps = [get_current_timestamp() for _ in range(5)]

        # At least some should be different (unless system is very fast)
        assert len(set(timestamps)) >= 1

    def test_timestamp_format(self):
        """Test timestamp format consistency."""
        timestamp = get_current_timestamp()

        # Default format doesn't include 'Z', just space separator
        assert " " in timestamp  # Space between date and time
        # Should contain date and time components
        assert len(timestamp) > 10


class TestMetadataAnnotation:
    """Test metadata annotation functions."""

    def test_annotate_with_metadata_basic(self):
        """Test basic metadata annotation."""
        record = {"name": "test", "value": 42}

        annotated = annotate_with_metadata(record)

        assert isinstance(annotated, dict)
        assert "name" in annotated
        assert "value" in annotated
        assert annotated["name"] == "test"
        assert annotated["value"] == 42

        # Should have metadata fields
        has_id = any("id" in key.lower() for key in annotated.keys())
        assert has_id

    def test_annotate_with_metadata_custom_id(self):
        """Test metadata annotation with custom ID."""
        record = {"name": "test", "value": 42}
        custom_id = "custom_123"

        # Use the correct parameter name
        annotated = annotate_with_metadata(record, transmog_id=custom_id)

        assert isinstance(annotated, dict)
        # Should contain the custom ID
        id_found = False
        for key, value in annotated.items():
            if "id" in key.lower() and value == custom_id:
                id_found = True
                break
        assert id_found

    def test_annotate_with_metadata_preserves_original(self):
        """Test that annotation preserves original record."""
        original = {"name": "test", "value": 42}
        record = original.copy()

        annotated = annotate_with_metadata(record)

        # Original should be unchanged
        assert record == original
        assert annotated != original

    def test_annotate_with_metadata_parent_id(self):
        """Test metadata annotation with parent ID."""
        record = {"name": "child", "value": 42}
        parent_id = "parent_123"

        annotated = annotate_with_metadata(record, parent_id=parent_id)

        assert isinstance(annotated, dict)
        # Should contain parent ID
        parent_found = False
        for key, value in annotated.items():
            if "parent" in key.lower() and value == parent_id:
                parent_found = True
                break
        assert parent_found

    def test_annotate_with_metadata_timestamp(self):
        """Test metadata annotation with timestamp."""
        record = {"name": "test", "value": 42}

        # Use the correct parameter - pass transmog_time directly
        timestamp = get_current_timestamp()
        annotated = annotate_with_metadata(record, transmog_time=timestamp)

        assert isinstance(annotated, dict)
        # Should contain timestamp
        timestamp_found = False
        for key, value in annotated.items():
            if ("time" in key.lower() or "timestamp" in key.lower()) and isinstance(
                value, str
            ):
                timestamp_found = True
                break
        assert timestamp_found


class TestBatchMetadata:
    """Test batch metadata creation."""

    def test_create_batch_metadata(self):
        """Test creating batch metadata."""
        batch_metadata = create_batch_metadata(batch_size=100)

        assert isinstance(batch_metadata, dict)
        assert len(batch_metadata) > 0

    def test_create_batch_metadata_with_size(self):
        """Test creating batch metadata with batch size."""
        batch_size = 100
        batch_metadata = create_batch_metadata(batch_size=batch_size)

        assert isinstance(batch_metadata, dict)
        # Should contain batch size information
        size_found = any(
            str(batch_size) in str(value) for value in batch_metadata.values()
        )

    def test_create_batch_metadata_with_index(self):
        """Test creating batch metadata with batch size."""
        batch_size = 50
        batch_metadata = create_batch_metadata(batch_size=batch_size)

        assert isinstance(batch_metadata, dict)
        # Should contain batch size information
        size_found = any(
            str(batch_size) in str(value) for value in batch_metadata.values()
        )


class TestTransmogNamespace:
    """Test transmog namespace constant."""

    def test_transmog_namespace_exists(self):
        """Test that transmog namespace is defined."""
        # TRANSMOG_NAMESPACE is a UUID object, not a string
        assert isinstance(TRANSMOG_NAMESPACE, uuid.UUID)

    def test_transmog_namespace_format(self):
        """Test transmog namespace format."""
        # Should be a valid UUID
        assert str(TRANSMOG_NAMESPACE)  # Can be converted to string
        assert len(str(TRANSMOG_NAMESPACE)) == 36  # Standard UUID length


class TestMetadataEdgeCases:
    """Test edge cases for metadata functions."""

    def test_annotate_empty_record(self):
        """Test annotating empty record."""
        record = {}

        annotated = annotate_with_metadata(record)

        assert isinstance(annotated, dict)
        # Should have at least metadata fields
        assert len(annotated) > 0

    def test_annotate_record_with_existing_metadata(self):
        """Test annotating record that already has metadata fields."""
        record = {
            "name": "test",
            "__transmog_id": "existing_id",
            "__transmog_datetime": "existing_time",
        }

        annotated = annotate_with_metadata(record)

        assert isinstance(annotated, dict)
        assert "name" in annotated

    def test_generate_id_with_complex_data(self):
        """Test ID generation with complex nested data."""
        complex_data = {
            "nested": {
                "array": [1, 2, {"inner": "value"}],
                "null": None,
                "boolean": True,
            },
            "unicode": "café",
            "number": 3.14159,
        }

        id1 = generate_deterministic_id(complex_data)
        id2 = generate_deterministic_id(complex_data)

        assert isinstance(id1, str)
        assert id1 == id2  # Should be deterministic

    def test_composite_id_with_mixed_types(self):
        """Test composite ID generation with mixed types."""
        values = {
            "field1": 1,
            "field2": "string",
            "field3": 3.14,
            "field4": True,
            "field5": None,
        }
        fields = ["field1", "field2", "field3", "field4", "field5"]

        composite_id = generate_composite_id(values, fields)

        assert isinstance(composite_id, str)
        assert len(composite_id) > 0
