"""Tests for metadata annotation, timestamp generation, and processing context."""

from datetime import datetime

import pytest

from transmog.config import TransmogConfig
from transmog.flattening import (
    annotate_with_metadata,
    get_current_timestamp,
)
from transmog.types import ProcessingContext


class TestTimestampGeneration:
    """Test timestamp generation functions."""

    def test_get_current_timestamp(self):
        """Test generating current timestamp."""
        timestamp = get_current_timestamp()

        assert isinstance(timestamp, str)
        assert len(timestamp) > 0

        try:
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            pytest.fail("Timestamp is not in valid ISO format")

    def test_timestamp_format_is_iso(self):
        """Test that timestamps follow ISO format consistently."""
        timestamps = [get_current_timestamp() for _ in range(5)]
        for ts in timestamps:
            assert isinstance(ts, str)
            assert len(ts) > 10
            # Should be parseable as ISO datetime
            datetime.fromisoformat(ts.replace("Z", "+00:00"))

    def test_timestamp_format(self):
        """Test timestamp format consistency."""
        timestamp = get_current_timestamp()

        assert " " in timestamp
        assert len(timestamp) > 10


class TestMetadataAnnotation:
    """Test metadata annotation functions."""

    def test_annotate_with_metadata_basic(self):
        """Test basic metadata annotation."""
        record = {"name": "test", "value": 42}
        config = TransmogConfig()

        annotated = annotate_with_metadata(record, config)

        assert isinstance(annotated, dict)
        assert "name" in annotated
        assert "value" in annotated
        assert annotated["name"] == "test"
        assert annotated["value"] == 42
        assert "_id" in annotated

    def test_annotate_modifies_in_place(self):
        """Test that annotation modifies record in place."""
        record = {"name": "test", "value": 42}
        config = TransmogConfig()

        annotated = annotate_with_metadata(record, config)

        assert annotated is record
        assert "_id" in record

    def test_annotate_with_parent_id(self):
        """Test metadata annotation with parent ID."""
        record = {"name": "child", "value": 42}
        parent_id = "parent_123"
        config = TransmogConfig()

        annotated = annotate_with_metadata(record, config, parent_id=parent_id)

        assert isinstance(annotated, dict)
        assert "_parent_id" in annotated
        assert annotated["_parent_id"] == parent_id

    def test_annotate_with_timestamp(self):
        """Test metadata annotation with timestamp."""
        record = {"name": "test", "value": 42}
        config = TransmogConfig()

        timestamp = get_current_timestamp()
        annotated = annotate_with_metadata(record, config, transmog_time=timestamp)

        assert isinstance(annotated, dict)
        assert "_timestamp" in annotated
        timestamp_found = False
        for key, value in annotated.items():
            if ("time" in key.lower() or "timestamp" in key.lower()) and isinstance(
                value, str
            ):
                timestamp_found = True
                break
        assert timestamp_found

    def test_annotate_empty_record(self):
        """Test annotating empty record."""
        record = {}
        config = TransmogConfig()

        annotated = annotate_with_metadata(record, config)

        assert isinstance(annotated, dict)
        assert len(annotated) > 0

    def test_annotate_record_with_existing_metadata(self):
        """Test annotating record that already has metadata fields."""
        record = {
            "name": "test",
            "_id": "existing_id",
            "_timestamp": "existing_time",
        }
        config = TransmogConfig()

        annotated = annotate_with_metadata(record, config)

        assert isinstance(annotated, dict)
        assert "name" in annotated


class TestProcessingContext:
    """Test ProcessingContext functionality."""

    def test_processing_context_defaults(self):
        """Test ProcessingContext default values."""
        context = ProcessingContext()

        assert context.current_depth == 0
        assert context.path_components == []
        assert isinstance(context.extract_time, str)

    def test_processing_context_descend(self):
        """Test creating nested contexts."""
        context = ProcessingContext(current_depth=1, path_components=["parent"])

        child_context = ProcessingContext(
            current_depth=context.current_depth + 1,
            path_components=context.path_components + ["child"],
            extract_time=context.extract_time,
        )

        assert child_context.current_depth == 2
        assert child_context.path_components == ["parent", "child"]
        assert child_context.extract_time == context.extract_time

    def test_processing_context_descend_preserves_extract_time(self):
        """Test that nested contexts preserve extract_time."""
        context = ProcessingContext(extract_time="2023-01-01T12:00:00Z")

        # Multiple levels of nesting
        level1 = ProcessingContext(
            current_depth=context.current_depth + 1,
            path_components=context.path_components + ["level1"],
            extract_time=context.extract_time,
        )
        level2 = ProcessingContext(
            current_depth=level1.current_depth + 1,
            path_components=level1.path_components + ["level2"],
            extract_time=level1.extract_time,
        )
        level3 = ProcessingContext(
            current_depth=level2.current_depth + 1,
            path_components=level2.path_components + ["level3"],
            extract_time=level2.extract_time,
        )

        assert level1.extract_time == "2023-01-01T12:00:00Z"
        assert level2.extract_time == "2023-01-01T12:00:00Z"
        assert level3.extract_time == "2023-01-01T12:00:00Z"
