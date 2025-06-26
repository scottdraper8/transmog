"""
Tests for metadata interface conformance.

This module defines an abstract test class for testing metadata functionality.
"""

import datetime

import pytest

from transmog.core.metadata import (
    annotate_with_metadata,
    generate_deterministic_id,
    generate_transmog_id,
)


class TestMetadataInterface:
    """Test that metadata functions conform to the required interface."""

    def test_metadata_functions_exist(self):
        """Test that core metadata functions exist."""
        # Check main metadata functions
        assert callable(annotate_with_metadata), (
            "annotate_with_metadata should be a callable function"
        )
        assert callable(generate_transmog_id), (
            "generate_transmog_id should be a callable function"
        )
        assert callable(generate_deterministic_id), (
            "generate_deterministic_id should be a callable function"
        )


class AbstractMetadataTest:
    """
    Abstract base class for metadata function tests.

    This class defines a standardized set of tests that should apply to metadata functionality.
    """

    @pytest.fixture
    def simple_record(self):
        """Create a simple record for metadata testing."""
        return {"id": "123", "name": "Test Record", "value": 42}

    def test_annotate_with_metadata(self, simple_record):
        """Test basic metadata annotation."""
        # Annotate record with metadata
        annotated = annotate_with_metadata(simple_record)

        # Check that either natural ID or transmog ID is present
        has_natural_id = "id" in simple_record
        if has_natural_id:
            # When natural ID exists, transmog ID should not be added
            assert "__transmog_id" not in annotated
            assert annotated["id"] == simple_record["id"]
        else:
            # When no natural ID, transmog ID should be added
            assert "__transmog_id" in annotated
            assert isinstance(annotated["__transmog_id"], str)
            assert len(annotated["__transmog_id"]) > 0

        # Datetime should always be added
        assert "__transmog_datetime" in annotated

        # Verify original fields are preserved
        assert annotated["name"] == simple_record["name"]
        assert annotated["value"] == simple_record["value"]

        # Check datetime format
        assert isinstance(annotated["__transmog_datetime"], (str, datetime.datetime))

    def test_annotate_with_parent_id(self, simple_record):
        """Test metadata annotation with parent ID."""
        # Define a parent ID
        parent_id = "parent123"

        # Annotate with parent ID
        annotated = annotate_with_metadata(simple_record, parent_id=parent_id)

        # Check that parent ID field is added
        assert "__parent_transmog_id" in annotated
        assert annotated["__parent_transmog_id"] == parent_id

    def test_annotate_with_path(self, simple_record):
        """Test metadata annotation with extraction path."""
        # Define an extraction path
        path = "items.subitem"

        # Create extra fields with path
        extra_fields = {"__transmog_path": path}

        # Annotate with path in extra fields
        annotated = annotate_with_metadata(simple_record, extra_fields=extra_fields)

        # Check that path field is added
        assert "__transmog_path" in annotated
        assert annotated["__transmog_path"] == path

    def test_annotate_with_custom_time(self, simple_record):
        """Test metadata annotation with custom extraction time."""
        # Define a specific extraction time
        transmog_time = datetime.datetime(2023, 1, 1, 12, 0, 0)

        # Annotate with custom time
        annotated = annotate_with_metadata(simple_record, transmog_time=transmog_time)

        # Check that time field uses the custom time
        assert "__transmog_datetime" in annotated

        # Extract time might be stored as string or datetime object
        if isinstance(annotated["__transmog_datetime"], str):
            # If string, should contain the year at minimum
            assert "2023" in annotated["__transmog_datetime"]
        else:
            # If datetime, should match the provided time
            assert annotated["__transmog_datetime"] == transmog_time

    def test_deterministic_id_generation(self):
        """Test deterministic ID generation."""
        # Use a record without natural ID field
        record = {"name": "Test Record", "value": 42, "code": "ABC123"}

        # Generate IDs using the same source field and record
        id1 = generate_transmog_id(record, source_field="code")
        id2 = generate_transmog_id(record, source_field="code")

        # IDs should be identical when using the same source field
        assert id1 == id2

        # Generate ID with a different source field
        id3 = generate_transmog_id(record, source_field="name")

        # ID should be different with a different source field
        assert id1 != id3

        # Test with a non-existent source field (should use random UUID)
        id4 = generate_transmog_id(record, source_field="non_existent")
        id5 = generate_transmog_id(record, source_field="non_existent")

        # IDs should be different when source field doesn't exist
        assert id4 != id5

    def test_metadata_field_names(self):
        """Test standard metadata field names."""
        # Check standard field names in a record without natural ID
        record = annotate_with_metadata({"test": "value"})

        # Should have transmog ID since no natural ID exists
        assert "__transmog_id" in record
        assert "__transmog_datetime" in record

        # Check record with natural ID
        record_with_id = annotate_with_metadata({"id": "123", "test": "value"})

        # Should not have transmog ID since natural ID exists
        assert "__transmog_id" not in record_with_id
        assert "__transmog_datetime" in record_with_id

    def test_custom_id_generation_strategy(self):
        """Test custom ID generation strategy."""
        # Use a record without natural ID
        record = {"name": "Test", "value": 42}

        # Define a custom ID generation function
        def custom_id_generator(record):
            return f"CUSTOM-{record.get('name', 'UNKNOWN')}"

        # Annotate with custom ID generator
        annotated = annotate_with_metadata(
            record, id_generation_strategy=custom_id_generator
        )

        # Check that ID uses the custom format
        assert "__transmog_id" in annotated
        assert annotated["__transmog_id"] == f"CUSTOM-{record['name']}"

    def test_generate_deterministic_id(self):
        """Test generating a deterministic ID from a value."""
        # Generate IDs from the same value
        value = "test-value"
        id1 = generate_deterministic_id(value)
        id2 = generate_deterministic_id(value)

        # IDs should be identical
        assert id1 == id2

        # IDs should be consistent regardless of whitespace or case
        id3 = generate_deterministic_id(" test-value ")
        id4 = generate_deterministic_id("TEST-value")

        # Should normalize whitespace and case
        assert id1 == id3
        assert id1 == id4

        # Different values should produce different IDs
        id5 = generate_deterministic_id("different-value")
        assert id1 != id5
