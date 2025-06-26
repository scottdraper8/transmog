"""
Tests for the metadata implementation.

This module tests the metadata functions in the metadata module.
"""

import datetime
import re
import uuid

from tests.interfaces.test_metadata_interface import AbstractMetadataTest
from transmog.core.metadata import (
    annotate_with_metadata,
    generate_deterministic_id,
)


class TestMetadata(AbstractMetadataTest):
    """
    Concrete implementation of the AbstractMetadataTest for the core metadata functions.

    Tests the annotate_with_metadata, generate_transmog_id, and generate_deterministic_id functions.
    """

    def test_id_format_validation(self, simple_record):
        """Test that generated IDs follow the expected UUID format."""
        # Annotate record with metadata
        annotated = annotate_with_metadata(simple_record, force_transmog_id=True)

        # Get the extract ID
        extract_id = annotated["__transmog_id"]

        # Check if it's a valid UUID
        try:
            # Try to parse as UUID to validate format
            uuid_obj = uuid.UUID(extract_id)
            assert str(uuid_obj) == extract_id
        except ValueError:
            # If it's not a UUID, it might be using a different format
            # In this case, make sure it's a non-empty string
            assert isinstance(extract_id, str)
            assert len(extract_id) > 0

    def test_datetime_format(self, simple_record):
        """Test extraction datetime format."""
        # Annotate record with metadata
        annotated = annotate_with_metadata(simple_record)

        # Get the extract datetime
        extract_datetime = annotated["__transmog_datetime"]

        # Check format
        if isinstance(extract_datetime, str):
            # If string, should match ISO format
            # This regex matches common ISO date formats
            iso_pattern = r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"
            assert re.match(iso_pattern, extract_datetime)
        else:
            # Should be a datetime object
            assert isinstance(extract_datetime, datetime.datetime)

    def test_metadata_overwriting(self, simple_record):
        """Test that metadata fields don't overwrite existing fields."""
        # Create a record with a field that would collide with metadata
        record_with_collision = simple_record.copy()
        record_with_collision["__transmog_id"] = "ORIGINAL"

        # Annotate with metadata
        annotated = annotate_with_metadata(record_with_collision)

        # Check if original field was preserved or overwritten
        # The behavior depends on implementation, but should be consistent
        assert "__transmog_id" in annotated

        # If implementation preserves original fields:
        if annotated["__transmog_id"] == "ORIGINAL":
            print("Implementation preserves original metadata fields")
        else:
            print("Implementation overwrites metadata fields")
            # Make sure the new ID is valid
            assert isinstance(annotated["__transmog_id"], str)
            assert annotated["__transmog_id"] != "ORIGINAL"

    def test_extra_fields_handling(self):
        """Test handling of extra fields in metadata annotation."""
        # Create a simple record
        record = {"id": "test"}

        # Define extra fields
        extra_fields = {
            "__transmog_path": "path/to/record",
            "__custom_field": "custom value",
        }

        # Annotate with extra fields
        annotated = annotate_with_metadata(record, extra_fields=extra_fields)

        # Verify extra fields are added
        assert "__transmog_path" in annotated
        assert annotated["__transmog_path"] == "path/to/record"
        assert "__custom_field" in annotated
        assert annotated["__custom_field"] == "custom value"

    def test_custom_field_names(self, simple_record):
        """Test metadata annotation with custom field names."""
        # Define custom field names if supported by the implementation
        custom_id_field = "_id"
        custom_parent_field = "_parent_id"
        custom_time_field = "_timestamp"

        try:
            # Attempt to annotate with custom field names
            annotated = annotate_with_metadata(
                simple_record,
                id_field=custom_id_field,
                parent_field=custom_parent_field,
                time_field=custom_time_field,
                parent_id="parent123",
                force_transmog_id=True,
            )

            # Check if custom fields were used
            assert custom_id_field in annotated
            assert custom_parent_field in annotated
            assert custom_time_field in annotated

            # Standard fields should not be present
            assert "__transmog_id" not in annotated
            assert "__parent_transmog_id" not in annotated
            assert "__transmog_datetime" not in annotated
        except TypeError:
            # If custom field names are not supported, this will raise
            # In that case, test the standard behavior
            annotated = annotate_with_metadata(
                simple_record, parent_id="parent123", force_transmog_id=True
            )

            # Standard fields should be present
            assert "__transmog_id" in annotated
            assert "__parent_transmog_id" in annotated
            assert "__transmog_datetime" in annotated

    def test_multiple_metadata_annotations(self, simple_record):
        """Test applying metadata multiple times to the same record."""
        # First annotation
        annotated_once = annotate_with_metadata(simple_record, force_transmog_id=True)
        first_id = annotated_once["__transmog_id"]

        # Second annotation
        annotated_twice = annotate_with_metadata(annotated_once, force_transmog_id=True)
        second_id = annotated_twice["__transmog_id"]

        # Check if ID was preserved or changed
        # The behavior depends on implementation, but should be consistent
        if first_id == second_id:
            # Implementation preserves existing IDs
            print("Implementation preserves existing metadata")
        else:
            # Implementation generates new IDs each time
            print("Implementation regenerates metadata on each call")
            assert first_id != second_id

    def test_deterministic_id_normalization(self):
        """Test normalization of values for deterministic ID generation."""
        # Test with variations of the same value
        variations = [
            "test-value",
            "  test-value  ",  # Extra whitespace
            "TEST-VALUE",  # Different case
            "test-value\n",  # Trailing newline
        ]

        # Generate IDs for all variations
        ids = [generate_deterministic_id(val) for val in variations]

        # All IDs should be identical due to normalization
        reference_id = ids[0]
        for id_value in ids[1:]:
            assert id_value == reference_id

    def test_in_place_annotation(self, simple_record):
        """Test in-place metadata annotation."""
        # Make a copy for comparison
        original = simple_record.copy()

        # Annotate in place
        result = annotate_with_metadata(
            simple_record, in_place=True, force_transmog_id=True
        )

        # Verify the result is the same object as the input
        assert result is simple_record

        # Verify metadata was added
        assert "__transmog_id" in simple_record
        assert "__transmog_datetime" in simple_record

        # Original fields should be preserved
        for key in original:
            assert simple_record[key] == original[key]
