"""
Tests for ID discovery functionality.

Tests natural ID field discovery and ID extraction from records.
"""

from typing import Any, Optional

import pytest

from transmog.core.id_discovery import get_record_id


class TestGetRecordId:
    """Test record ID extraction functionality."""

    def test_get_record_id_simple(self):
        """Test getting ID from record with simple ID field."""
        record = {"id": 123, "name": "test"}
        field_name, id_value = get_record_id(record)
        assert field_name == "id"
        assert id_value == 123

    def test_get_record_id_string(self):
        """Test getting string ID from record."""
        record = {"id": "abc123", "name": "test"}
        field_name, id_value = get_record_id(record)
        assert field_name == "id"
        assert id_value == "abc123"

    def test_get_record_id_uuid(self):
        """Test getting UUID from record."""
        record = {"uuid": "550e8400-e29b-41d4-a716-446655440000", "name": "test"}
        field_name, id_value = get_record_id(record)
        assert field_name == "uuid"
        assert id_value == "550e8400-e29b-41d4-a716-446655440000"

    def test_get_record_id_no_id_field(self):
        """Test getting ID from record with no ID field."""
        record = {"name": "test", "value": "data"}
        field_name, id_value = get_record_id(record)
        assert field_name is None
        assert id_value is None

    def test_get_record_id_with_custom_patterns(self):
        """Test getting ID with custom patterns."""
        record = {"custom_id": 999, "name": "test"}
        field_name, id_value = get_record_id(record, id_patterns=["custom_id"])
        assert field_name == "custom_id"
        assert id_value == 999

    def test_get_record_id_none_value(self):
        """Test getting ID when value is None."""
        record = {"id": None, "name": "test"}
        field_name, id_value = get_record_id(record)
        assert field_name is None
        assert id_value is None

    def test_get_record_id_zero_value(self):
        """Test getting ID when value is zero."""
        record = {"id": 0, "name": "test"}
        field_name, id_value = get_record_id(record)
        assert field_name == "id"
        assert id_value == 0

    def test_get_record_id_empty_string(self):
        """Test getting ID when value is empty string."""
        record = {"id": "", "name": "test"}
        field_name, id_value = get_record_id(record)
        assert field_name is None
        assert id_value is None

    def test_get_record_id_complex_value(self):
        """Test getting ID when value is complex object."""
        record = {"id": {"nested": "value"}, "name": "test"}
        field_name, id_value = get_record_id(record)
        assert field_name is None
        assert id_value is None

    def test_get_record_id_with_fallback(self):
        """Test getting ID with fallback field."""
        record = {"name": "test", "__transmog_id": "fallback-id"}
        field_name, id_value = get_record_id(record, fallback_field="__transmog_id")
        assert field_name == "__transmog_id"
        assert id_value == "fallback-id"


class TestShouldAddTransmogId:
    """Test logic for determining when to add transmog ID."""

    def test_should_add_with_natural_id(self):
        """Test should not add transmog ID when natural ID exists."""
        record = {"id": 123, "name": "test"}
        field, _ = get_record_id(record)
        should_add = field is None
        assert should_add is False

    def test_should_add_without_natural_id(self):
        """Test should add transmog ID when no natural ID exists."""
        record = {"name": "test", "value": "data"}
        field, _ = get_record_id(record)
        should_add = field is None
        assert should_add is True

    def test_should_add_with_custom_patterns(self):
        """Test should add logic with custom ID patterns."""
        record = {"custom_id": 999, "name": "test"}

        # Should add with default patterns (no custom_id recognized)
        field, _ = get_record_id(record)
        should_add = field is None
        assert should_add is True

        # Should not add with custom patterns
        field, _ = get_record_id(record, id_patterns=["custom_id"])
        should_add = field is None
        assert should_add is False


class TestIdDiscoveryIntegration:
    """Test integration of ID discovery functionality."""

    def test_discovery_workflow(self):
        """Test complete ID discovery workflow."""
        record_with_id = {"user_id": 123, "name": "John", "email": "john@example.com"}

        field_name, id_value = get_record_id(record_with_id, id_patterns=["user_id"])
        assert field_name == "user_id"
        assert id_value == 123

        field, _ = get_record_id(record_with_id, id_patterns=["user_id"])
        should_add = field is None
        assert should_add is False

    def test_discovery_fallback_behavior(self):
        """Test fallback behavior when no natural ID found."""
        record = {"name": "test", "description": "no ID field"}

        field_name, id_value = get_record_id(record)
        assert field_name is None
        assert id_value is None

        field, _ = get_record_id(record)
        should_add = field is None
        assert should_add is True

    def test_discovery_with_multiple_candidates(self):
        """Test discovery when multiple ID candidates exist."""
        record = {"id": 1, "uuid": "test-uuid", "pk": 2, "key": "test-key"}

        field_name, id_value = get_record_id(record)
        assert field_name == "id"
        assert id_value == 1
