"""Tests for natural ID discovery functionality.

This module tests the functionality for discovering and using
natural ID fields in data records.
"""

import pytest

from transmog.core.id_discovery import (
    DEFAULT_ID_FIELD_PATTERNS,
    _is_valid_id_value,
    build_id_field_mapping,
    discover_id_field,
    get_record_id,
    should_add_transmog_id,
)


class TestIdDiscovery:
    """Tests for ID discovery functionality."""

    def test_discover_id_field_default_patterns(self):
        """Test discovering ID field using default patterns."""
        # Test with default ID field
        record = {"id": "123", "name": "Test"}
        assert discover_id_field(record) == "id"

        # Test with uppercase ID
        record = {"ID": "123", "name": "Test"}
        assert discover_id_field(record) == "ID"

        # Test with uuid
        record = {"uuid": "123-456", "name": "Test"}
        assert discover_id_field(record) == "uuid"

    def test_discover_id_field_custom_patterns(self):
        """Test discovering ID field using custom patterns."""
        # Test with custom patterns
        record = {"code": "ABC123", "name": "Test"}
        patterns = ["code", "ref"]
        assert discover_id_field(record, id_field_patterns=patterns) == "code"

        # Test with custom patterns in different order
        record = {"ref": "REF001", "code": "ABC123", "name": "Test"}
        patterns = ["ref", "code"]
        assert discover_id_field(record, id_field_patterns=patterns) == "ref"

    def test_discover_id_field_path_specific(self):
        """Test discovering ID field using path-specific mapping."""
        # Test with path-specific mapping
        record = {"employee_id": "E123", "name": "Test"}
        mapping = {"employees": "employee_id"}
        assert (
            discover_id_field(record, path="employees", id_field_mapping=mapping)
            == "employee_id"
        )

        # Test with wildcard mapping
        record = {"ref": "R456", "name": "Test"}
        mapping = {"*": "ref"}
        assert (
            discover_id_field(record, path="any_path", id_field_mapping=mapping)
            == "ref"
        )

    def test_discover_id_field_not_found(self):
        """Test when no ID field is found."""
        # Test with no matching field
        record = {"name": "Test", "value": 123}
        assert discover_id_field(record) is None

        # Test with empty record
        record = {}
        assert discover_id_field(record) is None

        # Test with non-dict input
        assert discover_id_field("not a dict") is None
        assert discover_id_field(None) is None

    def test_is_valid_id_value(self):
        """Test validation of ID values."""
        # Valid ID values
        assert _is_valid_id_value("123") is True
        assert _is_valid_id_value(123) is True
        assert _is_valid_id_value(123.45) is True

        # Invalid ID values
        assert _is_valid_id_value(None) is False
        assert _is_valid_id_value("") is False
        assert _is_valid_id_value("   ") is False
        assert _is_valid_id_value([]) is False
        assert _is_valid_id_value({}) is False

    def test_get_record_id(self):
        """Test getting record ID with various configurations."""
        # Test with natural ID
        record = {"id": "123", "name": "Test"}
        field, value = get_record_id(record)
        assert field == "id"
        assert value == "123"

        # Test with custom field patterns
        record = {"code": "ABC", "name": "Test"}
        field, value = get_record_id(record, id_field_patterns=["code"])
        assert field == "code"
        assert value == "ABC"

        # Test with path-specific mapping
        record = {"product_id": "P123", "name": "Test"}
        mapping = {"products": "product_id"}
        field, value = get_record_id(record, path="products", id_field_mapping=mapping)
        assert field == "product_id"
        assert value == "P123"

        # Test with fallback field
        record = {"name": "Test", "__transmog_id": "T123"}
        field, value = get_record_id(record, fallback_field="__transmog_id")
        assert field == "__transmog_id"
        assert value == "T123"

        # Test when no ID found
        record = {"name": "Test"}
        field, value = get_record_id(record)
        assert field is None
        assert value is None

    def test_should_add_transmog_id(self):
        """Test determining if transmog ID should be added."""
        # Test with natural ID present
        record = {"id": "123", "name": "Test"}
        assert should_add_transmog_id(record) is False

        # Test with no natural ID
        record = {"name": "Test"}
        assert should_add_transmog_id(record) is True

        # Test with force_transmog_id=True
        record = {"id": "123", "name": "Test"}
        assert should_add_transmog_id(record, force_transmog_id=True) is True

        # Test with custom patterns
        record = {"code": "ABC", "name": "Test"}
        assert should_add_transmog_id(record, id_field_patterns=["code"]) is False

    def test_build_id_field_mapping(self):
        """Test building ID field mapping from configuration."""
        # Test with direct mapping
        config = {"id_field_mapping": {"users": "user_id", "products": "sku"}}
        mapping = build_id_field_mapping(config)
        assert mapping == {"users": "user_id", "products": "sku"}

        # Test with single field
        config = {"natural_id_field": "id"}
        mapping = build_id_field_mapping(config)
        assert mapping == {"*": "id"}

        # Test with no config
        assert build_id_field_mapping(None) is None
        assert build_id_field_mapping({}) is None
