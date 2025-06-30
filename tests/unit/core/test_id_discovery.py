"""
Tests for ID discovery functionality.

Tests natural ID field discovery and ID extraction from records.
"""

import pytest
from typing import Dict, Any, Optional, List

from transmog.core.id_discovery import (
    DEFAULT_ID_FIELD_PATTERNS,
    discover_id_field,
    get_record_id,
    should_add_transmog_id,
    build_id_field_mapping,
)


class TestDiscoverIdField:
    """Test ID field discovery functionality."""

    def test_discover_with_default_patterns(self):
        """Test discovery using default ID field patterns."""
        # Record with standard 'id' field
        record = {"id": 123, "name": "test", "value": "data"}
        discovered = discover_id_field(record)
        assert discovered == "id"

        # Record with 'uuid' field
        record = {"uuid": "550e8400-e29b-41d4-a716-446655440000", "name": "test"}
        discovered = discover_id_field(record)
        assert discovered == "uuid"

        # Record with no ID field
        record = {"name": "test", "value": "data"}
        discovered = discover_id_field(record)
        assert discovered is None

    def test_discover_with_custom_patterns(self):
        """Test discovery using custom ID field patterns."""
        record = {"custom_id": 999, "name": "test"}

        # Should not find with default patterns
        discovered = discover_id_field(record)
        assert discovered is None

        # Should find with custom patterns
        discovered = discover_id_field(record, id_field_patterns=["custom_id"])
        assert discovered == "custom_id"

    def test_discover_with_empty_patterns(self):
        """Test discovery with empty patterns list."""
        record = {"id": 123, "name": "test"}
        discovered = discover_id_field(record, id_field_patterns=[])
        assert discovered is None

    def test_discover_with_path_mapping(self):
        """Test discovery with path-specific ID field mapping."""
        record = {"special_key": 456, "name": "test"}

        # Test path-specific mapping
        mapping = {"users": "special_key"}
        discovered = discover_id_field(record, path="users", id_field_mapping=mapping)
        assert discovered == "special_key"

        # Test wildcard mapping
        mapping = {"*": "special_key"}
        discovered = discover_id_field(
            record, path="anything", id_field_mapping=mapping
        )
        assert discovered == "special_key"

    def test_discover_priority_order(self):
        """Test that discovery follows priority order."""
        # Record with multiple potential ID fields
        record = {"pk": 1, "id": 2, "uuid": "test-uuid"}

        # Should prefer 'id' over others based on default patterns
        discovered = discover_id_field(record)
        assert discovered == "id"

    def test_discover_with_null_values(self):
        """Test discovery with null/empty values."""
        # Null value should be skipped
        record = {"id": None, "uuid": "valid-uuid"}
        discovered = discover_id_field(record)
        assert discovered == "uuid"

        # Empty string should be skipped
        record = {"id": "", "uuid": "valid-uuid"}
        discovered = discover_id_field(record)
        assert discovered == "uuid"

    def test_discover_with_invalid_record_types(self):
        """Test discovery with invalid record types."""
        # Non-dict should return None
        assert discover_id_field(None) is None
        assert discover_id_field("string") is None
        assert discover_id_field(123) is None
        assert discover_id_field([]) is None


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
        field_name, id_value = get_record_id(record, id_field_patterns=["custom_id"])
        assert field_name == "custom_id"
        assert id_value == 999

    def test_get_record_id_with_mapping(self):
        """Test getting ID with path mapping."""
        record = {"special_key": 456, "name": "test"}
        mapping = {"users": "special_key"}
        field_name, id_value = get_record_id(
            record, path="users", id_field_mapping=mapping
        )
        assert field_name == "special_key"
        assert id_value == 456

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
        should_add = should_add_transmog_id(record)
        assert should_add is False

    def test_should_add_without_natural_id(self):
        """Test should add transmog ID when no natural ID exists."""
        record = {"name": "test", "value": "data"}
        should_add = should_add_transmog_id(record)
        assert should_add is True

    def test_should_add_with_force_flag(self):
        """Test should add transmog ID when force flag is True."""
        record = {"id": 123, "name": "test"}
        should_add = should_add_transmog_id(record, force_transmog_id=True)
        assert should_add is True

    def test_should_add_with_custom_patterns(self):
        """Test should add logic with custom ID patterns."""
        record = {"custom_id": 999, "name": "test"}

        # Should add with default patterns (no custom_id recognized)
        should_add = should_add_transmog_id(record)
        assert should_add is True

        # Should not add with custom patterns
        should_add = should_add_transmog_id(record, id_field_patterns=["custom_id"])
        assert should_add is False


class TestBuildIdFieldMapping:
    """Test ID field mapping construction."""

    def test_build_mapping_from_dict(self):
        """Test building mapping from dictionary config."""
        config = {
            "id_field_mapping": {
                "users": "user_id",
                "orders": "order_number",
                "products": "product_code",
            }
        }

        mapping = build_id_field_mapping(config)
        expected = {
            "users": "user_id",
            "orders": "order_number",
            "products": "product_code",
        }
        assert mapping == expected

    def test_build_mapping_from_list(self):
        """Test building mapping from simple field name."""
        config = {"natural_id_field": "entity_id"}

        mapping = build_id_field_mapping(config)
        expected = {"*": "entity_id"}
        assert mapping == expected

    def test_build_mapping_from_empty_dict(self):
        """Test building mapping from empty config."""
        config = {}
        mapping = build_id_field_mapping(config)
        assert mapping is None

    def test_build_mapping_from_empty_list(self):
        """Test building mapping from None config."""
        mapping = build_id_field_mapping(None)
        assert mapping is None

    def test_build_mapping_invalid_input(self):
        """Test building mapping with invalid input types."""
        # Invalid mapping type
        config = {"id_field_mapping": "invalid"}
        mapping = build_id_field_mapping(config)
        assert mapping is None

        # Invalid natural field type
        config = {"natural_id_field": 123}
        mapping = build_id_field_mapping(config)
        assert mapping is None

    def test_build_mapping_invalid_list_format(self):
        """Test building mapping with invalid format."""
        # Test that function handles invalid input gracefully
        config = {"id_field_mapping": ["invalid", "format"]}
        mapping = build_id_field_mapping(config)
        assert mapping is None


class TestIdDiscoveryIntegration:
    """Test integration of ID discovery functionality."""

    def test_discovery_workflow(self):
        """Test complete ID discovery workflow."""
        # Record with natural ID
        record_with_id = {"user_id": 123, "name": "John", "email": "john@example.com"}

        # Discover ID field
        id_field = discover_id_field(record_with_id, id_field_patterns=["user_id"])
        assert id_field == "user_id"

        # Get record ID
        field_name, id_value = get_record_id(
            record_with_id, id_field_patterns=["user_id"]
        )
        assert field_name == "user_id"
        assert id_value == 123

        # Should not add transmog ID
        should_add = should_add_transmog_id(
            record_with_id, id_field_patterns=["user_id"]
        )
        assert should_add is False

    def test_discovery_with_mapping_workflow(self):
        """Test ID discovery with path-specific mapping."""
        record = {"order_number": "ORD-123", "customer": "John"}
        mapping = {"orders": "order_number"}

        # Discover with mapping
        id_field = discover_id_field(record, path="orders", id_field_mapping=mapping)
        assert id_field == "order_number"

        # Get ID with mapping
        field_name, id_value = get_record_id(
            record, path="orders", id_field_mapping=mapping
        )
        assert field_name == "order_number"
        assert id_value == "ORD-123"

    def test_discovery_fallback_behavior(self):
        """Test fallback behavior when no natural ID found."""
        record = {"name": "test", "description": "no ID field"}

        # No ID discovered
        id_field = discover_id_field(record)
        assert id_field is None

        # No ID retrieved
        field_name, id_value = get_record_id(record)
        assert field_name is None
        assert id_value is None

        # Should add transmog ID
        should_add = should_add_transmog_id(record)
        assert should_add is True

    def test_discovery_with_multiple_candidates(self):
        """Test discovery when multiple ID candidates exist."""
        record = {"id": 1, "uuid": "test-uuid", "pk": 2, "key": "test-key"}

        # Should select based on priority (id comes first in default patterns)
        id_field = discover_id_field(record)
        assert id_field == "id"

        field_name, id_value = get_record_id(record)
        assert field_name == "id"
        assert id_value == 1
