"""Tests for ID strategy functionality.

Tests ID generation strategies including auto, random, natural, hash, and composite.
"""

import pytest

import transmog as tm
from transmog.config import TransmogConfig
from transmog.exceptions import ValidationError
from transmog.flattening import generate_transmog_id


class TestIdStrategyRandom:
    """Test random ID strategy."""

    def test_random_generates_uuid(self):
        """Random strategy always generates UUID."""
        record = {"name": "test"}
        result = generate_transmog_id(record, "random", "_id")
        assert isinstance(result, str)
        assert len(result) == 36

    def test_random_ignores_existing(self):
        """Random strategy ignores existing ID."""
        record = {"_id": "existing-123", "name": "test"}
        result = generate_transmog_id(record, "random", "_id")
        assert isinstance(result, str)
        assert result != "existing-123"

    def test_random_generates_different_ids(self):
        """Random strategy generates different IDs."""
        record1 = {"name": "test1"}
        record2 = {"name": "test2"}
        id1 = generate_transmog_id(record1, "random", "_id")
        id2 = generate_transmog_id(record2, "random", "_id")
        assert id1 != id2


class TestIdStrategyNatural:
    """Test natural ID strategy."""

    def test_natural_uses_existing_id(self):
        """Natural strategy uses existing ID."""
        record = {"_id": "natural-123", "name": "test"}
        result = generate_transmog_id(record, "natural", "_id")
        assert result is None
        assert record["_id"] == "natural-123"

    def test_natural_fails_when_missing(self):
        """Natural strategy fails when field missing."""
        record = {"name": "test"}
        with pytest.raises(ValidationError, match="requires field '_id'"):
            generate_transmog_id(record, "natural", "_id")

    def test_natural_fails_when_empty(self):
        """Natural strategy fails when field is empty."""
        record = {"_id": "", "name": "test"}
        with pytest.raises(ValidationError, match="requires non-empty"):
            generate_transmog_id(record, "natural", "_id")

    def test_natural_fails_when_none(self):
        """Natural strategy fails when field is None."""
        record = {"_id": None, "name": "test"}
        with pytest.raises(ValidationError, match="requires non-empty"):
            generate_transmog_id(record, "natural", "_id")

    def test_natural_with_custom_field(self):
        """Natural strategy works with custom field name."""
        record = {"product_id": "prod-456", "name": "widget"}
        result = generate_transmog_id(record, "natural", "product_id")
        assert result is None
        assert record["product_id"] == "prod-456"


class TestIdStrategyHash:
    """Test hash ID strategy."""

    def test_hash_generates_deterministic_id(self):
        """Hash strategy generates deterministic ID."""
        record = {"name": "test", "value": 123}
        id1 = generate_transmog_id(record, "hash", "_id")
        id2 = generate_transmog_id(record, "hash", "_id")
        assert id1 == id2

    def test_hash_different_for_different_records(self):
        """Hash strategy generates different IDs for different records."""
        record1 = {"name": "test1"}
        record2 = {"name": "test2"}
        id1 = generate_transmog_id(record1, "hash", "_id")
        id2 = generate_transmog_id(record2, "hash", "_id")
        assert id1 != id2

    def test_hash_same_content_same_id(self):
        """Hash strategy generates same ID for same content."""
        record1 = {"name": "test", "value": 123}
        record2 = {"value": 123, "name": "test"}
        id1 = generate_transmog_id(record1, "hash", "_id")
        id2 = generate_transmog_id(record2, "hash", "_id")
        assert id1 == id2

    def test_hash_returns_valid_uuid(self):
        """Hash strategy returns valid UUID format."""
        record = {"name": "test"}
        result = generate_transmog_id(record, "hash", "_id")
        assert len(result) == 36
        assert result.count("-") == 4


class TestIdStrategyComposite:
    """Test composite ID strategy (list of fields)."""

    def test_composite_generates_from_fields(self):
        """Composite strategy generates ID from specific fields."""
        record = {"user_id": 123, "timestamp": "2025-01-01", "extra": "ignored"}
        id1 = generate_transmog_id(record, ["user_id", "timestamp"], "_id")
        id2 = generate_transmog_id(record, ["user_id", "timestamp"], "_id")
        assert id1 == id2

    def test_composite_different_fields_different_id(self):
        """Composite strategy generates different ID with different fields."""
        record = {"user_id": 123, "timestamp": "2025-01-01"}
        id1 = generate_transmog_id(record, ["user_id"], "_id")
        id2 = generate_transmog_id(record, ["user_id", "timestamp"], "_id")
        assert id1 != id2

    def test_composite_handles_missing_fields(self):
        """Composite strategy handles missing fields."""
        record = {"user_id": 123}
        id1 = generate_transmog_id(record, ["user_id", "missing"], "_id")
        assert isinstance(id1, str)
        assert len(id1) == 36

    def test_composite_order_independent(self):
        """Composite strategy ignores field order."""
        record = {"a": 1, "b": 2}
        id1 = generate_transmog_id(record, ["a", "b"], "_id")
        id2 = generate_transmog_id(record, ["b", "a"], "_id")
        assert id1 == id2

    def test_composite_returns_valid_uuid(self):
        """Composite strategy returns valid UUID format."""
        record = {"field1": "value1", "field2": "value2"}
        result = generate_transmog_id(record, ["field1", "field2"], "_id")
        assert len(result) == 36
        assert result.count("-") == 4


class TestInvalidStrategy:
    """Test invalid strategy handling."""

    def test_invalid_string_strategy(self):
        """Unknown strategy raises ValidationError."""
        with pytest.raises(ValidationError, match="Invalid id_generation"):
            generate_transmog_id({"name": "test"}, "invalid", "_id")

    def test_valid_strategies_return_expected_types(self):
        """Each valid strategy returns its documented type."""
        record = {"_id": "123", "name": "test"}

        random_id = generate_transmog_id(record, "random", "_id")
        assert isinstance(random_id, str)
        assert len(random_id) == 36

        hash_id = generate_transmog_id(record, "hash", "_id")
        assert isinstance(hash_id, str)
        assert len(hash_id) == 36

        natural_id = generate_transmog_id(record, "natural", "_id")
        assert natural_id is None


class TestNaturalIdEdgeCases:
    """Test edge cases for natural ID strategy."""

    def test_natural_id_field_name_conflict(self):
        """Natural strategy uses id_field even when another id column exists."""
        data = {"_id": "natural-id-value", "id": "other-id", "name": "test"}
        result = tm.flatten(
            data, name="test", config=TransmogConfig(id_generation="natural")
        )
        assert result.main[0]["_id"] == "natural-id-value"
        assert result.main[0]["id"] == "other-id"
