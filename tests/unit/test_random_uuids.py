"""
Unit tests for random UUID generation.

These tests verify random UUID generation behavior, including
predictability, stability and various edge cases.
"""

import uuid
import re
import pytest

from transmog import Processor, TransmogConfig
from transmog.core.metadata import generate_extract_id


def is_valid_uuid(id_str):
    """Check if a string is a valid UUID format."""
    uuid_pattern = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )
    return uuid_pattern.match(id_str) is not None


class TestRandomUuids:
    """Unit tests for random UUID generation."""

    def test_random_uuid_format(self):
        """Test that random UUIDs are correctly formatted."""
        # Generate random IDs
        id1 = generate_extract_id()
        id2 = generate_extract_id()

        # Check UUID format (8-4-4-4-12 hexadecimal format)
        uuid_pattern = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        )

        assert uuid_pattern.match(id1) is not None
        assert uuid_pattern.match(id2) is not None

        # Verify that UUIDs are different
        assert id1 != id2

    def test_random_uuid_uniqueness(self):
        """Test that random UUIDs are unique."""
        # Generate a large number of IDs
        ids = [generate_extract_id() for _ in range(1000)]

        # Check that all IDs are unique
        unique_ids = set(ids)
        assert len(unique_ids) == len(ids)

    def test_random_uuid_distribution(self):
        """Test the distribution of random UUIDs."""
        # Generate a large number of UUIDs
        uuids = [generate_extract_id() for _ in range(1000)]

        # Count occurrences of the first character in each UUID
        first_chars = [uid[0] for uid in uuids]
        char_counts = {}

        for char in first_chars:
            if char in char_counts:
                char_counts[char] += 1
            else:
                char_counts[char] = 1

        # Basic distribution check - all hexadecimal characters should have roughly
        # equal representation (though with only 1000 samples, there will be some variance)
        for char in "0123456789abcdef":
            # Each character should appear roughly 1000/16 = 62.5 times
            # Allow for statistical variation (4 standard deviations)
            # With binomial distribution, stddev = sqrt(np(1-p))
            # where n = 1000 and p = 1/16 = 0.0625
            expected = 1000 / 16
            stddev = (1000 * (1 / 16) * (1 - 1 / 16)) ** 0.5

            # If the character appears in the distribution, check it's within reasonable bounds
            if char in char_counts:
                assert char_counts[char] >= expected - 4 * stddev
                assert char_counts[char] <= expected + 4 * stddev

    def test_processor_with_random_uuids(self):
        """Test that processor generates different random UUIDs for identical data."""
        data = {
            "name": "Test Item",
            "value": 42,
            "nested": {"field1": "value1", "field2": "value2"},
            "array": [1, 2, 3, 4, 5],
        }

        # Create a processor with no deterministic ID fields
        processor = Processor()

        # Process the same data multiple times
        result1 = processor.process(data, entity_name="test")
        result2 = processor.process(data, entity_name="test")

        # Extract IDs
        id1 = result1.to_dict()["main_table"][0]["__extract_id"]
        id2 = result2.to_dict()["main_table"][0]["__extract_id"]

        # Verify different IDs
        assert id1 != id2, "Generated UUIDs should be different for identical data"

        # Verify IDs follow UUID format
        uuid_pattern = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        )
        assert uuid_pattern.match(id1) is not None
        assert uuid_pattern.match(id2) is not None

    def test_random_uuids_with_array_data(self):
        """Test random UUID generation with array data."""
        # Create array data
        data = [
            {"name": "Item 1", "value": 42},
            {"name": "Item 2", "value": 43},
            {"name": "Item 3", "value": 44},
        ]

        # Create a processor with no deterministic ID fields
        processor = Processor()

        # Process the data
        result = processor.process(data, entity_name="items")
        tables = result.to_dict()

        # Get the main table records
        main_records = tables["main_table"]

        # Verify each record has a random UUID
        assert len(main_records) == 3
        for record in main_records:
            assert "__extract_id" in record
            assert is_valid_uuid(record["__extract_id"])

        # Verify all IDs are unique
        ids = [record["__extract_id"] for record in main_records]
        assert len(set(ids)) == len(ids), "All UUIDs should be unique"

    def test_mixed_deterministic_and_random_ids(self):
        """Test mixed deterministic and random ID generation."""
        # Create data with nested structures
        data = {
            "id": "ROOT001",  # For deterministic ID at root
            "items": [
                {"name": "Item 1"},  # For random ID (no deterministic field)
                {"name": "Item 2"},  # For random ID (no deterministic field)
            ],
            "products": [
                {"sku": "PROD001", "name": "Product 1"},  # For deterministic ID
                {"sku": "PROD002", "name": "Product 2"},  # For deterministic ID
            ],
        }

        # Create deterministic ID fields for specific paths
        deterministic_id_fields = {
            "": "id",  # Root level uses 'id' field
            "products": "sku",  # Products array uses 'sku' field
        }

        # Create processor with deterministic ID fields
        config = TransmogConfig.default().with_metadata(
            deterministic_id_fields=deterministic_id_fields
        )
        processor = Processor(config=config)

        # Process the data multiple times
        result1 = processor.process(data, entity_name="store")
        result2 = processor.process(data, entity_name="store")

        tables1 = result1.to_dict()
        tables2 = result2.to_dict()

        # Check deterministic IDs - should be the same across runs
        # Root record
        assert (
            tables1["main_table"][0]["__extract_id"]
            == tables2["main_table"][0]["__extract_id"]
        )

        # Products table - should have deterministic IDs
        products1 = sorted(tables1.get("store_products", []), key=lambda x: x["sku"])
        products2 = sorted(tables2.get("store_products", []), key=lambda x: x["sku"])

        for i in range(len(products1)):
            assert products1[i]["__extract_id"] == products2[i]["__extract_id"]

        # Items table - should have random IDs
        items1 = sorted(tables1.get("store_items", []), key=lambda x: x["name"])
        items2 = sorted(tables2.get("store_items", []), key=lambda x: x["name"])

        for i in range(len(items1)):
            # IDs should be different
            assert items1[i]["__extract_id"] != items2[i]["__extract_id"]

            # Both should follow UUID format
            uuid_pattern = re.compile(
                r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
            )
            assert uuid_pattern.match(items1[i]["__extract_id"]) is not None
            assert uuid_pattern.match(items2[i]["__extract_id"]) is not None

    def test_wildcard_path_with_random_ids(self):
        """Test wildcard paths with random ID generation."""
        # Create data with nested structures
        data = {
            "id": "ROOT001",
            "level1": {
                "level2": {"items": [{"name": "Deep Item 1"}, {"name": "Deep Item 2"}]}
            },
            "items": [{"name": "Shallow Item 1"}, {"name": "Shallow Item 2"}],
        }

        # Create deterministic ID fields with a wildcard path
        # Only the root level gets deterministic ID
        deterministic_id_fields = {
            "": "id",  # Root level uses 'id' field
        }

        # Create processor with deterministic ID fields
        config = TransmogConfig.default().with_metadata(
            deterministic_id_fields=deterministic_id_fields
        )
        processor = Processor(config=config)

        # Process the data multiple times
        result1 = processor.process(data, entity_name="test")
        result2 = processor.process(data, entity_name="test")

        tables1 = result1.to_dict()
        tables2 = result2.to_dict()

        # Root level should have deterministic ID
        assert (
            tables1["main_table"][0]["__extract_id"]
            == tables2["main_table"][0]["__extract_id"]
        )

        # All nested arrays should have random IDs
        # Get table names excluding "main_table"
        array_tables1 = [name for name in tables1["child_tables"].keys()]
        array_tables2 = [name for name in tables2["child_tables"].keys()]

        # Check each array table
        for table_name in array_tables1:
            if table_name in array_tables2:
                # Ensure we're dealing with lists of records, not strings
                if isinstance(tables1["child_tables"][table_name], list) and isinstance(
                    tables2["child_tables"][table_name], list
                ):
                    items1 = sorted(
                        tables1["child_tables"][table_name],
                        key=lambda x: x.get("name", ""),
                    )
                    items2 = sorted(
                        tables2["child_tables"][table_name],
                        key=lambda x: x.get("name", ""),
                    )

                    # Verify each record has a different random ID across runs
                    for i in range(min(len(items1), len(items2))):
                        assert items1[i]["__extract_id"] != items2[i]["__extract_id"]

    def test_uuid_version_and_variant(self):
        """Test that UUIDs have correct version and variant."""
        # Generate a random UUID
        random_id = generate_extract_id()

        # Parse as UUID object
        uuid_obj = uuid.UUID(random_id)

        # Verify it's UUID version 4 (random)
        assert uuid_obj.version == 4

        # Verify variant is RFC 4122
        assert uuid_obj.variant == uuid.RFC_4122

    def test_uuid_with_custom_namespace(self):
        """Test deterministic UUID generation with custom namespace."""
        # Use a custom namespace UUID
        custom_namespace = uuid.uuid4()

        # Generate deterministic IDs with custom namespace
        value = "test_value"
        id1 = str(uuid.uuid5(custom_namespace, value))
        id2 = str(uuid.uuid5(custom_namespace, value))

        # IDs should be the same
        assert id1 == id2

        # ID should match UUID5 pattern
        uuid_obj = uuid.UUID(id1)
        assert uuid_obj.version == 5
        assert uuid_obj.variant == uuid.RFC_4122
