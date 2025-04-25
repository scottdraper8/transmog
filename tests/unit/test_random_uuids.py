"""
Unit tests for random UUID generation.

These tests verify random UUID generation behavior, including
predictability, stability and various edge cases.
"""

import uuid
import re
import pytest

from src.transmog import Processor
from src.transmog.core.metadata import generate_extract_id


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
        id1 = result1.to_dict()["main"][0]["__extract_id"]
        id2 = result2.to_dict()["main"][0]["__extract_id"]

        # Verify IDs are different
        assert id1 != id2

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
        main_records = tables["main"]

        # Verify each record has a unique ID
        ids = [record["__extract_id"] for record in main_records]
        assert len(ids) == len(set(ids))

        # Verify each ID follows UUID format
        uuid_pattern = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        )
        for id in ids:
            assert uuid_pattern.match(id) is not None

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
        processor = Processor(deterministic_id_fields=deterministic_id_fields)

        # Process the data multiple times
        result1 = processor.process(data, entity_name="store")
        result2 = processor.process(data, entity_name="store")

        tables1 = result1.to_dict()
        tables2 = result2.to_dict()

        # Check deterministic IDs - should be the same across runs
        # Root record
        assert tables1["main"][0]["__extract_id"] == tables2["main"][0]["__extract_id"]

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
        processor = Processor(deterministic_id_fields=deterministic_id_fields)

        # Process the data multiple times
        result1 = processor.process(data, entity_name="test")
        result2 = processor.process(data, entity_name="test")

        tables1 = result1.to_dict()
        tables2 = result2.to_dict()

        # Check that only root has deterministic ID
        assert tables1["main"][0]["__extract_id"] == tables2["main"][0]["__extract_id"]

        # All other tables should have random IDs
        for table_name in tables1.keys():
            if table_name != "main":
                table1 = tables1[table_name]
                table2 = tables2[table_name]

                # Sort by some common field like name if available
                if table1 and "name" in table1[0]:
                    table1 = sorted(table1, key=lambda x: x["name"])
                    table2 = sorted(table2, key=lambda x: x["name"])

                # Check that IDs are different
                for i in range(len(table1)):
                    assert table1[i]["__extract_id"] != table2[i]["__extract_id"]

    def test_uuid_version_and_variant(self):
        """Test that generated UUIDs are Version 4 random UUIDs with correct variant."""
        # Generate multiple UUIDs
        for _ in range(100):
            id_str = generate_extract_id()

            # Convert string to UUID object for inspection
            id_obj = uuid.UUID(id_str)

            # Check UUID version (should be 4 for random UUIDs)
            assert id_obj.version == 4

            # Check UUID variant (should be RFC 4122 variant)
            # For RFC 4122 variant, the most significant bits of the 8th octet
            # should be '10xx' in binary, which is checked by the variant property
            assert id_obj.variant == uuid.RFC_4122

    def test_uuid_with_custom_namespace(self):
        """Test handling of custom namespace parameters if supported."""
        # This is a theoretical test - if your generate_random_id supports
        # custom namespace parameters or other configuration options,
        # those would be tested here.

        # For now, just ensure the basic function works without parameters
        id1 = generate_extract_id()
        assert id1 is not None

        # If your implementation supports custom namespaces, test those here
        # For example:
        # id2 = generate_extract_id(namespace="custom")
        # assert id2 is not None
        # assert id2 != id1
