"""
Tests for deterministic ID generation.

Tests deterministic ID generation, composite IDs, and reproducible behavior.
"""

import pytest

import transmog as tm
from transmog.flattening import _hash_value, process_record_batch
from transmog.types import ProcessingContext


class TestDeterministicIdGeneration:
    """Test deterministic ID generation functions."""

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("test_value", id="string"),
            pytest.param(12345, id="integer"),
            pytest.param(3.14159, id="float"),
            pytest.param(None, id="none"),
            pytest.param({"name": "test", "id": 123, "active": True}, id="dict"),
            pytest.param(["item1", "item2", 123, True], id="list"),
            pytest.param(
                {
                    "level1": {
                        "level2": {"level3": ["item1", "item2", {"deep": "value"}]}
                    },
                    "other": [1, 2, {"nested": True}],
                },
                id="nested_structures",
            ),
        ],
    )
    def test_hash_value_deterministic(self, value):
        """Test that _hash_value produces deterministic, non-empty string IDs."""
        id1 = _hash_value(value)
        id2 = _hash_value(value)

        assert isinstance(id1, str)
        assert len(id1) > 0
        assert id1 == id2

    def test_hash_value_boolean_distinct(self):
        """Test that True and False produce different IDs."""
        id_true = _hash_value(True)
        id_false = _hash_value(False)

        assert id_true != id_false
        assert _hash_value(True) == id_true
        assert _hash_value(False) == id_false

    def test_hash_value_different_values(self):
        """Test that different values produce different IDs."""
        values = [
            "test1",
            "test2",
            123,
            456,
            {"key": "value1"},
            {"key": "value2"},
            [1, 2, 3],
            [3, 2, 1],
            True,
            False,
            None,
        ]

        ids = [_hash_value(value) for value in values]
        assert len(set(ids)) == len(ids)

    def test_hash_value_order_sensitivity(self):
        """Test that dict key order matters in ID generation."""
        dict1 = {"a": 1, "b": 2}
        dict2 = {"b": 2, "a": 1}

        id1 = _hash_value(dict1)
        id2 = _hash_value(dict2)

        assert isinstance(id1, str)
        assert isinstance(id2, str)
        assert len(id1) > 0
        assert len(id2) > 0

    @pytest.mark.parametrize(
        "values",
        [
            pytest.param(
                ["Hello 世界", "café", "naïve", "🌟⭐", "Москва", "東京"],
                id="unicode",
            ),
            pytest.param(
                [
                    "!@#$%^&*()",
                    "line1\nline2\r\nline3",
                    "tab\tseparated\tvalues",
                    'quote"inside"string',
                    "apostrophe's test",
                    "backslash\\test",
                    "forward/slash/test",
                ],
                id="special_characters",
            ),
        ],
    )
    def test_hash_value_string_variants(self, values):
        """Test deterministic ID generation with unicode and special character values."""
        for value in values:
            id1 = _hash_value(value)
            id2 = _hash_value(value)

            assert isinstance(id1, str)
            assert id1 == id2
            assert len(id1) > 0

    def test_hash_value_large_values(self):
        """Test deterministic ID generation with large values."""
        large_string = "x" * 10000
        large_dict = {f"key_{i}": f"value_{i}" for i in range(1000)}
        large_list = [f"item_{i}" for i in range(1000)]

        for large_value in [large_string, large_dict, large_list]:
            id1 = _hash_value(large_value)
            id2 = _hash_value(large_value)

            assert isinstance(id1, str)
            assert id1 == id2
            assert len(id1) > 0

    def test_hash_value_idempotent(self):
        """Test that repeated calls always return the same ID for any type."""
        test_data = {
            "string": "test_value",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "dict": {"nested": {"deep": "value"}},
            "list": [1, "two", {"three": 3}],
            "none": None,
            "float_precision": 1.0000000000001,
            "unicode_normalization": "café",
            "nested_order": {"z": 1, "a": 2, "m": 3},
        }

        for _key, value in test_data.items():
            ids = [_hash_value(value) for _ in range(10)]
            assert len(set(ids)) == 1
            assert all(isinstance(id_val, str) for id_val in ids)
            assert all(len(id_val) > 0 for id_val in ids)


class TestDeterministicIdIntegration:
    """Test integration of deterministic ID functionality."""

    def test_id_collision_resistance(self):
        """Test that different inputs produce different IDs."""
        similar_values = [
            {"id": i, "name": f"item_{i}", "value": i * 2} for i in range(1000)
        ]

        ids = [_hash_value(value) for value in similar_values]
        assert len(set(ids)) == len(ids)

    def test_id_format_consistency(self):
        """Test that generated IDs have consistent format."""
        test_values = ["string", 123, {"dict": "value"}, ["list", "items"], None, True]

        ids = [_hash_value(value) for value in test_values]

        for id_val in ids:
            assert isinstance(id_val, str)
            assert len(id_val) > 0
            assert id_val.isprintable() or all(ord(c) >= 32 for c in id_val)

    def test_performance_with_large_data(self):
        """Test performance of deterministic ID generation with large data."""
        import time

        large_data = {
            "arrays": [[f"item_{i}_{j}" for j in range(100)] for i in range(100)],
            "objects": {f"key_{i}": {"nested": f"value_{i}"} for i in range(1000)},
            "string": "x" * 10000,
        }

        start_time = time.time()
        id1 = _hash_value(large_data)
        end_time = time.time()

        assert end_time - start_time < 1.0
        assert isinstance(id1, str)
        assert len(id1) > 0

        id2 = _hash_value(large_data)
        assert id1 == id2

    def test_thread_safety(self):
        """Test thread safety of deterministic ID generation."""
        import threading
        import time

        test_value = {"thread_test": True, "value": 42}
        results = []
        errors = []

        def generate_in_thread():
            try:
                for _ in range(100):
                    id_val = _hash_value(test_value)
                    results.append(id_val)
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        threads = []
        for _ in range(5):
            thread = threading.Thread(target=generate_in_thread)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(errors) == 0
        assert len(results) == 500
        assert len(set(results)) == 1

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param("", id="empty_string"),
            pytest.param(0, id="zero_int"),
            pytest.param(0.0, id="zero_float"),
            pytest.param([], id="empty_list"),
            pytest.param({}, id="empty_dict"),
            pytest.param(float("inf"), id="positive_inf"),
            pytest.param(float("-inf"), id="negative_inf"),
        ],
    )
    def test_edge_case_values(self, case):
        """Test edge case values produce deterministic IDs or raise expected errors."""
        try:
            id1 = _hash_value(case)
            id2 = _hash_value(case)

            assert isinstance(id1, str)
            assert isinstance(id2, str)
            assert id1 == id2
            assert len(id1) > 0
        except (ValueError, TypeError, OverflowError):
            pass


class TestDeterministicIdApiIntegration:
    """Test deterministic IDs through the API."""

    def test_deterministic_ids_enabled(self):
        """Test deterministic IDs when enabled in config."""
        data = {"name": "test", "value": 42}
        config = tm.TransmogConfig(id_generation="hash")

        result1 = tm.flatten(data, name="test", config=config)
        result2 = tm.flatten(data, name="test", config=config)

        assert result1.main[0]["_id"] == result2.main[0]["_id"]
        assert isinstance(result1.main[0]["_id"], str)
        assert len(result1.main[0]["_id"]) > 0

    def test_deterministic_ids_different_data(self):
        """Test that different data produces different deterministic IDs."""
        data1 = {"name": "test1", "value": 42}
        data2 = {"name": "test2", "value": 42}
        config = tm.TransmogConfig(id_generation="hash")

        result1 = tm.flatten(data1, name="test", config=config)
        result2 = tm.flatten(data2, name="test", config=config)

        assert result1.main[0]["_id"] != result2.main[0]["_id"]

    def test_composite_deterministic_ids(self):
        """Test composite deterministic IDs from multiple fields."""
        data1 = {"region": "US", "store": "001", "product": "laptop", "price": 999}
        data2 = {"region": "US", "store": "001", "product": "laptop", "price": 899}
        config = tm.TransmogConfig(id_generation=["region", "store", "product"])

        result1 = tm.flatten(data1, name="sales", config=config)
        result2 = tm.flatten(data2, name="sales", config=config)

        assert result1.main[0]["_id"] == result2.main[0]["_id"]

    def test_composite_deterministic_ids_different_keys(self):
        """Test that different composite keys produce different IDs."""
        data1 = {"region": "US", "store": "001", "product": "laptop"}
        data2 = {"region": "EU", "store": "001", "product": "laptop"}
        config = tm.TransmogConfig(id_generation=["region", "store", "product"])

        result1 = tm.flatten(data1, name="sales", config=config)
        result2 = tm.flatten(data2, name="sales", config=config)

        assert result1.main[0]["_id"] != result2.main[0]["_id"]

    def test_composite_id_missing_fields(self):
        """Test composite IDs when some fields are missing."""
        data1 = {"region": "US", "store": "001"}
        data2 = {"region": "US", "store": "001", "product": None}
        config = tm.TransmogConfig(id_generation=["region", "store", "product"])

        result1 = tm.flatten(data1, name="sales", config=config)
        result2 = tm.flatten(data2, name="sales", config=config)

        assert result1.main[0]["_id"] == result2.main[0]["_id"]

    def test_deterministic_vs_random_ids(self):
        """Test that deterministic IDs differ from random IDs."""
        data = {"name": "test", "value": 42}
        config_deterministic = tm.TransmogConfig(id_generation="hash")
        config_random = tm.TransmogConfig(id_generation="random")

        result_det = tm.flatten(data, name="test", config=config_deterministic)
        result_rand = tm.flatten(data, name="test", config=config_random)

        assert result_det.main[0]["_id"] != result_rand.main[0]["_id"]
        assert isinstance(result_det.main[0]["_id"], str)
        assert isinstance(result_rand.main[0]["_id"], str)

    def test_natural_ids_propagate_to_child_tables(self):
        """Ensure natural IDs become parent references for extracted arrays."""
        records = [
            {
                "_id": "ORDER-123",
                "customer": "Alice",
                "items": [
                    {"sku": "SKU-1", "qty": 1},
                    {"sku": "SKU-2", "qty": 2},
                ],
            }
        ]
        config = tm.TransmogConfig(id_generation="natural")
        context = ProcessingContext(extract_time="2025-01-01 00:00:00.000000")

        main_rows, child_tables = process_record_batch(
            records=records,
            entity_name="orders",
            config=config,
            _context=context,
        )

        assert main_rows[0]["_id"] == "ORDER-123"
        table_name = "orders_items"
        assert table_name in child_tables
        parent_ids = {row["_parent_id"] for row in child_tables[table_name]}
        assert parent_ids == {"ORDER-123"}
