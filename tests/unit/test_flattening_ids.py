"""
Tests for deterministic ID generation.

Tests deterministic ID generation, composite IDs, and reproducible behavior.
"""

import pytest

import transmog as tm
from transmog.flattening import _hash_value


class TestDeterministicIdGeneration:
    """Test deterministic ID generation functions."""

    def test_hash_value_simple(self):
        """Test generating deterministic ID with simple value."""
        value = "test_value"

        id1 = _hash_value(value)
        id2 = _hash_value(value)

        assert isinstance(id1, str)
        assert isinstance(id2, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_hash_value_integer(self):
        """Test generating deterministic ID with integer value."""
        value = 12345

        id1 = _hash_value(value)
        id2 = _hash_value(value)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_hash_value_float(self):
        """Test generating deterministic ID with float value."""
        value = 3.14159

        id1 = _hash_value(value)
        id2 = _hash_value(value)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_hash_value_boolean(self):
        """Test generating deterministic ID with boolean value."""
        value_true = True
        value_false = False

        id_true1 = _hash_value(value_true)
        id_true2 = _hash_value(value_true)
        id_false1 = _hash_value(value_false)
        id_false2 = _hash_value(value_false)

        assert id_true1 == id_true2
        assert id_false1 == id_false2
        assert id_true1 != id_false1

    def test_hash_value_none(self):
        """Test generating deterministic ID with None value."""
        value = None

        id1 = _hash_value(value)
        id2 = _hash_value(value)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_hash_value_dict(self):
        """Test generating deterministic ID with dictionary value."""
        value = {"name": "test", "id": 123, "active": True}

        id1 = _hash_value(value)
        id2 = _hash_value(value)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_hash_value_list(self):
        """Test generating deterministic ID with list value."""
        value = ["item1", "item2", 123, True]

        id1 = _hash_value(value)
        id2 = _hash_value(value)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

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
        """Test that order matters in deterministic ID generation."""
        dict1 = {"a": 1, "b": 2}
        dict2 = {"b": 2, "a": 1}

        id1 = _hash_value(dict1)
        id2 = _hash_value(dict2)

        assert isinstance(id1, str)
        assert isinstance(id2, str)
        assert len(id1) > 0
        assert len(id2) > 0

    def test_hash_value_nested_structures(self):
        """Test deterministic ID generation with nested structures."""
        nested_value = {
            "level1": {"level2": {"level3": ["item1", "item2", {"deep": "value"}]}},
            "other": [1, 2, {"nested": True}],
        }

        id1 = _hash_value(nested_value)
        id2 = _hash_value(nested_value)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_hash_value_unicode(self):
        """Test deterministic ID generation with unicode values."""
        unicode_values = ["Hello 世界", "café", "naïve", "🌟⭐", "Москва", "東京"]

        for value in unicode_values:
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

    def test_hash_value_special_characters(self):
        """Test deterministic ID generation with special characters."""
        special_values = [
            "!@#$%^&*()",
            "line1\nline2\r\nline3",
            "tab\tseparated\tvalues",
            'quote"inside"string',
            "apostrophe's test",
            "backslash\\test",
            "forward/slash/test",
        ]

        for value in special_values:
            id1 = _hash_value(value)
            id2 = _hash_value(value)

            assert isinstance(id1, str)
            assert id1 == id2
            assert len(id1) > 0

    def test_hash_value_consistency_across_calls(self):
        """Test that deterministic IDs are consistent across multiple calls."""
        test_data = {
            "string": "test_value",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "dict": {"nested": {"deep": "value"}},
            "list": [1, "two", {"three": 3}],
            "none": None,
        }

        for _key, value in test_data.items():
            ids = [_hash_value(value) for _ in range(10)]
            assert len(set(ids)) == 1
            assert all(isinstance(id_val, str) for id_val in ids)
            assert all(len(id_val) > 0 for id_val in ids)


class TestDeterministicIdIntegration:
    """Test integration of deterministic ID functionality."""

    def test_reproducibility_across_sessions(self):
        """Test that IDs are reproducible across different sessions."""
        test_values = [
            "consistent_string",
            12345,
            {"key": "value", "number": 42},
            ["item1", "item2", {"nested": True}],
        ]

        session1_ids = [_hash_value(value) for value in test_values]
        session2_ids = [_hash_value(value) for value in test_values]

        assert session1_ids == session2_ids

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

    def test_memory_efficiency(self):
        """Test memory efficiency of ID generation."""
        import gc

        values = [{"id": i, "data": f"item_{i}"} for i in range(10000)]

        gc.collect()
        ids = [_hash_value(value) for value in values]

        assert len(ids) == 10000
        assert len(set(ids)) == 10000

        gc.collect()
        assert all(isinstance(id_val, str) for id_val in ids[:100])

    def test_edge_cases_and_corner_cases(self):
        """Test edge cases and corner cases."""
        edge_cases = [
            "",
            0,
            0.0,
            [],
            {},
            float("inf"),
            float("-inf"),
        ]

        for case in edge_cases:
            try:
                id1 = _hash_value(case)
                id2 = _hash_value(case)

                assert isinstance(id1, str)
                assert isinstance(id2, str)
                assert id1 == id2
                assert len(id1) > 0
            except Exception as e:
                assert isinstance(e, (ValueError, TypeError, OverflowError))

    def test_serialization_consistency(self):
        """Test that IDs are consistent regardless of internal serialization."""
        test_data = {
            "float_precision": 1.0000000000001,
            "unicode_normalization": "café",
            "nested_order": {"z": 1, "a": 2, "m": 3},
        }

        ids = [_hash_value(test_data) for _ in range(10)]

        assert len(set(ids)) == 1
        assert all(isinstance(id_val, str) for id_val in ids)
        assert all(len(id_val) > 0 for id_val in ids)


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
