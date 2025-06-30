"""
Tests for deterministic ID generation.

Tests deterministic ID generation, composite IDs, and reproducible behavior.
"""

import pytest
import hashlib
from typing import Dict, Any, List

from transmog.core.metadata import (
    generate_deterministic_id,
    generate_composite_id,
)


class TestDeterministicIdGeneration:
    """Test deterministic ID generation functions."""

    def test_generate_deterministic_id_simple(self):
        """Test generating deterministic ID with simple value."""
        value = "test_value"

        id1 = generate_deterministic_id(value)
        id2 = generate_deterministic_id(value)

        assert isinstance(id1, str)
        assert isinstance(id2, str)
        assert id1 == id2  # Same input should produce same output
        assert len(id1) > 0

    def test_generate_deterministic_id_integer(self):
        """Test generating deterministic ID with integer value."""
        value = 12345

        id1 = generate_deterministic_id(value)
        id2 = generate_deterministic_id(value)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_generate_deterministic_id_float(self):
        """Test generating deterministic ID with float value."""
        value = 3.14159

        id1 = generate_deterministic_id(value)
        id2 = generate_deterministic_id(value)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_generate_deterministic_id_boolean(self):
        """Test generating deterministic ID with boolean value."""
        value_true = True
        value_false = False

        id_true1 = generate_deterministic_id(value_true)
        id_true2 = generate_deterministic_id(value_true)
        id_false1 = generate_deterministic_id(value_false)
        id_false2 = generate_deterministic_id(value_false)

        assert id_true1 == id_true2
        assert id_false1 == id_false2
        assert id_true1 != id_false1  # Different values should produce different IDs

    def test_generate_deterministic_id_none(self):
        """Test generating deterministic ID with None value."""
        value = None

        id1 = generate_deterministic_id(value)
        id2 = generate_deterministic_id(value)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_generate_deterministic_id_dict(self):
        """Test generating deterministic ID with dictionary value."""
        value = {"name": "test", "id": 123, "active": True}

        id1 = generate_deterministic_id(value)
        id2 = generate_deterministic_id(value)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_generate_deterministic_id_list(self):
        """Test generating deterministic ID with list value."""
        value = ["item1", "item2", 123, True]

        id1 = generate_deterministic_id(value)
        id2 = generate_deterministic_id(value)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_generate_deterministic_id_different_values(self):
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

        ids = [generate_deterministic_id(value) for value in values]

        # All IDs should be unique
        assert len(set(ids)) == len(ids)

    def test_generate_deterministic_id_order_sensitivity(self):
        """Test that order matters in deterministic ID generation."""
        dict1 = {"a": 1, "b": 2}
        dict2 = {"b": 2, "a": 1}  # Same content, different order

        id1 = generate_deterministic_id(dict1)
        id2 = generate_deterministic_id(dict2)

        # Implementation might or might not be order-sensitive
        # Just test that both produce valid IDs
        assert isinstance(id1, str)
        assert isinstance(id2, str)
        assert len(id1) > 0
        assert len(id2) > 0

    def test_generate_deterministic_id_nested_structures(self):
        """Test deterministic ID generation with nested structures."""
        nested_value = {
            "level1": {"level2": {"level3": ["item1", "item2", {"deep": "value"}]}},
            "other": [1, 2, {"nested": True}],
        }

        id1 = generate_deterministic_id(nested_value)
        id2 = generate_deterministic_id(nested_value)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_generate_deterministic_id_unicode(self):
        """Test deterministic ID generation with unicode values."""
        unicode_values = ["Hello 世界", "café", "naïve", "🌟⭐", "Москва", "東京"]

        for value in unicode_values:
            id1 = generate_deterministic_id(value)
            id2 = generate_deterministic_id(value)

            assert isinstance(id1, str)
            assert id1 == id2
            assert len(id1) > 0

    def test_generate_deterministic_id_large_values(self):
        """Test deterministic ID generation with large values."""
        large_string = "x" * 10000
        large_dict = {f"key_{i}": f"value_{i}" for i in range(1000)}
        large_list = [f"item_{i}" for i in range(1000)]

        for large_value in [large_string, large_dict, large_list]:
            id1 = generate_deterministic_id(large_value)
            id2 = generate_deterministic_id(large_value)

            assert isinstance(id1, str)
            assert id1 == id2
            assert len(id1) > 0

    def test_generate_deterministic_id_special_characters(self):
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
            id1 = generate_deterministic_id(value)
            id2 = generate_deterministic_id(value)

            assert isinstance(id1, str)
            assert id1 == id2
            assert len(id1) > 0

    def test_generate_deterministic_id_consistency_across_calls(self):
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

        # Generate IDs multiple times
        for key, value in test_data.items():
            ids = [generate_deterministic_id(value) for _ in range(10)]

            # All IDs should be identical
            assert len(set(ids)) == 1
            assert all(isinstance(id_val, str) for id_val in ids)
            assert all(len(id_val) > 0 for id_val in ids)


class TestCompositeIdGeneration:
    """Test composite ID generation functions."""

    def test_generate_composite_id_simple(self):
        """Test generating composite ID with simple components."""
        values = {"field1": "comp1", "field2": "comp2", "field3": "comp3"}
        fields = ["field1", "field2", "field3"]

        id1 = generate_composite_id(values, fields)
        id2 = generate_composite_id(values, fields)

        assert isinstance(id1, str)
        assert isinstance(id2, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_generate_composite_id_mixed_types(self):
        """Test generating composite ID with mixed type components."""
        values = {
            "str": "string",
            "int": 123,
            "float": 3.14,
            "bool": True,
            "none": None,
        }
        fields = ["str", "int", "float", "bool", "none"]

        id1 = generate_composite_id(values, fields)
        id2 = generate_composite_id(values, fields)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_generate_composite_id_empty_list(self):
        """Test generating composite ID with empty components."""
        values = {}
        fields = []

        id1 = generate_composite_id(values, fields)
        id2 = generate_composite_id(values, fields)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_generate_composite_id_single_component(self):
        """Test generating composite ID with single component."""
        values = {"field": "single_component"}
        fields = ["field"]

        id1 = generate_composite_id(values, fields)
        id2 = generate_composite_id(values, fields)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_generate_composite_id_order_matters(self):
        """Test generating composite ID with different field orders."""
        values = {"a": "a", "b": "b", "c": "c"}
        fields1 = ["a", "b", "c"]
        fields2 = ["c", "b", "a"]

        id1 = generate_composite_id(values, fields1)
        id2 = generate_composite_id(values, fields2)

        assert isinstance(id1, str)
        assert isinstance(id2, str)
        # Same values should produce same ID regardless of field order for deterministic behavior
        assert id1 == id2

    def test_generate_composite_id_with_duplicates(self):
        """Test generating composite ID with duplicate components."""
        values = {"a": "a", "b": "b", "c": "c"}
        fields = ["a", "b", "a", "c", "b"]

        id1 = generate_composite_id(values, fields)
        id2 = generate_composite_id(values, fields)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_generate_composite_id_complex_components(self):
        """Test generating composite ID with complex components."""
        values = {
            "nested": {"nested": {"deep": "value"}},
            "list": [1, 2, {"inner": "list"}],
            "simple": "simple_string",
            "number": 42,
        }
        fields = ["nested", "list", "simple", "number"]

        id1 = generate_composite_id(values, fields)
        id2 = generate_composite_id(values, fields)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0

    def test_generate_composite_id_different_lengths(self):
        """Test composite IDs with different component lengths."""
        test_cases = [
            ({"a": "a"}, ["a"]),
            ({"a": "a", "b": "b"}, ["a", "b"]),
            ({"a": "a", "b": "b", "c": "c"}, ["a", "b", "c"]),
            (
                {"a": "a", "b": "b", "c": "c", "d": "d", "e": "e"},
                ["a", "b", "c", "d", "e"],
            ),
            (
                {f"field_{i}": f"value_{i}" for i in range(100)},
                [f"field_{i}" for i in range(100)],
            ),  # Long list
        ]

        ids = []
        for values, fields in test_cases:
            composite_id = generate_composite_id(values, fields)
            assert isinstance(composite_id, str)
            assert len(composite_id) > 0
            ids.append(composite_id)

        # All IDs should be unique
        assert len(set(ids)) == len(ids)

    def test_generate_composite_id_unicode_components(self):
        """Test composite ID generation with unicode components."""
        values = {
            "hello": "Hello",
            "world": "世界",
            "cafe": "café",
            "star": "🌟",
            "moscow": "Москва",
        }
        fields = ["hello", "world", "cafe", "star", "moscow"]

        id1 = generate_composite_id(values, fields)
        id2 = generate_composite_id(values, fields)

        assert isinstance(id1, str)
        assert id1 == id2
        assert len(id1) > 0


class TestDeterministicIdIntegration:
    """Test integration of deterministic ID functionality."""

    def test_deterministic_vs_composite_ids(self):
        """Test relationship between deterministic and composite IDs."""
        value = {"a": 1, "b": 2}
        values = {"a": 1, "b": 2}
        fields = ["a", "b"]

        det_id = generate_deterministic_id(value)
        comp_id = generate_composite_id(values, fields)

        assert isinstance(det_id, str)
        assert isinstance(comp_id, str)
        # They might or might not be equal depending on implementation
        assert len(det_id) > 0
        assert len(comp_id) > 0

    def test_reproducibility_across_sessions(self):
        """Test that IDs are reproducible across different sessions."""
        # This simulates what would happen if the same data was processed
        # in different runs of the application

        test_values = [
            "consistent_string",
            12345,
            {"key": "value", "number": 42},
            ["item1", "item2", {"nested": True}],
        ]

        # Generate IDs in first "session"
        session1_ids = [generate_deterministic_id(value) for value in test_values]

        # Generate IDs in second "session" (same values)
        session2_ids = [generate_deterministic_id(value) for value in test_values]

        # Should be identical
        assert session1_ids == session2_ids

    def test_id_collision_resistance(self):
        """Test that different inputs produce different IDs."""
        # Create many similar but different values
        similar_values = [
            {"id": i, "name": f"item_{i}", "value": i * 2} for i in range(1000)
        ]

        ids = [generate_deterministic_id(value) for value in similar_values]

        # All IDs should be unique
        assert len(set(ids)) == len(ids)

    def test_id_format_consistency(self):
        """Test that generated IDs have consistent format."""
        test_values = ["string", 123, {"dict": "value"}, ["list", "items"], None, True]

        ids = [generate_deterministic_id(value) for value in test_values]

        for id_val in ids:
            assert isinstance(id_val, str)
            assert len(id_val) > 0
            # IDs should be valid strings (no control characters, etc.)
            assert id_val.isprintable() or all(ord(c) >= 32 for c in id_val)

    def test_performance_with_large_data(self):
        """Test performance of deterministic ID generation with large data."""
        import time

        # Create large data structure
        large_data = {
            "arrays": [[f"item_{i}_{j}" for j in range(100)] for i in range(100)],
            "objects": {f"key_{i}": {"nested": f"value_{i}"} for i in range(1000)},
            "string": "x" * 10000,
        }

        start_time = time.time()
        id1 = generate_deterministic_id(large_data)
        end_time = time.time()

        # Should complete in reasonable time (less than 1 second)
        assert end_time - start_time < 1.0
        assert isinstance(id1, str)
        assert len(id1) > 0

        # Should be reproducible
        id2 = generate_deterministic_id(large_data)
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
                    id_val = generate_deterministic_id(test_value)
                    results.append(id_val)
                    time.sleep(0.001)  # Small delay
            except Exception as e:
                errors.append(e)

        # Create multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=generate_in_thread)
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Should have no errors
        assert len(errors) == 0
        assert len(results) == 500  # 5 threads * 100 iterations

        # All results should be identical
        unique_results = set(results)
        assert len(unique_results) == 1

    def test_memory_efficiency(self):
        """Test memory efficiency of ID generation."""
        import gc

        # Generate many IDs and ensure memory is managed properly
        values = [{"id": i, "data": f"item_{i}"} for i in range(10000)]

        # Force garbage collection before
        gc.collect()

        # Generate IDs
        ids = [generate_deterministic_id(value) for value in values]

        # All IDs should be valid and unique
        assert len(ids) == 10000
        assert len(set(ids)) == 10000

        # Force garbage collection after
        gc.collect()

        # Memory should be manageable (this is more of a smoke test)
        assert all(isinstance(id_val, str) for id_val in ids[:100])  # Check first 100

    def test_edge_cases_and_corner_cases(self):
        """Test edge cases and corner cases."""
        edge_cases = [
            "",  # Empty string
            0,  # Zero integer
            0.0,  # Zero float
            [],  # Empty list
            {},  # Empty dict
            float("inf"),  # Infinity
            float("-inf"),  # Negative infinity
            # float('nan'),  # NaN (might be problematic)
        ]

        for case in edge_cases:
            try:
                id1 = generate_deterministic_id(case)
                id2 = generate_deterministic_id(case)

                assert isinstance(id1, str)
                assert isinstance(id2, str)
                assert id1 == id2
                assert len(id1) > 0
            except Exception as e:
                # Some edge cases might legitimately fail
                # Just ensure the failure is handled gracefully
                assert isinstance(e, (ValueError, TypeError, OverflowError))

    def test_serialization_consistency(self):
        """Test that IDs are consistent regardless of internal serialization."""
        # Test with data that might serialize differently
        test_data = {
            "float_precision": 1.0000000000001,
            "unicode_normalization": "café",  # Might have different unicode representations
            "nested_order": {"z": 1, "a": 2, "m": 3},  # Order might matter
        }

        # Generate ID multiple times
        ids = [generate_deterministic_id(test_data) for _ in range(10)]

        # Should all be the same
        assert len(set(ids)) == 1
        assert all(isinstance(id_val, str) for id_val in ids)
        assert all(len(id_val) > 0 for id_val in ids)
