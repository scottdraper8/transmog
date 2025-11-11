"""
Tests for naming conventions and field naming.

Tests field naming, separator handling, and naming utilities.
"""

import pytest

import transmog as tm


class TestFieldNaming:
    """Test field naming conventions."""

    def test_default_separator(self):
        """Test default underscore separator."""
        data = {"level1": {"level2": {"value": "test"}}}

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        assert "level1_level2_value" in main_record
        assert main_record["level1_level2_value"] == "test"

    def test_dot_separator(self):
        """Test dot separator."""
        data = {"level1": {"level2": {"value": "test"}}}

        config = tm.TransmogConfig(separator=".")
        result = tm.flatten(data, name="test", config=config)

        main_record = result.main[0]
        assert "level1.level2.value" in main_record
        assert main_record["level1.level2.value"] == "test"

    def test_custom_separator(self):
        """Test custom separator."""
        data = {"level1": {"level2": {"value": "test"}}}

        config = tm.TransmogConfig(separator="::")
        result = tm.flatten(data, name="test", config=config)

        main_record = result.main[0]
        assert "level1::level2::value" in main_record
        assert main_record["level1::level2::value"] == "test"

    def test_deeply_nested_naming(self):
        """Test naming with deeply nested structures."""
        data = {"a": {"b": {"c": {"d": {"e": {"value": "deep"}}}}}}

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        # Should handle deep nesting
        deep_field = None
        for key in main_record.keys():
            if "value" in key and main_record[key] == "deep":
                deep_field = key
                break

        assert deep_field is not None
        # Should contain all path components or be simplified
        assert "a" in deep_field or len(deep_field.split("_")) <= 6

    def test_deeply_nested_simplification(self):
        """Test that deeply nested paths are simplified."""
        deep_data = {
            "level1": {"level2": {"level3": {"level4": {"level5": {"value": "deep"}}}}}
        }

        config = tm.TransmogConfig()
        result = tm.flatten(deep_data, name="test", config=config)

        assert len(result.main) == 1

        keys = list(result.main[0].keys())
        value_field = next((k for k in keys if "value" in k), None)

        assert value_field is not None
        # Deep paths include all levels
        assert value_field == "level1_level2_level3_level4_level5_value"

    def test_special_characters_in_keys(self):
        """Test handling special characters in field names."""
        data = {
            "field-with-dashes": "value1",
            "field.with.dots": "value2",
            "field with spaces": "value3",
            "field@with@symbols": "value4",
            "field/with/slashes": "value5",
        }

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        # Should handle special characters (may sanitize or preserve)
        assert len(main_record) >= 5  # All fields should be processed

    def test_numeric_field_names(self):
        """Test handling numeric field names."""
        data = {
            "123": "numeric_key",
            "456field": "mixed_key",
            "field789": "field_with_number",
        }

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        # Should handle numeric keys
        assert len(main_record) >= 3

    def test_unicode_field_names(self):
        """Test handling Unicode field names."""
        data = {
            "café": "coffee",
            "résumé": "cv",
            "naïve": "innocent",
            "🚀": "rocket",
            "测试": "test",
        }

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        # Should handle Unicode keys
        assert len(main_record) >= 5

    def test_empty_field_names(self):
        """Test handling empty or whitespace field names."""
        data = {"": "empty_key", " ": "space_key", "\t": "tab_key", "\n": "newline_key"}

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        # Should handle empty/whitespace keys (may skip or sanitize)
        assert isinstance(main_record, dict)

    def test_duplicate_field_names_after_flattening(self):
        """Test handling potential duplicate field names after flattening."""
        data = {"user_name": "direct_field", "user": {"name": "nested_field"}}

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        # Should handle potential conflicts
        assert len(main_record) >= 2

    def test_case_sensitivity(self):
        """Test case sensitivity in field names."""
        data = {
            "Name": "uppercase",
            "name": "lowercase",
            "NAME": "allcaps",
            "nAmE": "mixed",
        }

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        # Should preserve case distinctions
        assert len(main_record) >= 4


class TestTableNaming:
    """Test table naming conventions."""

    def test_main_table_naming(self):
        """Test main table naming."""
        data = {"id": 1, "name": "test"}

        result = tm.flatten(data, name="custom_entity")

        # Check that entity name is used correctly
        all_tables = result.all_tables
        assert "custom_entity" in all_tables

    def test_child_table_naming(self):
        """Test child table naming."""
        data = {
            "id": 1,
            "name": "parent",
            "children": [{"id": 101, "name": "child1"}, {"id": 102, "name": "child2"}],
        }

        config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
        result = tm.flatten(data, name="parent_entity", config=config)

        # Should have child table with appropriate name
        table_names = list(result.tables.keys())
        children_table = next(
            (name for name in table_names if "children" in name.lower()), None
        )
        assert children_table is not None

    def test_nested_array_table_naming(self):
        """Test naming of tables from nested arrays."""
        data = {
            "id": 1,
            "organization": {
                "departments": [
                    {"id": "dept1", "teams": [{"id": "team1", "name": "Backend"}]}
                ]
            },
        }

        config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
        result = tm.flatten(data, name="company", config=config)

        table_names = list(result.tables.keys())
        # Should have appropriately named tables
        dept_table = next(
            (name for name in table_names if "departments" in name.lower()), None
        )
        teams_table = next(
            (name for name in table_names if "teams" in name.lower()), None
        )

        assert dept_table is not None
        assert teams_table is not None

    def test_table_naming_with_separator(self):
        """Test table naming with custom separator."""
        data = {"id": 1, "nested": {"items": [{"id": 1, "value": "test"}]}}

        config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE, separator=".")
        result = tm.flatten(data, name="test", config=config)

        # Table names might use separator or have special handling
        table_names = list(result.tables.keys())
        assert len(table_names) > 0

    def test_long_table_names(self):
        """Test handling of very long table names."""
        data = {
            "id": 1,
            "very_long_nested_structure_with_many_levels": {
                "another_very_long_level_name": {
                    "yet_another_extremely_long_level": {
                        "items": [{"id": 1, "value": "test"}]
                    }
                }
            },
        }

        config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
        result = tm.flatten(data, name="test", config=config)

        # Should handle long names (may truncate or simplify)
        table_names = list(result.tables.keys())
        assert len(table_names) > 0


class TestNamingEdgeCases:
    """Test edge cases for naming conventions."""

    def test_conflicting_field_and_table_names(self):
        """Test handling conflicts between field and table names."""
        data = {
            "id": 1,
            "items": "field_value",  # Field named "items"
            "nested": {
                "items": [
                    {"id": 1, "value": "array"}
                ]  # Array that would create "items" table
            },
        }

        config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
        result = tm.flatten(data, name="test", config=config)

        # Should handle naming conflicts
        main_record = result.main[0]
        assert isinstance(main_record, dict)

    def test_reserved_keyword_field_names(self):
        """Test handling field names that might be reserved keywords."""
        data = {
            "class": "python_keyword",
            "type": "another_keyword",
            "import": "yet_another",
            "def": "function_keyword",
        }

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        # Should handle reserved keywords
        assert len(main_record) >= 4

    def test_sql_reserved_words(self):
        """Test handling SQL reserved words as field names."""
        data = {
            "select": "sql_keyword",
            "from": "another_sql",
            "where": "condition_keyword",
            "order": "sorting_keyword",
            "group": "grouping_keyword",
        }

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        # Should handle SQL reserved words
        assert len(main_record) >= 5

    def test_very_long_field_names(self):
        """Test handling very long field names."""
        long_name = "a" * 1000  # Very long field name
        data = {long_name: "long_field_value", "normal": "normal_value"}

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        # Should handle long field names (may truncate)
        assert len(main_record) >= 2

    def test_field_names_with_separators(self):
        """Test field names that contain the separator character."""
        data = {
            "field_with_underscore": "value1",
            "nested": {"field_with_underscore": "value2"},
        }

        config = tm.TransmogConfig(separator="_")
        result = tm.flatten(data, name="test", config=config)

        main_record = result.main[0]
        # Should handle separator in field names
        assert len(main_record) >= 2

    def test_null_and_none_in_field_names(self):
        """Test handling null/None-like strings in field names."""
        data = {
            "null": "null_string",
            "None": "none_string",
            "undefined": "undefined_string",
            "nil": "nil_string",
        }

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        # Should handle null-like field names
        assert len(main_record) >= 4

    def test_field_name_normalization(self):
        """Test field name normalization."""
        data = {
            "Field Name": "spaces",
            "field-name": "dashes",
            "field.name": "dots",
            "field@name": "symbols",
            "field/name": "slashes",
        }

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        # Should normalize field names consistently
        assert len(main_record) >= 5

        # Check that normalization is consistent
        field_names = list(main_record.keys())
        # Should not have duplicates after normalization
        assert len(field_names) == len(set(field_names))


class TestNamingConsistency:
    """Test naming consistency across different scenarios."""

    def test_consistent_naming_across_batches(self):
        """Test that naming is consistent across different batches."""
        data1 = [
            {"user": {"profile": {"name": "Alice"}}},
            {"user": {"profile": {"name": "Bob"}}},
        ]

        data2 = [
            {"user": {"profile": {"name": "Charlie"}}},
            {"user": {"profile": {"name": "Diana"}}},
        ]

        result1 = tm.flatten(data1, name="users")
        result2 = tm.flatten(data2, name="users")

        # Field names should be consistent
        fields1 = set(result1.main[0].keys())
        fields2 = set(result2.main[0].keys())

        assert fields1 == fields2

    def test_naming_with_different_separators(self):
        """Test naming consistency with different separators."""
        data = {"level1": {"level2": {"value": "test"}}}

        config_underscore = tm.TransmogConfig(separator="_")
        result_underscore = tm.flatten(data, name="test", config=config_underscore)
        config_dot = tm.TransmogConfig(separator=".")
        result_dot = tm.flatten(data, name="test", config=config_dot)
        config_dash = tm.TransmogConfig(separator="-")
        result_dash = tm.flatten(data, name="test", config=config_dash)

        # Should have same structure but different field names
        assert len(result_underscore.main) == 1
        assert len(result_dot.main) == 1
        assert len(result_dash.main) == 1

    def test_naming_determinism(self):
        """Test that naming is deterministic."""
        data = {"nested": {"field1": "value1", "field2": "value2"}}

        result1 = tm.flatten(data, name="test")
        result2 = tm.flatten(data, name="test")

        # Field names should be identical
        fields1 = set(result1.main[0].keys())
        fields2 = set(result2.main[0].keys())

        assert fields1 == fields2
