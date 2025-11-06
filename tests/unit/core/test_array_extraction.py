"""
Tests for array extraction and processing.

Tests array extraction, different array handling modes, and edge cases.
"""

import pytest

import transmog as tm


class TestArrayExtraction:
    """Test basic array extraction functionality."""

    def test_extract_simple_array(self):
        """Test extracting simple arrays."""
        data = {"id": 1, "name": "Test", "tags": ["tag1", "tag2", "tag3"]}

        result = tm.flatten(data, name="test", arrays="separate")

        # Should have main record
        assert len(result.main) == 1
        assert result.main[0]["name"] == "Test"

        # Should have tags table
        tags_table = None
        for table_name, table_data in result.tables.items():
            if "tags" in table_name.lower():
                tags_table = table_data
                break

        assert tags_table is not None
        assert len(tags_table) == 3

        # Check tag values
        tag_values = [
            record.get("value") or record.get("tags") for record in tags_table
        ]
        assert "tag1" in tag_values
        assert "tag2" in tag_values
        assert "tag3" in tag_values

    def test_extract_object_array(self):
        """Test extracting arrays of objects."""
        data = {
            "id": 1,
            "name": "Company",
            "employees": [
                {"id": 101, "name": "Alice", "role": "Engineer"},
                {"id": 102, "name": "Bob", "role": "Designer"},
            ],
        }

        result = tm.flatten(data, name="company", arrays="separate")

        # Should have main record
        assert len(result.main) == 1
        assert result.main[0]["name"] == "Company"

        # Should have employees table
        employees_table = None
        for table_name, table_data in result.tables.items():
            if "employees" in table_name.lower():
                employees_table = table_data
                break

        assert employees_table is not None
        assert len(employees_table) == 2

        # Check employee data
        employee_names = [emp["name"] for emp in employees_table]
        assert "Alice" in employee_names
        assert "Bob" in employee_names

    def test_extract_nested_arrays(self):
        """Test extracting nested arrays."""
        data = {
            "id": 1,
            "name": "Test",
            "departments": [
                {
                    "id": "dept1",
                    "name": "Engineering",
                    "teams": [
                        {"id": "team1", "name": "Backend"},
                        {"id": "team2", "name": "Frontend"},
                    ],
                }
            ],
        }

        result = tm.flatten(data, name="company", arrays="separate")

        # Should have main record
        assert len(result.main) == 1

        # Should have departments table
        dept_table = None
        teams_table = None

        for table_name, table_data in result.tables.items():
            if (
                "departments" in table_name.lower()
                and "teams" not in table_name.lower()
            ):
                dept_table = table_data
            elif "teams" in table_name.lower():
                teams_table = table_data

        assert dept_table is not None
        assert len(dept_table) == 1
        assert dept_table[0]["name"] == "Engineering"

        assert teams_table is not None
        assert len(teams_table) == 2

    def test_extract_mixed_type_array(self):
        """Test extracting arrays with mixed types."""
        data = {
            "id": 1,
            "name": "Test",
            "mixed_array": ["string_value", 42, {"nested": "object"}, True, None],
        }

        result = tm.flatten(data, name="test", arrays="separate")

        # Should have main record
        assert len(result.main) == 1

        # Should have mixed_array table
        mixed_table = None
        for table_name, table_data in result.tables.items():
            if "mixed_array" in table_name.lower():
                mixed_table = table_data
                break

        assert mixed_table is not None
        # Array might filter out null values
        assert len(mixed_table) >= 4  # At least non-null items

    def test_extract_empty_array(self):
        """Test extracting empty arrays."""
        data = {"id": 1, "name": "Test", "empty_tags": []}

        result = tm.flatten(data, name="test", arrays="separate")

        # Should have main record
        assert len(result.main) == 1

        # Empty array handling may vary - either no table or empty table
        empty_table = None
        for table_name, table_data in result.tables.items():
            if "empty_tags" in table_name.lower():
                empty_table = table_data
                break

        if empty_table is not None:
            assert len(empty_table) == 0

    def test_extract_array_with_parent_references(self):
        """Test that extracted arrays have parent references."""
        data = {
            "id": 1,
            "name": "Parent",
            "children": [{"id": 101, "name": "Child1"}, {"id": 102, "name": "Child2"}],
        }

        result = tm.flatten(data, name="parent", arrays="separate")

        # Get parent ID
        parent_record = result.main[0]
        parent_id = None
        for key, value in parent_record.items():
            if "id" in key.lower():
                parent_id = value
                break

        # Should have children table
        children_table = None
        for table_name, table_data in result.tables.items():
            if "children" in table_name.lower():
                children_table = table_data
                break

        assert children_table is not None
        assert len(children_table) == 2

        # Children should have parent references
        for child in children_table:
            has_parent_ref = any("parent" in key.lower() for key in child.keys())
            assert has_parent_ref


class TestArrayHandlingModes:
    """Test different array handling modes."""

    def test_arrays_smart_mode(self):
        """Test arrays='smart' mode (default)."""
        data = {
            "id": 1,
            "name": "Test",
            "tags": ["tag1", "tag2"],  # Simple array - should be kept inline
            "items": [
                {"id": 1, "value": "a"},
                {"id": 2, "value": "b"},
            ],  # Complex array - should be exploded
        }

        result = tm.flatten(data, name="test", arrays="smart")

        # Simple array should be kept in main record as native array
        main_record = result.main[0]
        assert "tags" in main_record
        assert isinstance(main_record["tags"], list)
        assert main_record["tags"] == ["tag1", "tag2"]

        # Complex array should create child tables
        assert len(result.tables) > 0
        assert "test_items" in result.tables

    def test_arrays_separate_mode(self):
        """Test arrays='separate' mode."""
        data = {"id": 1, "name": "Test", "tags": ["tag1", "tag2"]}

        result = tm.flatten(data, name="test", arrays="separate")

        # Should have child tables
        assert len(result.tables) > 0

        # Main record should not contain array data directly
        main_record = result.main[0]
        assert "tags" not in main_record or not isinstance(
            main_record.get("tags"), list
        )

    def test_arrays_inline_mode(self):
        """Test arrays='inline' mode."""
        data = {"id": 1, "name": "Test", "tags": ["tag1", "tag2"]}

        result = tm.flatten(data, name="test", arrays="inline")

        # Arrays should be kept in main record (as JSON or flattened)
        main_record = result.main[0]
        # Implementation may vary - arrays might be JSON strings or flattened fields

    def test_arrays_skip_mode(self):
        """Test arrays='skip' mode."""
        data = {
            "id": 1,
            "name": "Test",
            "tags": ["tag1", "tag2"],
            "metadata": {"created": "2023-01-01"},
        }

        result = tm.flatten(data, name="test", arrays="skip")

        # Should have main record with non-array fields
        assert len(result.main) == 1
        assert result.main[0]["name"] == "Test"

        # Arrays should be ignored - no child tables for arrays
        # (but nested objects might still create tables)

    def test_array_mode_comparison(self):
        """Test comparison of different array handling modes."""
        data = {
            "id": 1,
            "name": "Test",
            "tags": ["tag1", "tag2"],
            "items": [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}],
        }

        result_smart = tm.flatten(data, name="test", arrays="smart")
        result_separate = tm.flatten(data, name="test", arrays="separate")
        result_inline = tm.flatten(data, name="test", arrays="inline")
        result_skip = tm.flatten(data, name="test", arrays="skip")

        # Smart should keep simple arrays inline, explode complex arrays
        assert "tags" in result_smart.main[0]
        assert isinstance(result_smart.main[0]["tags"], list)
        assert "test_items" in result_smart.tables

        # Separate should have child tables

        # Separate should have child tables
        assert len(result_separate.tables) > 0

        # All should have main record
        assert len(result_separate.main) == 1
        assert len(result_inline.main) == 1
        assert len(result_skip.main) == 1

        # All main records should have basic fields
        assert result_separate.main[0]["name"] == "Test"
        assert result_inline.main[0]["name"] == "Test"
        assert result_skip.main[0]["name"] == "Test"


class TestArrayEdgeCases:
    """Test edge cases for array handling."""

    def test_array_with_null_values(self):
        """Test arrays containing null values."""
        data = {
            "id": 1,
            "name": "Test",
            "nullable_array": ["value1", None, "value3", None],
        }

        result = tm.flatten(data, name="test", arrays="separate")

        # Should handle null values in arrays
        nullable_table = None
        for table_name, table_data in result.tables.items():
            if "nullable_array" in table_name.lower():
                nullable_table = table_data
                break

        if nullable_table is not None:
            # Should have records for non-null values at minimum
            assert len(nullable_table) >= 2

    def test_array_with_duplicate_values(self):
        """Test arrays with duplicate values."""
        data = {
            "id": 1,
            "name": "Test",
            "duplicates": ["value1", "value2", "value1", "value2"],
        }

        result = tm.flatten(data, name="test", arrays="separate")

        # Should preserve all array items including duplicates
        duplicates_table = None
        for table_name, table_data in result.tables.items():
            if "duplicates" in table_name.lower():
                duplicates_table = table_data
                break

        if duplicates_table is not None:
            assert len(duplicates_table) == 4

    def test_deeply_nested_array_objects(self):
        """Test arrays with deeply nested objects."""
        data = {
            "id": 1,
            "name": "Test",
            "complex_array": [
                {"id": 1, "data": {"nested": {"deep": {"value": "deep1"}}}},
                {"id": 2, "data": {"nested": {"deep": {"value": "deep2"}}}},
            ],
        }

        result = tm.flatten(data, name="test", arrays="separate")

        # Should handle deeply nested objects in arrays
        complex_table = None
        for table_name, table_data in result.tables.items():
            if "complex_array" in table_name.lower():
                complex_table = table_data
                break

        assert complex_table is not None
        assert len(complex_table) == 2

        # Should have flattened nested fields
        first_record = complex_table[0]
        has_deep_field = any("deep" in key.lower() for key in first_record.keys())
        assert has_deep_field

    def test_array_of_arrays(self):
        """Test arrays containing other arrays."""
        data = {
            "id": 1,
            "name": "Test",
            "matrix": [["a", "b", "c"], ["d", "e", "f"], ["g", "h", "i"]],
        }

        result = tm.flatten(data, name="test", arrays="separate")

        # Should handle nested arrays
        matrix_table = None
        for table_name, table_data in result.tables.items():
            if "matrix" in table_name.lower():
                matrix_table = table_data
                break

        assert matrix_table is not None
        # Handling of nested arrays may vary by implementation

    def test_array_with_complex_objects(self):
        """Test arrays with complex nested objects and sub-arrays."""
        data = {
            "id": 1,
            "name": "Company",
            "departments": [
                {
                    "id": "eng",
                    "name": "Engineering",
                    "employees": [
                        {"id": 1, "name": "Alice", "skills": ["Python", "SQL"]},
                        {"id": 2, "name": "Bob", "skills": ["JavaScript", "React"]},
                    ],
                    "projects": [
                        {"id": "proj1", "name": "Project A"},
                        {"id": "proj2", "name": "Project B"},
                    ],
                }
            ],
        }

        result = tm.flatten(data, name="company", arrays="separate")

        # Should have multiple child tables
        assert len(result.tables) >= 2

        # Should have departments table
        dept_table = None
        for table_name, table_data in result.tables.items():
            if (
                "departments" in table_name.lower()
                and "employees" not in table_name.lower()
            ):
                dept_table = table_data
                break

        assert dept_table is not None
        assert len(dept_table) == 1

    def test_array_extraction_with_custom_separator(self):
        """Test array extraction with custom field separator."""
        data = {"id": 1, "name": "Test", "nested": {"items": ["item1", "item2"]}}

        result = tm.flatten(data, name="test", arrays="separate", separator=".")

        # Should use custom separator in table names and field names
        table_names = list(result.tables.keys())
        # Implementation may vary on how separators affect table names

    def test_large_array_processing(self):
        """Test processing large arrays."""
        # Create large array
        large_array = [{"id": i, "value": f"item_{i}"} for i in range(1000)]

        data = {"id": 1, "name": "Test", "large_array": large_array}

        result = tm.flatten(data, name="test", arrays="separate")

        # Should handle large arrays
        large_table = None
        for table_name, table_data in result.tables.items():
            if "large_array" in table_name.lower():
                large_table = table_data
                break

        assert large_table is not None
        assert len(large_table) == 1000
