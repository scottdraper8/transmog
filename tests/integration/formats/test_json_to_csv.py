"""
Integration tests for JSON to CSV conversion.

Tests complete workflows converting JSON data to CSV format.
"""

import json
import tempfile
from pathlib import Path

import pytest

import transmog as tm


class TestJsonToCsvConversion:
    """Test JSON to CSV conversion workflows."""

    def test_simple_json_to_csv(self, simple_data, output_dir):
        """Test converting simple JSON to CSV."""
        # Flatten JSON data
        result = tm.flatten(simple_data, name="simple")

        # Save as CSV
        csv_paths = result.save(str(output_dir / "simple_csv"), format="csv")

        # Verify CSV files were created
        if isinstance(csv_paths, dict):
            csv_files = list(csv_paths.values())
        else:
            csv_files = csv_paths if isinstance(csv_paths, list) else [csv_paths]

        assert len(csv_files) > 0

        # Verify main CSV file exists and has content
        main_csv = Path(csv_files[0])
        assert main_csv.exists()
        assert main_csv.stat().st_size > 0

        # Read and verify CSV content
        import csv

        with open(main_csv) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 1
            assert rows[0]["name"] == "Test Entity"
            assert "metadata_created_at" in rows[0]

    def test_nested_json_to_csv(self, complex_nested_data, output_dir):
        """Test converting nested JSON to CSV."""
        # Flatten complex nested JSON
        result = tm.flatten(complex_nested_data, name="organization", arrays="separate")

        # Save as CSV
        csv_paths = result.save(str(output_dir / "nested_csv"), format="csv")

        # Should create multiple CSV files for nested arrays
        if isinstance(csv_paths, dict):
            assert len(csv_paths) > 1  # Main table + child tables

            # Verify main organization CSV
            main_csv = None
            for table_name, file_path in csv_paths.items():
                if table_name == "organization" or "organization" in table_name.lower():
                    main_csv = Path(file_path)
                    break

            assert main_csv is not None
            assert main_csv.exists()

            # Verify departments CSV exists
            dept_csv = None
            for table_name, file_path in csv_paths.items():
                if "departments" in table_name.lower():
                    dept_csv = Path(file_path)
                    break

            if dept_csv:
                assert dept_csv.exists()

    def test_array_json_to_csv(self, array_data, output_dir):
        """Test converting JSON with arrays to CSV."""
        # Flatten JSON with arrays
        result = tm.flatten(array_data, name="company", arrays="separate")

        # Save as CSV
        csv_paths = result.save(str(output_dir / "array_csv"), format="csv")

        if isinstance(csv_paths, dict):
            # Should have main company CSV
            company_csv = None
            employees_csv = None
            tags_csv = None

            for table_name, file_path in csv_paths.items():
                if table_name == "company":
                    company_csv = Path(file_path)
                elif table_name == "company_employees":
                    employees_csv = Path(file_path)
                elif table_name == "company_tags":
                    tags_csv = Path(file_path)

            # Verify main CSV
            if company_csv:
                assert company_csv.exists()

                import csv

                with open(company_csv) as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    assert len(rows) == 1
                    assert rows[0]["name"] == "Company"

            # Verify employees CSV
            if employees_csv:
                assert employees_csv.exists()

                import csv

                with open(employees_csv) as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    assert len(rows) == 2  # Alice and Bob

    def test_batch_json_to_csv(self, batch_data, output_dir):
        """Test converting batch JSON data to CSV."""
        # Flatten batch data
        result = tm.flatten(batch_data, name="records")

        # Save as CSV
        csv_paths = result.save(str(output_dir / "batch_csv"), format="csv")

        # Verify main CSV has all records
        if isinstance(csv_paths, dict):
            main_csv = Path(list(csv_paths.values())[0])
        else:
            main_csv = Path(csv_paths[0] if isinstance(csv_paths, list) else csv_paths)

        assert main_csv.exists()

        import csv

        with open(main_csv) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == len(batch_data)  # All batch records
            assert rows[0]["name"] == "Record 1"
            assert rows[-1]["name"] == f"Record {len(batch_data)}"

    def test_json_file_to_csv(self, json_file, output_dir):
        """Test converting JSON file directly to CSV."""
        # Use flatten_file to process JSON file
        result = tm.flatten_file(json_file, name="from_file")

        # Save as CSV
        csv_paths = result.save(str(output_dir / "file_csv"), format="csv")

        # Verify conversion
        if isinstance(csv_paths, dict):
            main_csv = Path(list(csv_paths.values())[0])
        else:
            main_csv = Path(csv_paths[0] if isinstance(csv_paths, list) else csv_paths)

        assert main_csv.exists()

        import csv

        with open(main_csv) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 1
            assert rows[0]["name"] == "Test Entity"

    def test_large_json_to_csv(self, large_json_file, output_dir):
        """Test converting large JSON file to CSV."""
        # Process large JSON file
        result = tm.flatten_file(large_json_file, name="large_data")

        # Save as CSV
        csv_paths = result.save(str(output_dir / "large_csv"), format="csv")

        # Verify large CSV was created
        if isinstance(csv_paths, dict):
            main_csv = Path(list(csv_paths.values())[0])
        else:
            main_csv = Path(csv_paths[0] if isinstance(csv_paths, list) else csv_paths)

        assert main_csv.exists()
        assert main_csv.stat().st_size > 1000  # Should be substantial file

        # Verify record count
        import csv

        with open(main_csv) as f:
            reader = csv.DictReader(f)
            row_count = sum(1 for _ in reader)

            assert row_count == 1000  # From large_json_file fixture


class TestJsonToCsvOptions:
    """Test JSON to CSV conversion with various options."""

    def test_json_to_csv_with_custom_separator(self, simple_data, output_dir):
        """Test JSON to CSV conversion with custom field separator."""
        # Flatten with dot separator
        result = tm.flatten(simple_data, name="dot_sep", separator=".")

        # Save as CSV
        csv_paths = result.save(str(output_dir / "dot_csv"), format="csv")

        # Verify CSV has dot-separated field names
        if isinstance(csv_paths, dict):
            main_csv = Path(list(csv_paths.values())[0])
        else:
            main_csv = Path(csv_paths[0] if isinstance(csv_paths, list) else csv_paths)

        import csv

        with open(main_csv) as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames

            # Should have dot-separated headers
            assert any("metadata.created_at" in header for header in headers)

    def test_json_to_csv_with_error_handling(self, problematic_data, output_dir):
        """Test JSON to CSV conversion with error handling."""
        # Flatten with error handling
        result = tm.flatten(problematic_data, name="error_test", errors="skip")

        # Save as CSV
        csv_paths = result.save(str(output_dir / "error_csv"), format="csv")

        # Should create CSV with valid records
        if isinstance(csv_paths, dict):
            main_csv = Path(list(csv_paths.values())[0])
        else:
            main_csv = Path(csv_paths[0] if isinstance(csv_paths, list) else csv_paths)

        assert main_csv.exists()

        import csv

        with open(main_csv) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # Should have at least one valid record
            assert len(rows) >= 1

    def test_json_to_csv_preserve_types(self, mixed_types_data, output_dir):
        """Test JSON to CSV conversion with type preservation."""
        # Flatten with type preservation
        result = tm.flatten(mixed_types_data, name="types", preserve_types=True)

        # Save as CSV
        csv_paths = result.save(str(output_dir / "types_csv"), format="csv")

        # Verify CSV was created
        if isinstance(csv_paths, dict):
            main_csv = Path(list(csv_paths.values())[0])
        else:
            main_csv = Path(csv_paths[0] if isinstance(csv_paths, list) else csv_paths)

        assert main_csv.exists()

        import csv

        with open(main_csv) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 1
            # CSV will have string representations, but should be valid
            assert rows[0]["name"] == "Mixed Types Test"

    def test_json_to_csv_with_natural_ids(self, simple_data, output_dir):
        """Test JSON to CSV conversion with natural ID fields."""
        # Flatten with natural ID
        result = tm.flatten(simple_data, name="natural", id_field="id")

        # Save as CSV
        csv_paths = result.save(str(output_dir / "natural_csv"), format="csv")

        # Verify CSV has natural ID
        if isinstance(csv_paths, dict):
            main_csv = Path(list(csv_paths.values())[0])
        else:
            main_csv = Path(csv_paths[0] if isinstance(csv_paths, list) else csv_paths)

        import csv

        with open(main_csv) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 1
            assert "id" in rows[0]
            assert rows[0]["id"] == "1"


class TestJsonToCsvEdgeCases:
    """Test edge cases in JSON to CSV conversion."""

    def test_empty_json_to_csv(self, output_dir):
        """Test converting empty JSON to CSV."""
        empty_data = {}

        result = tm.flatten(empty_data, name="empty")

        # Save as CSV
        csv_paths = result.save(str(output_dir / "empty_csv"), format="csv")

        # Should create empty CSV or handle gracefully
        if isinstance(csv_paths, dict):
            main_csv = Path(list(csv_paths.values())[0])
        else:
            main_csv = Path(csv_paths[0] if isinstance(csv_paths, list) else csv_paths)

        # File should exist (even if empty)
        assert main_csv.exists()

    def test_json_with_special_characters_to_csv(self, output_dir):
        """Test converting JSON with special characters to CSV."""
        special_data = {
            "name": "Test with émojis 🚀",
            "description": "Line 1\nLine 2\tTabbed",
            "quote": 'He said "Hello"',
            "comma": "Value, with, commas",
        }

        result = tm.flatten(special_data, name="special")

        # Save as CSV
        csv_paths = result.save(str(output_dir / "special_csv"), format="csv")

        # Verify CSV handles special characters
        if isinstance(csv_paths, dict):
            main_csv = Path(list(csv_paths.values())[0])
        else:
            main_csv = Path(csv_paths[0] if isinstance(csv_paths, list) else csv_paths)

        assert main_csv.exists()

        import csv

        with open(main_csv, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 1
            # CSV should preserve special characters
            assert "émojis 🚀" in rows[0]["name"]
            assert "commas" in rows[0]["comma"]

    def test_deeply_nested_json_to_csv(self, output_dir):
        """Test converting deeply nested JSON to CSV."""
        # Create deeply nested structure
        deep_data = {"level0": {}}
        current = deep_data["level0"]

        for i in range(10):  # 10 levels deep
            current[f"level{i + 1}"] = {"value": f"level_{i + 1}"}
            current = current[f"level{i + 1}"]

        result = tm.flatten(deep_data, name="deep")

        # Save as CSV
        csv_paths = result.save(str(output_dir / "deep_csv"), format="csv")

        # Should handle deep nesting
        if isinstance(csv_paths, dict):
            main_csv = Path(list(csv_paths.values())[0])
        else:
            main_csv = Path(csv_paths[0] if isinstance(csv_paths, list) else csv_paths)

        assert main_csv.exists()

        import csv

        with open(main_csv) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 1
            # Should have flattened deep fields with simplification for deeply nested structures
            deep_fields = [h for h in reader.fieldnames if "level" in h]
            assert len(deep_fields) >= 3  # Should have simplified deep structure
            # Should contain "nested" indicator for deeply nested paths
            nested_fields = [h for h in reader.fieldnames if "nested" in h]
            assert len(nested_fields) >= 1  # Should have nested simplification

    def test_json_with_null_values_to_csv(self, output_dir):
        """Test converting JSON with null values to CSV."""
        null_data = {
            "id": 1,
            "name": "Test",
            "null_field": None,
            "nested": {"null_nested": None, "value": "valid"},
        }

        result = tm.flatten(null_data, name="nulls")

        # Save as CSV
        csv_paths = result.save(str(output_dir / "nulls_csv"), format="csv")

        # Verify CSV handles null values
        if isinstance(csv_paths, dict):
            main_csv = Path(list(csv_paths.values())[0])
        else:
            main_csv = Path(csv_paths[0] if isinstance(csv_paths, list) else csv_paths)

        assert main_csv.exists()

        import csv

        with open(main_csv) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 1
            assert rows[0]["name"] == "Test"
            assert rows[0]["nested_value"] == "valid"
            # Null values are filtered out during flattening (expected behavior)
            assert "null_field" not in reader.fieldnames
            assert "nested_null_nested" not in reader.fieldnames
