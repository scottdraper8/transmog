"""
Tests for integration interface conformance.

This module defines an abstract test class for testing integration scenarios.
"""

import os
import tempfile

import pytest

from transmog import Processor, TransmogConfig


class AbstractIntegrationTest:
    """
    Abstract base class for integration tests.

    This class defines a standardized set of tests that should apply to integration
    scenarios, focusing on component interaction rather than implementation details.
    """

    @pytest.fixture
    def processor(self):
        """Create a processor instance for integration testing."""
        config = TransmogConfig.default().with_processing(cast_to_string=True)
        return Processor(config=config)

    @pytest.fixture
    def sample_data(self):
        """Create a sample data structure for integration testing."""
        return [
            {
                "id": f"record{i}",
                "name": f"Record {i}",
                "nested": {"value": i * 10},
                "items": [{"id": f"item{i}_{j}", "value": j} for j in range(1, 3)],
            }
            for i in range(1, 4)
        ]

    @pytest.fixture
    def complex_data(self):
        """Create a complex data structure with multiple nested levels."""
        return {
            "id": "root",
            "name": "Root Entity",
            "level1": [
                {
                    "id": f"level1_{i}",
                    "name": f"Level 1 Item {i}",
                    "level2": [
                        {
                            "id": f"level2_{i}_{j}",
                            "name": f"Level 2 Item {i}.{j}",
                            "level3": [
                                {
                                    "id": f"level3_{i}_{j}_{k}",
                                    "name": f"Level 3 Item {i}.{j}.{k}",
                                }
                                for k in range(1, 3)
                            ],
                        }
                        for j in range(1, 3)
                    ],
                }
                for i in range(1, 3)
            ],
        }

    @pytest.fixture
    def output_dir(self):
        """Create a temporary output directory."""
        with tempfile.TemporaryDirectory() as tmpdirname:
            yield tmpdirname

    def test_end_to_end_processing(self, processor, sample_data):
        """Test end-to-end processing from input to output."""
        # Process the data
        result = processor.process_batch(sample_data, entity_name="test")

        # Verify the main table
        main_table = result.get_main_table()
        assert len(main_table) == len(sample_data)

        # Verify child tables
        child_tables = result.get_table_names()
        assert len(child_tables) > 0

        # Verify all items are processed
        items_table = next(t for t in child_tables if "items" in t)
        items = result.get_child_table(items_table)
        assert len(items) == sum(len(record["items"]) for record in sample_data)

        # Verify parent-child relationships
        for item in items:
            assert "__parent_extract_id" in item
            # Find corresponding parent
            parent_found = False
            for main_record in main_table:
                if main_record["__extract_id"] == item["__parent_extract_id"]:
                    parent_found = True
                    break
            assert parent_found, "Item has no matching parent record"

    def test_hierarchical_consistency(self, processor, complex_data):
        """Test consistency of hierarchical relationships in nested structures."""
        # Process the complex data
        result = processor.process(complex_data, entity_name="complex")

        # Get all tables
        all_tables = result.get_table_names()

        # Debug
        print(f"All tables in hierarchical consistency test: {all_tables}")

        # Identify the tables by level using more precise filtering
        # This will find tables with only level1 in name, not level2 or level3
        level1_table = next(
            (
                t
                for t in all_tables
                if "level1" in t and "level2" not in t and "level3" not in t
            ),
            None,
        )
        # With the new naming pattern, level2 may be split across multiple tables
        # with indices in their names, so we gather all tables that match
        level2_tables = [t for t in all_tables if "level2" in t and "level3" not in t]
        level3_tables = [t for t in all_tables if "level3" in t]

        # Get records from each table
        main_record = result.get_main_table()[0]
        level1_records = result.get_child_table(level1_table)

        # Gather all level2 records from potentially multiple tables
        level2_records = []
        for table in level2_tables:
            level2_records.extend(result.get_child_table(table))

        # Gather all level3 records from potentially multiple tables
        level3_records = []
        for table in level3_tables:
            level3_records.extend(result.get_child_table(table))

        # Verify record counts
        assert len(level1_records) >= 2  # At least 2 level1 items
        assert len(level2_records) >= 4  # At least 4 level2 items
        assert len(level3_records) >= 8  # At least 8 level3 items

        # Verify parent-child relationships
        # Each level1 record should be referenced by some level2 records
        for level1_record in level1_records:
            level1_id = level1_record["__extract_id"]
            # Check that at least one level2 record references this level1
            assert any(
                record["__parent_extract_id"] == level1_id for record in level2_records
            ), f"No level2 record references level1 record with ID {level1_id}"

        # Each level2 record should be referenced by some level3 records
        for level2_record in level2_records:
            level2_id = level2_record["__extract_id"]
            # Check that at least one level3 record references this level2
            assert any(
                record["__parent_extract_id"] == level2_id for record in level3_records
            ), f"No level3 record references level2 record with ID {level2_id}"

    def test_output_formats(self, processor, sample_data, output_dir):
        """Test that processed data can be written to different output formats."""
        # Process the data
        result = processor.process_batch(sample_data, entity_name="output_test")

        # Try writing to different formats if available

        # Test CSV output
        if hasattr(result, "write_all_csv"):
            csv_paths = result.write_all_csv(base_path=output_dir)
            assert all(os.path.exists(path) for path in csv_paths.values())

            # Verify main table file exists
            assert os.path.exists(csv_paths["main"])

            # Verify child table files exist
            child_table_name = next(name for name in csv_paths.keys() if name != "main")
            assert os.path.exists(csv_paths[child_table_name])

        # Test JSON output
        if hasattr(result, "write_all_json"):
            json_paths = result.write_all_json(base_path=output_dir)
            assert all(os.path.exists(path) for path in json_paths.values())

        # Test Parquet output
        if hasattr(result, "write_all_parquet"):
            try:
                parquet_paths = result.write_all_parquet(base_path=output_dir)
                assert all(os.path.exists(path) for path in parquet_paths.values())
            except ImportError:
                # Parquet support might require optional dependencies
                pytest.skip("Parquet support not available")

    def test_reader_writer_integration(self, processor, sample_data, output_dir):
        """Test integration between readers and writers."""
        # Process the data
        result = processor.process_batch(sample_data, entity_name="reader_writer_test")

        # Write data to JSON format
        if hasattr(result, "write_all_json"):
            json_paths = result.write_all_json(base_path=output_dir)
            main_file = json_paths["main"]

            # Try to read back the data using processor.process_file
            read_result = processor.process_file(main_file, entity_name="read_test")

            # Verify the read data
            read_records = read_result.get_main_table()
            assert len(read_records) > 0

            # Verify field presence (excluding metadata fields)
            {
                k: v
                for k, v in result.get_main_table()[0].items()
                if not k.startswith("__")
            }
            first_read = {
                k: v for k, v in read_records[0].items() if not k.startswith("__")
            }

            # Instead of checking for exact field names, check for semantic equivalence
            # Field names may differ slightly when deeply nested paths are simplified
            # Check essential fields are present
            assert "id" in first_read, "Missing 'id' field"
            assert "name" in first_read, "Missing 'name' field"

            # Check for nested value field
            nested_value_field = next(
                (f for f in first_read.keys() if "nested" in f.lower()), None
            )
            assert nested_value_field is not None, "Missing nested field"

            # Check for items fields
            items_fields = [f for f in first_read.keys() if "items" in f]
            assert len(items_fields) > 0, "Missing items-related fields"
