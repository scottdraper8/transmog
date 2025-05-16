"""
Integration tests for the Transmog processor.

This module tests the complete data processing pipeline with real data and real writers.
"""

import json
import os

import pytest

from transmog import Processor


@pytest.mark.integration
class TestProcessorIntegration:
    """Integration tests for the Processor class."""

    def test_process_and_write(self, complex_data, tmp_path, dependency_status):
        """Test processing data and writing to multiple formats."""
        # Create processor with default configuration
        processor = Processor()

        # Process data
        result = processor.process(complex_data, entity_name="test_entity")

        # Verify processing result structure
        main_table = result.get_main_table()
        assert main_table, "Main table should not be empty"
        assert isinstance(main_table, list), "Main table should be a list"
        assert len(main_table) == 1, "Main table should contain one record"

        # Test each available format
        formats_to_test = ["json"]

        # Add optional formats if available
        if dependency_status["pyarrow"]:
            formats_to_test.append("parquet")

        # Create output directories for each format
        output_dirs = {fmt: tmp_path / fmt for fmt in formats_to_test}
        for output_dir in output_dirs.values():
            os.makedirs(output_dir, exist_ok=True)

        # Write to each format
        for fmt in formats_to_test:
            # Get the write method
            write_method = getattr(result, f"write_all_{fmt}")

            # Write output
            output_files = write_method(base_path=output_dirs[fmt])

            # Verify output files
            assert "main" in output_files, f"Missing main table in {fmt} output"
            assert os.path.exists(output_files["main"]), (
                f"Main table file not found for {fmt}"
            )

            # Verify all tables from get_table_names() are in the output files
            for table_name in result.get_table_names():
                if table_name == "main":  # Skip main, it's already checked
                    continue

                # Find a matching output file - it might have a reformatted name
                table_found = False
                formatted_name = result.get_formatted_table_name(table_name)
                for output_table_name in output_files.keys():
                    if output_table_name == "main":
                        continue

                    if (
                        table_name == output_table_name
                        or formatted_name in output_files[output_table_name]
                    ):
                        table_found = True
                        assert os.path.exists(output_files[output_table_name]), (
                            f"Table file not found: {output_table_name}"
                        )
                        break

                assert table_found, f"Missing table {table_name} in {fmt} output"

    def test_processing_modes(self, complex_data, tmp_path):
        """Test different processing modes."""
        # Create processors with different modes
        default_processor = Processor()
        memory_processor = Processor.memory_optimized()
        performance_processor = Processor.performance_optimized()

        # Process with each processor
        default_result = default_processor.process(complex_data, entity_name="default")
        memory_result = memory_processor.process(complex_data, entity_name="memory")
        performance_result = performance_processor.process(
            complex_data, entity_name="performance"
        )

        # Create output directory
        output_dir = tmp_path / "modes"
        os.makedirs(output_dir, exist_ok=True)

        # Write results to JSON for comparison
        default_path = output_dir / "default.json"
        memory_path = output_dir / "memory.json"
        performance_path = output_dir / "performance.json"

        with open(default_path, "w") as f:
            json.dump(default_result.to_dict(), f, default=str)

        with open(memory_path, "w") as f:
            json.dump(memory_result.to_dict(), f, default=str)

        with open(performance_path, "w") as f:
            json.dump(performance_result.to_dict(), f, default=str)

        # Load results back
        with open(default_path) as f:
            default_data = json.load(f)

        with open(memory_path) as f:
            memory_data = json.load(f)

        with open(performance_path) as f:
            performance_data = json.load(f)

        # Verify all modes produce output
        assert "main_table" in default_data
        assert "main_table" in memory_data
        assert "main_table" in performance_data

        # Verify all record counts match
        assert len(default_data["main_table"]) == len(memory_data["main_table"])
        assert len(default_data["main_table"]) == len(performance_data["main_table"])

    def test_deterministic_ids(self, complex_data, tmp_path):
        """Test deterministic ID generation."""
        # Create processor with deterministic ID field
        processor = Processor.with_deterministic_ids("id")

        # Process the data twice - should get the same IDs
        result1 = processor.process(complex_data, entity_name="test")
        result2 = processor.process(complex_data, entity_name="test")

        # Get main tables
        main1 = result1.get_main_table()
        main2 = result2.get_main_table()

        # Extract IDs
        id1 = main1[0]["__extract_id"]
        id2 = main2[0]["__extract_id"]

        # IDs should match
        assert id1 == id2, "Deterministic IDs should be the same across runs"

        # We can only verify tables that use the "id" field for deterministic IDs
        # Other tables will use random UUIDs
        common_tables = []
        for table_name in result1.get_table_names():
            if table_name == "main":
                continue

            table1 = result1.get_child_table(table_name)
            table2 = result2.get_child_table(table_name)

            # Only check tables where all records have an "id" field
            if all("id" in record for record in table1) and all(
                "id" in record for record in table2
            ):
                common_tables.append(table_name)

                # Sort records by id for comparison
                sorted_table1 = sorted(table1, key=lambda x: x["id"])
                sorted_table2 = sorted(table2, key=lambda x: x["id"])

                # Compare IDs for each record
                for i in range(len(sorted_table1)):
                    if (
                        i < len(sorted_table2)
                        and sorted_table1[i]["id"] == sorted_table2[i]["id"]
                    ):
                        child_id1 = sorted_table1[i]["__extract_id"]
                        child_id2 = sorted_table2[i]["__extract_id"]
                        assert child_id1 == child_id2, (
                            f"Deterministic IDs should match for {table_name} record with id={sorted_table1[i]['id']}"
                        )

    def test_process_file(self, json_file, tmp_path):
        """Test processing a file."""
        # Create processor
        processor = Processor()

        # Process the file
        result = processor.process_file(json_file, entity_name="file_test")

        # Verify processing result
        main_table = result.get_main_table()
        assert main_table, "Main table should not be empty"

        # Write to JSON
        output_dir = tmp_path / "file_output"
        os.makedirs(output_dir, exist_ok=True)

        output_files = result.write_all_json(base_path=output_dir)

        # Verify output
        assert "main" in output_files
        assert os.path.exists(output_files["main"])

    def test_process_jsonl(self, jsonl_file, tmp_path):
        """Test processing a JSONL file."""
        # Create processor
        processor = Processor()

        # Process the file
        result = processor.process_file(jsonl_file, entity_name="jsonl_test")

        # Verify processing result
        main_table = result.get_main_table()
        assert main_table, "Main table should not be empty"
        assert len(main_table) > 1, (
            "Multiple records should be processed from JSONL file"
        )

        # Write to JSON
        output_dir = tmp_path / "jsonl_output"
        os.makedirs(output_dir, exist_ok=True)

        output_files = result.write_all_json(base_path=output_dir)

        # Verify output
        assert "main" in output_files
        assert os.path.exists(output_files["main"])

    def test_process_csv(self, csv_file, tmp_path):
        """Test processing a CSV file."""
        # Create processor
        processor = Processor()

        # Process the file
        result = processor.process_csv(
            csv_file, entity_name="csv_test", delimiter=",", has_header=True
        )

        # Verify processing result
        main_table = result.get_main_table()
        assert main_table, "Main table should not be empty"
        assert len(main_table) > 1, "Multiple records should be processed from CSV file"

        # Write to JSON
        output_dir = tmp_path / "csv_output"
        os.makedirs(output_dir, exist_ok=True)

        output_files = result.write_all_json(base_path=output_dir)

        # Verify output
        assert "main" in output_files
        assert os.path.exists(output_files["main"])

    def test_process_chunked(self, complex_batch, tmp_path):
        """Test processing data in chunks."""
        # Create processor
        processor = Processor()

        # Process in chunks
        result = processor.process_chunked(
            complex_batch,
            entity_name="chunk_test",
            chunk_size=2,  # Small chunk size to ensure multiple chunks
        )

        # Verify processing result
        main_table = result.get_main_table()
        assert main_table, "Main table should not be empty"
        assert len(main_table) == len(complex_batch), "All records should be processed"

        # Check for child tables (from nested arrays)
        table_names = result.get_table_names()
        assert len(table_names) > 0, "Should have extracted child tables"

        # Write to JSON
        output_dir = tmp_path / "chunked_output"
        os.makedirs(output_dir, exist_ok=True)

        output_files = result.write_all_json(base_path=output_dir)

        # Verify output
        assert "main" in output_files
        assert os.path.exists(output_files["main"])

        # Verify child tables
        for table_name in table_names:
            assert table_name in output_files
            assert os.path.exists(output_files[table_name])
