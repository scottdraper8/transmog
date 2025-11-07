"""
Tests for file handling utilities.

Tests file processing utilities, format detection, and file I/O operations.
"""

import json
import tempfile
from pathlib import Path

import pytest

from transmog.config import TransmogConfig
from transmog.error import FileError, ParsingError, ProcessingError
from transmog.process import Processor
from transmog.process.file_handling import (
    detect_input_format,
    process_file,
    process_file_to_format,
)
from transmog.process.result import ProcessingResult


class TestFileFormatDetection:
    """Test file format detection utilities."""

    def test_detect_json_format(self):
        """Test detection of JSON files."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            format_detected = detect_input_format(tmp_path)
            assert format_detected == "json"
        finally:
            Path(tmp_path).unlink()

    def test_detect_jsonl_format(self):
        """Test detection of JSONL files."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            format_detected = detect_input_format(tmp_path)
            assert format_detected == "jsonl"
        finally:
            Path(tmp_path).unlink()

    def test_detect_ndjson_format(self):
        """Test detection of NDJSON files."""
        with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            format_detected = detect_input_format(tmp_path)
            assert format_detected == "jsonl"  # NDJSON maps to JSONL
        finally:
            Path(tmp_path).unlink()

    def test_detect_unknown_format_defaults_to_json(self):
        """Test detection of unknown file formats defaults to JSON."""
        with tempfile.NamedTemporaryFile(suffix=".unknown", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            format_detected = detect_input_format(tmp_path)
            assert format_detected == "json"  # Unknown formats default to JSON
        finally:
            Path(tmp_path).unlink()


class TestFileProcessing:
    """Test file processing utilities."""

    @pytest.fixture
    def processor(self):
        """Create a processor for testing."""
        config = TransmogConfig()
        return Processor(config)

    def test_process_json_file(self, processor):
        """Test processing JSON files."""
        test_data = {"name": "Test", "values": [1, 2, 3]}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            json.dump(test_data, tmp)
            tmp_path = tmp.name

        try:
            result = process_file(processor, tmp_path, "test")
            assert isinstance(result, ProcessingResult)
            assert len(result.main_table) == 1
            assert result.main_table[0]["name"] == "Test"
        finally:
            Path(tmp_path).unlink()

    def test_process_nonexistent_file(self, processor):
        """Test processing nonexistent file."""
        with pytest.raises((FileError, ProcessingError)):
            process_file(processor, "/path/that/does/not/exist.json", "test")


class TestFileToFormatProcessing:
    """Test file processing with output format specification."""

    @pytest.fixture
    def processor(self):
        """Create a processor for testing."""
        config = TransmogConfig()
        return Processor(config)

    def test_process_file_to_csv_format(self, processor):
        """Test processing file and outputting to CSV."""
        test_data = {"name": "Test", "values": [1, 2, 3]}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            json.dump(test_data, tmp)
            tmp_path = tmp.name

        with tempfile.TemporaryDirectory() as output_dir:
            try:
                result = process_file_to_format(
                    processor, tmp_path, "test_entity", "csv", output_dir
                )
                assert result is not None

                output_files = list(Path(output_dir).glob("*.csv"))
                assert len(output_files) > 0
            finally:
                Path(tmp_path).unlink()

    def test_process_file_to_csv(self, processor):
        """Test processing file and outputting to CSV."""
        test_data = {"name": "Test", "age": 30}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            json.dump(test_data, tmp)
            tmp_path = tmp.name

        with tempfile.TemporaryDirectory() as output_dir:
            try:
                result = process_file_to_format(
                    processor, tmp_path, "test_entity", "csv", output_dir
                )
                assert result is not None

                # Check that output files were created
                output_files = list(Path(output_dir).glob("*.csv"))
                assert len(output_files) > 0
            finally:
                Path(tmp_path).unlink()

    def test_process_large_file_streaming(self, processor):
        """Test processing large file uses streaming."""
        large_data = [{"id": i, "value": f"item_{i}"} for i in range(1000)]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            json.dump(large_data, tmp)
            tmp_path = tmp.name

        with tempfile.TemporaryDirectory() as output_dir:
            try:
                result = process_file_to_format(
                    processor, tmp_path, "test_entity", "csv", output_dir
                )
                assert result is not None
            finally:
                Path(tmp_path).unlink()


class TestFileHandlingEdgeCases:
    """Test edge cases in file handling."""

    @pytest.fixture
    def processor(self):
        """Create a processor for testing."""
        config = TransmogConfig()
        return Processor(config)

    def test_empty_json_file(self, processor):
        """Test handling of empty JSON file."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            tmp.write(b"{}")
            tmp_path = tmp.name

        try:
            result = process_file(processor, tmp_path, "empty_test")
            assert result is not None
        finally:
            Path(tmp_path).unlink()

    def test_invalid_json_file(self, processor):
        """Test handling of invalid JSON file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp.write('{"invalid": json, "missing": quote}')
            tmp_path = tmp.name

        try:
            # Should raise ParsingError for invalid JSON content
            with pytest.raises(ParsingError):
                process_file(processor, tmp_path, "invalid_test")
        finally:
            Path(tmp_path).unlink()

    def test_unicode_filename_handling(self, processor):
        """Test handling of unicode filenames."""
        test_data = {"unicode": "test"}
        unicode_filename = "测试_файл_🌍.json"

        with tempfile.TemporaryDirectory() as tmp_dir:
            file_path = Path(tmp_dir) / unicode_filename

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(test_data, f)

            result = process_file(processor, str(file_path), "unicode_test")
            assert result is not None
