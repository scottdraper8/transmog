"""JSON Reader module for Transmog.

This module provides functions for reading JSON data from various sources
and in different formats, with support for streaming and chunking.
"""

import json
import logging
import os
from collections.abc import Generator
from typing import Any, Optional, Union, cast

# Optional high-performance JSON library import
try:
    import orjson

    ORJSON_AVAILABLE = True
except ImportError:
    ORJSON_AVAILABLE = False

logger = logging.getLogger(__name__)


class JsonReader:
    """Reader class for standard JSON files.

    Provides methods to read standard JSON files and convert them into dictionaries or
        lists.
    """

    def __init__(self) -> None:
        """Initialize the JSON reader."""
        pass

    def read_file(self, file_path: str) -> Union[dict[str, Any], list[dict[str, Any]]]:
        """Read a JSON file and return its contents.

        Args:
            file_path: Path to the JSON file

        Returns:
            Parsed JSON content as dictionary or list
        """
        return read_json_file(file_path)

    def read_stream(
        self, file_path: str, chunk_size: int = 100
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Read a JSON file in chunks for memory-efficient processing.

        Args:
            file_path: Path to the JSON file
            chunk_size: Number of records to yield in each chunk

        Yields:
            Lists of JSON records in chunks of chunk_size
        """
        yield from read_json_stream(file_path, chunk_size=chunk_size)

    def parse_data(
        self, data: Union[str, bytes]
    ) -> Union[dict[str, Any], list[dict[str, Any]]]:
        """Parse JSON data.

        Args:
            data: JSON content as string or bytes

        Returns:
            Parsed JSON as dictionary or list of dictionaries
        """
        return parse_json_data(data, format_hint="json")


class JsonlReader:
    """Reader class for JSON Lines (JSONL) files.

    Provides methods to read JSONL files (newline-delimited JSON) and convert them into
        lists of dictionaries.
    """

    def __init__(self) -> None:
        """Initialize the JSONL reader."""
        pass

    def read_file(self, file_path: str) -> list[dict[str, Any]]:
        """Read a JSON Lines file and return its contents.

        Args:
            file_path: Path to the JSONL file

        Returns:
            List of parsed JSON records
        """
        return read_jsonl_file(file_path)

    def read_stream(
        self, file_path: str, chunk_size: int = 100
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Read a JSONL file in chunks for memory-efficient processing.

        Args:
            file_path: Path to the JSONL file
            chunk_size: Number of records to yield in each chunk

        Yields:
            Lists of JSON records in chunks of chunk_size
        """
        yield from read_json_stream(file_path, chunk_size=chunk_size)

    def parse_data(self, data: Union[str, bytes]) -> list[dict[str, Any]]:
        """Parse JSONL data.

        Args:
            data: JSONL content as string or bytes

        Returns:
            List of parsed JSON records
        """
        # Ensure return type is always list[dict[str, Any]]
        result = parse_json_data(data, format_hint="jsonl")
        if isinstance(result, dict):
            return [result]
        return result


def read_json_file(file_path: str) -> Union[dict[str, Any], list[dict[str, Any]]]:
    """Read a JSON file and return its contents.

    Args:
        file_path: Path to the JSON file

    Returns:
        Parsed JSON content as dictionary or list

    Raises:
        FileNotFoundError: If the file doesn't exist
        json.JSONDecodeError: If the file contains invalid JSON
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, "rb") as f:
        content = f.read()

    # Use the most efficient JSON parser available
    if ORJSON_AVAILABLE:
        parsed_content: Union[dict[str, Any], list[dict[str, Any]]] = orjson.loads(
            content
        )
        return parsed_content
    else:
        parsed_content = json.loads(content)
        return cast(Union[dict[str, Any], list[dict[str, Any]]], parsed_content)


def read_jsonl_file(file_path: str) -> list[dict[str, Any]]:
    """Read a JSON Lines file (newline-delimited JSON) and return the records.

    Args:
        file_path: Path to the JSONL file

    Returns:
        List of parsed JSON records

    Raises:
        FileNotFoundError: If the file doesn't exist
        json.JSONDecodeError: If any line contains invalid JSON
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    records: list[dict[str, Any]] = []

    with open(file_path) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue  # Skip empty lines

            try:
                if ORJSON_AVAILABLE:
                    record = orjson.loads(line)
                else:
                    record = json.loads(line)
                records.append(cast(dict[str, Any], record))
            except json.JSONDecodeError as e:
                raise json.JSONDecodeError(
                    f"Invalid JSON on line {line_num}: {e.msg}", e.doc, e.pos
                ) from e

    return records


def detect_json_format(content: str) -> str:
    """Detect the format of a JSON string (regular JSON or JSONL).

    Args:
        content: The JSON content as a string

    Returns:
        "json" for regular JSON, "jsonl" for JSON Lines
    """
    # Check for JSONL format (multiple JSON objects on separate lines)
    content = content.strip()
    if "\n" in content:
        # Check for JSON objects on separate lines
        lines = [line.strip() for line in content.split("\n") if line.strip()]

        # Consider it JSONL if first few non-empty lines start with { and end with }
        valid_lines = 0
        for line in lines[:5]:  # Check first 5 non-empty lines
            if line.startswith("{") and (line.endswith("}") or "}" in line):
                valid_lines += 1

        if valid_lines >= 2:
            return "jsonl"

    # Default to regular JSON
    return "json"


def parse_json_data(
    data: Union[str, bytes], format_hint: Optional[str] = None
) -> Union[dict[str, Any], list[dict[str, Any]]]:
    """Parse JSON data with format auto-detection.

    Args:
        data: JSON content as string or bytes
        format_hint: Optional hint about format ("json" or "jsonl")

    Returns:
        Parsed JSON as dictionary or list of dictionaries

    Raises:
        json.JSONDecodeError: If data contains invalid JSON
    """
    if isinstance(data, bytes):
        data = data.decode("utf-8")

    # Detect format if not provided
    if not format_hint:
        format_hint = detect_json_format(data)

    if format_hint == "jsonl":
        # Parse as JSONL
        records: list[dict[str, Any]] = []
        for line_num, line in enumerate(data.split("\n"), 1):
            line = line.strip()
            if not line:
                continue

            try:
                if ORJSON_AVAILABLE:
                    record = orjson.loads(line)
                else:
                    record = json.loads(line)
                records.append(cast(dict[str, Any], record))
            except json.JSONDecodeError as e:
                raise json.JSONDecodeError(
                    f"Invalid JSON on line {line_num}: {e.msg}", e.doc, e.pos
                ) from e

        return records
    else:
        # Parse as standard JSON
        try:
            if ORJSON_AVAILABLE:
                parsed_data: Union[dict[str, Any], list[dict[str, Any]]] = orjson.loads(
                    data
                )
                return parsed_data
            else:
                parsed_data = json.loads(data)
                return cast(Union[dict[str, Any], list[dict[str, Any]]], parsed_data)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON: {e.msg}", e.doc, e.pos) from e


def read_json_stream(
    file_path: str, chunk_size: int = 100, buffer_size: int = 8192
) -> Generator[list[dict[str, Any]], None, None]:
    """Read a JSON file in streaming fashion, yielding records in chunks.

    This function handles both standard JSON arrays and JSONL (newline-delimited JSON).
    For standard JSON, it assumes the file contains a top-level array of objects.
    For JSONL, it processes one record per line.

    Args:
        file_path: Path to the JSON file
        chunk_size: Number of records to yield in each chunk
        buffer_size: Size of the read buffer in bytes

    Yields:
        Chunks of JSON records as lists of dictionaries

    Raises:
        FileNotFoundError: If the file doesn't exist
        json.JSONDecodeError: If the file contains invalid JSON
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Determine format by examining file beginning
    with open(file_path) as f:
        header = f.read(1024)
        format_type = detect_json_format(header)

    # Process file according to its format
    if format_type == "jsonl":
        # Process as JSONL (Line-delimited JSON)
        with open(file_path) as f:
            buffer: list[dict[str, Any]] = []
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    if ORJSON_AVAILABLE:
                        record = orjson.loads(line)
                    else:
                        record = json.loads(line)
                    buffer.append(cast(dict[str, Any], record))

                    if len(buffer) >= chunk_size:
                        yield buffer
                        buffer = []
                except json.JSONDecodeError as e:
                    raise json.JSONDecodeError(
                        f"Invalid JSON in file {file_path}: {e.msg}", e.doc, e.pos
                    ) from e

            # Yield any remaining records
            if buffer:
                yield buffer
    else:
        # Process as standard JSON (assuming an array of objects)
        result: Union[dict[str, Any], list[dict[str, Any]]] = read_json_file(file_path)

        # Ensure working with a list of records
        records: list[dict[str, Any]] = []
        if isinstance(result, dict):
            records = [result]
        else:
            records = result

        # Yield records in chunks
        for i in range(0, len(records), chunk_size):
            yield records[i : i + chunk_size]
