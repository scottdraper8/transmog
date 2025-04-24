"""
JSON Reader module for Transmogrify.

This module provides functions for reading JSON data from various sources
and in different formats, with support for streaming and chunking.
"""

import json
import os
from typing import Any, Dict, List, Optional, Union, Iterator, Generator
import logging

# Try to import optional high-performance JSON libraries
try:
    import orjson

    ORJSON_AVAILABLE = True
except ImportError:
    ORJSON_AVAILABLE = False

logger = logging.getLogger(__name__)


def read_json_file(file_path: str) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Read a JSON file and return its contents.

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

    # Try to use the most efficient JSON parser available
    if ORJSON_AVAILABLE:
        return orjson.loads(content)
    else:
        return json.loads(content)


def read_jsonl_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Read a JSON Lines file (newline-delimited JSON) and return the records.

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

    records = []

    with open(file_path, "r") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue  # Skip empty lines

            try:
                if ORJSON_AVAILABLE:
                    record = orjson.loads(line)
                else:
                    record = json.loads(line)
                records.append(record)
            except json.JSONDecodeError as e:
                raise json.JSONDecodeError(
                    f"Invalid JSON on line {line_num}: {e.msg}", e.doc, e.pos
                )

    return records


def detect_json_format(content: str) -> str:
    """
    Detect the format of a JSON string (regular JSON or JSONL).

    Args:
        content: The JSON content as a string

    Returns:
        "json" for regular JSON, "jsonl" for JSON Lines
    """
    # If there are multiple lines with JSON objects, it's likely JSONL
    content = content.strip()
    if "\n" in content:
        # Check if we have JSON objects on separate lines
        lines = [line.strip() for line in content.split("\n") if line.strip()]

        # Consider it JSONL if first two non-empty lines start with {
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
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Parse JSON data with format auto-detection.

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
        records = []
        for line_num, line in enumerate(data.split("\n"), 1):
            line = line.strip()
            if not line:
                continue

            try:
                if ORJSON_AVAILABLE:
                    record = orjson.loads(line)
                else:
                    record = json.loads(line)
                records.append(record)
            except json.JSONDecodeError as e:
                raise json.JSONDecodeError(
                    f"Invalid JSON on line {line_num}: {e.msg}", e.doc, e.pos
                )
        return records
    else:
        # Parse as regular JSON
        if ORJSON_AVAILABLE:
            return orjson.loads(data)
        else:
            return json.loads(data)


def read_json_stream(
    file_path: str, chunk_size: int = 100, buffer_size: int = 8192
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Read a JSON file in chunks for memory-efficient processing.

    Args:
        file_path: Path to the JSON or JSONL file
        chunk_size: Number of records to yield in each chunk
        buffer_size: Size of the buffer for reading file

    Yields:
        Lists of JSON records in chunks of chunk_size

    Raises:
        FileNotFoundError: If the file doesn't exist
        json.JSONDecodeError: If the file contains invalid JSON
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Check file extension to guess format
    _, ext = os.path.splitext(file_path)
    is_jsonl = ext.lower() in (".jsonl", ".ndjson")

    if is_jsonl:
        # For JSONL, we can process line by line
        chunk = []
        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    if ORJSON_AVAILABLE:
                        record = orjson.loads(line)
                    else:
                        record = json.loads(line)

                    chunk.append(record)

                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []

                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in file {file_path}: {e}")
                    raise

        # Yield any remaining records
        if chunk:
            yield chunk
    else:
        # For regular JSON, we need to read the whole file
        # This is less memory-efficient but necessary for regular JSON
        data = read_json_file(file_path)

        if isinstance(data, list):
            # Yield in chunks
            for i in range(0, len(data), chunk_size):
                yield data[i : i + chunk_size]
        else:
            # Single object, yield as a list with one item
            yield [data]
