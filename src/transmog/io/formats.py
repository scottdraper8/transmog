"""Format detection for IO operations.

This module provides format detection utilities.
"""

from typing import Any


def detect_format(data_source: Any) -> str:
    """Detect the format of a data source.

    Args:
        data_source: Data source to examine

    Returns:
        Detected format name or 'unknown'
    """
    import os

    if isinstance(data_source, str) and os.path.isfile(data_source):
        ext = os.path.splitext(data_source)[1].lower()
        if ext == ".json":
            return "json"
        elif ext in (".jsonl", ".ndjson"):
            return "jsonl"
        elif ext == ".parquet":
            return "parquet"
        return "unknown"

    if isinstance(data_source, (dict, list)):
        return "json"

    if isinstance(data_source, (str, bytes)):
        if isinstance(data_source, bytes):
            sample = data_source[:1000].decode("utf-8", errors="ignore")
        else:
            sample = data_source[:1000]

        sample = sample.strip()
        if sample.startswith(("{", "[")):
            if "\n{" in sample or "\n[" in sample:
                return "jsonl"
            return "json"

    return "unknown"
