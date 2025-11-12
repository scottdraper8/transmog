"""Data iterators for ingesting JSON-based inputs."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from transmog.exceptions import ProcessingError, ValidationError
from transmog.types import RecoveryMode

try:
    import orjson as _orjson  # type: ignore[import-untyped]
except ImportError:
    _orjson = None  # type: ignore[assignment]

if _orjson is not None:
    JSON_DECODE_ERRORS: tuple[type[Exception], ...] = (
        json.JSONDecodeError,
        _orjson.JSONDecodeError,
    )
else:
    JSON_DECODE_ERRORS = (json.JSONDecodeError,)


def get_data_iterator(
    config: Any,
    data: (
        dict[str, Any]
        | list[dict[str, Any]]
        | str
        | Path
        | bytes
        | Iterator[dict[str, Any]]
    ),
) -> Iterator[dict[str, Any]]:
    """Return an iterator over input records.

    Args:
        config: TransmogConfig instance for configuration settings
        data: Input data in various formats

    Returns:
        Iterator over data records
    """
    if isinstance(data, dict):
        return iter([data])
    if isinstance(data, list):
        return iter(data)
    if isinstance(data, Iterator):
        return data

    if isinstance(data, Path):
        data = str(data)

    if isinstance(data, str) and os.path.exists(data):
        extension = os.path.splitext(data)[1].lower()
        if extension in (".jsonl", ".ndjson"):
            return get_jsonl_file_iterator(config, data)
        return get_json_file_iterator(data)

    if isinstance(data, (str, bytes)):
        text = data if isinstance(data, str) else data.decode("utf-8")
        if not text.strip():
            raise ProcessingError("No JSON content provided")

        normalised = _detect_string_format(text)

        if normalised == "jsonl":
            return get_jsonl_data_iterator(config, data)
        if normalised == "json":
            return get_json_data_iterator(data)
        raise ValueError(f"Unsupported input format: {normalised}")

    raise ValidationError(f"Unsupported data type: {type(data)}")


def get_json_data_iterator(
    data: dict[str, Any] | list[dict[str, Any]] | str | bytes,
) -> Iterator[dict[str, Any]]:
    """Iterate over JSON data loaded into memory."""
    if isinstance(data, dict):
        yield data
        return

    if isinstance(data, list):
        for index, item in enumerate(data):
            if not isinstance(item, dict):
                raise ProcessingError(
                    f"Expected JSON object at index {index}, got {type(item).__name__}"
                )
            yield item
        return

    if not isinstance(data, (str, bytes)):
        raise ValidationError("JSON data must be a string, bytes, dict, or list")

    payload = data
    if isinstance(data, str) and not data.strip():
        raise ProcessingError("No JSON content provided")

    try:
        parsed = _loads(payload)
    except JSON_DECODE_ERRORS as exc:
        raise ProcessingError(f"Error parsing JSON data: {exc}") from exc

    yield from _iter_parsed_json(parsed)


def get_json_file_iterator(file_path: str) -> Iterator[dict[str, Any]]:
    """Iterate over records in a JSON file."""
    if not os.path.exists(file_path):
        raise ProcessingError(f"File not found: {file_path}")

    try:
        parsed = _load_json_file(file_path)
    except JSON_DECODE_ERRORS as exc:
        raise ProcessingError(f"Invalid JSON in file {file_path}: {exc}") from exc
    except OSError as exc:
        raise ProcessingError(f"Error reading file {file_path}: {exc}") from exc

    yield from _iter_parsed_json(parsed)


def get_jsonl_file_iterator(config: Any, file_path: str) -> Iterator[dict[str, Any]]:
    """Iterate over records in a JSON Lines file.

    Args:
        config: TransmogConfig instance for configuration settings
        file_path: Path to the JSONL file

    Returns:
        Iterator over data records
    """
    if not os.path.exists(file_path):
        raise ProcessingError(f"File not found: {file_path}")

    try:
        with open(file_path, encoding="utf-8") as handle:
            yield from _iter_jsonl_lines(config, handle, file_path)
    except OSError as exc:
        raise ProcessingError(f"Error reading file {file_path}: {exc}") from exc


def get_jsonl_data_iterator(config: Any, data: str | bytes) -> Iterator[dict[str, Any]]:
    """Iterate over JSON Lines content.

    Args:
        config: TransmogConfig instance for configuration settings
        data: JSONL data as string or bytes

    Returns:
        Iterator over data records
    """
    if not isinstance(data, (str, bytes)):
        raise ValidationError("JSONL data must be a string or bytes")

    text = data if isinstance(data, str) else data.decode("utf-8")
    if not text.strip():
        return

    lines = text.splitlines()
    yield from _iter_jsonl_lines(config, lines, "JSONL data")


def _loads(value: str | bytes) -> Any:
    """Load JSON content using the fastest available parser."""
    if _orjson is not None:
        return _orjson.loads(value)
    if isinstance(value, bytes):
        return json.loads(value.decode("utf-8"))
    return json.loads(value)


def _load_json_file(path: str) -> Any:
    """Load JSON payload from disk."""
    if _orjson is not None:
        with open(path, "rb") as handle:
            return _orjson.loads(handle.read())
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _iter_parsed_json(payload: Any) -> Iterator[dict[str, Any]]:
    """Yield dictionaries from parsed JSON structures."""
    if isinstance(payload, dict):
        yield payload
        return

    if isinstance(payload, list):
        for index, item in enumerate(payload):
            if not isinstance(item, dict):
                raise ProcessingError(
                    f"Expected JSON object at index {index}, got {type(item).__name__}"
                )
            yield item
        return

    raise ProcessingError(
        f"Expected JSON object or list of objects, got {type(payload).__name__}"
    )


def _iter_jsonl_lines(
    config: Any, lines: Iterable[str], source: str
) -> Iterator[dict[str, Any]]:
    """Yield dictionaries from JSON Lines content.

    Args:
        config: TransmogConfig instance for configuration settings
        lines: Iterable of JSONL lines
        source: Source description for error messages

    Returns:
        Iterator over data records
    """
    recovery_mode = (
        config.recovery_mode
        if hasattr(config, "recovery_mode")
        else RecoveryMode.STRICT
    )
    skip_errors = recovery_mode == RecoveryMode.SKIP

    for index, raw_line in enumerate(lines, 1):
        line = raw_line.strip()
        if not line:
            continue

        try:
            record = _loads(line)
        except JSON_DECODE_ERRORS as exc:
            parsing_error = ProcessingError(
                f"Invalid JSON on line {index} in {source}: {exc}"
            )
            if skip_errors:
                continue
            raise parsing_error from exc

        if not isinstance(record, dict):
            parsing_error = ProcessingError(
                f"Expected JSON object on line {index} in {source}, "
                f"got {type(record).__name__}"
            )
            if skip_errors:
                continue
            raise parsing_error

        yield record


def _detect_string_format(value: str) -> str:
    """Detect whether in-memory text is JSON or JSONL."""
    snippet = value.strip()
    if not snippet:
        return "json"
    if "\n" not in snippet:
        return "json"

    candidates = [line.strip() for line in snippet.splitlines() if line.strip()]
    hits = 0
    for line in candidates[:5]:
        if not line.startswith("{"):
            continue
        try:
            _loads(line)
        except JSON_DECODE_ERRORS:
            continue
        hits += 1

    return "jsonl" if hits >= 2 else "json"


__all__ = [
    "get_data_iterator",
    "get_json_data_iterator",
    "get_json_file_iterator",
    "get_jsonl_file_iterator",
    "get_jsonl_data_iterator",
]
