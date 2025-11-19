"""Data iterators for ingesting JSON-based inputs."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from transmog.exceptions import ValidationError

try:
    import orjson as _orjson  # type: ignore[import-untyped]
except ImportError:
    _orjson = None  # type: ignore[assignment]

try:
    import json5 as _json5  # type: ignore[import-untyped]
except ImportError:
    _json5 = None  # type: ignore[assignment]

try:
    import hjson as _hjson  # type: ignore[import-untyped]
except ImportError:
    _hjson = None  # type: ignore[assignment]

if _orjson is not None:
    JSON_DECODE_ERRORS: tuple[type[Exception], ...] = (
        json.JSONDecodeError,
        _orjson.JSONDecodeError,
    )
else:
    JSON_DECODE_ERRORS = (json.JSONDecodeError,)


def get_data_iterator(
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
            return get_jsonl_file_iterator(data)
        if extension == ".json5":
            return get_json5_file_iterator(data)
        if extension == ".hjson":
            return get_hjson_file_iterator(data)
        return get_json_file_iterator(data)

    if isinstance(data, (str, bytes)):
        text = data if isinstance(data, str) else data.decode("utf-8")
        if not text.strip():
            raise ValidationError("No JSON content provided")

        normalised = _detect_string_format(text)

        if normalised == "jsonl":
            return get_jsonl_data_iterator(data)
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
                raise ValidationError(
                    f"Expected JSON object at index {index}, got {type(item).__name__}"
                )
            yield item
        return

    if not isinstance(data, (str, bytes)):
        raise ValidationError("JSON data must be a string, bytes, dict, or list")

    payload = data
    if isinstance(data, str) and not data.strip():
        raise ValidationError("No JSON content provided")

    try:
        parsed = _loads(payload)
    except JSON_DECODE_ERRORS as exc:
        raise ValidationError(f"Error parsing JSON data: {exc}") from exc

    yield from _iter_parsed_json(parsed)


def get_json_file_iterator(file_path: str) -> Iterator[dict[str, Any]]:
    """Iterate over records in a JSON file."""
    if not os.path.exists(file_path):
        raise ValidationError(f"File not found: {file_path}")

    try:
        parsed = _load_json_file(file_path)
    except JSON_DECODE_ERRORS as exc:
        raise ValidationError(f"Invalid JSON in file {file_path}: {exc}") from exc
    except OSError as exc:
        raise ValidationError(f"Error reading file {file_path}: {exc}") from exc

    yield from _iter_parsed_json(parsed)


def get_jsonl_file_iterator(file_path: str) -> Iterator[dict[str, Any]]:
    """Iterate over records in a JSON Lines file.

    Args:
        file_path: Path to the JSONL file

    Returns:
        Iterator over data records
    """
    if not os.path.exists(file_path):
        raise ValidationError(f"File not found: {file_path}")

    try:
        with open(file_path, encoding="utf-8") as handle:
            yield from _iter_jsonl_lines(handle, file_path)
    except OSError as exc:
        raise ValidationError(f"Error reading file {file_path}: {exc}") from exc


def get_jsonl_data_iterator(data: str | bytes) -> Iterator[dict[str, Any]]:
    """Iterate over JSON Lines content.

    Args:
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
    yield from _iter_jsonl_lines(lines, "JSONL data")


def get_json5_file_iterator(file_path: str) -> Iterator[dict[str, Any]]:
    """Iterate over records in a JSON5 file.

    Args:
        file_path: Path to the JSON5 file

    Returns:
        Iterator over data records
    """
    if not os.path.exists(file_path):
        raise ValidationError(f"File not found: {file_path}")

    if _json5 is None:
        raise ValidationError(
            "json5 library is required for .json5 files. "
            "Install with: pip install json5"
        )

    try:
        parsed = _load_json5_file(file_path)
    except ValueError as exc:
        raise ValidationError(f"Invalid JSON5 in file {file_path}: {exc}") from exc
    except OSError as exc:
        raise ValidationError(f"Error reading file {file_path}: {exc}") from exc

    yield from _iter_parsed_json(parsed)


def get_hjson_file_iterator(file_path: str) -> Iterator[dict[str, Any]]:
    """Iterate over records in an HJSON file.

    Args:
        file_path: Path to the HJSON file

    Returns:
        Iterator over data records
    """
    if not os.path.exists(file_path):
        raise ValidationError(f"File not found: {file_path}")

    if _hjson is None:
        raise ValidationError(
            "hjson library is required for .hjson files. "
            "Install with: pip install hjson"
        )

    try:
        parsed = _load_hjson_file(file_path)
    except ValueError as exc:
        raise ValidationError(f"Invalid HJSON in file {file_path}: {exc}") from exc
    except OSError as exc:
        raise ValidationError(f"Error reading file {file_path}: {exc}") from exc

    yield from _iter_parsed_json(parsed)


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


def _load_json5_file(path: str) -> Any:
    """Load JSON5 payload from disk."""
    with open(path, encoding="utf-8") as handle:
        return _json5.load(handle)


def _load_hjson_file(path: str) -> Any:
    """Load HJSON payload from disk."""
    with open(path, encoding="utf-8") as handle:
        return _hjson.load(handle)


def _iter_parsed_json(payload: Any) -> Iterator[dict[str, Any]]:
    """Yield dictionaries from parsed JSON structures."""
    if isinstance(payload, dict):
        yield payload
        return

    if isinstance(payload, list):
        for index, item in enumerate(payload):
            if not isinstance(item, dict):
                raise ValidationError(
                    f"Expected JSON object at index {index}, got {type(item).__name__}"
                )
            yield item
        return

    raise ValidationError(
        f"Expected JSON object or list of objects, got {type(payload).__name__}"
    )


def _iter_jsonl_lines(lines: Iterable[str], source: str) -> Iterator[dict[str, Any]]:
    """Yield dictionaries from JSON Lines content.

    Args:
        lines: Iterable of JSONL lines
        source: Source description for error messages

    Returns:
        Iterator over data records
    """
    for index, raw_line in enumerate(lines, 1):
        line = raw_line.strip()
        if not line:
            continue

        try:
            record = _loads(line)
        except JSON_DECODE_ERRORS as exc:
            raise ValidationError(
                f"Invalid JSON on line {index} in {source}: {exc}"
            ) from exc

        if not isinstance(record, dict):
            raise ValidationError(
                f"Expected JSON object on line {index} in {source}, "
                f"got {type(record).__name__}"
            )

        yield record


def _detect_string_format(value: str) -> str:
    """Detect whether in-memory text is JSON or JSONL."""
    snippet = value.strip()
    if not snippet or "\n" not in snippet:
        return "json"

    hits = 0
    checked = 0
    for line in snippet.splitlines():
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            _loads(line)
            hits += 1
        except JSON_DECODE_ERRORS:
            pass

        checked += 1
        if checked >= 5:
            break

    return "jsonl" if hits >= 2 else "json"


__all__ = [
    "get_data_iterator",
    "get_json_data_iterator",
    "get_json_file_iterator",
    "get_jsonl_file_iterator",
    "get_jsonl_data_iterator",
    "get_json5_file_iterator",
    "get_hjson_file_iterator",
]
