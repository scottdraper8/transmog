#!/usr/bin/env python3
"""Inventory corpus value categories, not behavioral correctness or coverage.

The inventory inspects parsed values and numeric token syntax. Executed
fixture-to-sensor links are audited separately by pytest --audit-corpus.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

CORPUS_DIR = Path(__file__).resolve().parents[1] / "tests" / "corpus"
REQUIRED_TYPES = {
    "null_scalar": "null as an object value",
    "null_in_array": "null as an array element",
    "bool_true": "boolean true",
    "bool_false": "boolean false",
    "int_positive": "positive integer",
    "int_zero": "integer zero",
    "int_negative": "negative integer",
    "number_float": "a floating-point token",
    "float_fractional": "a finite non-integral float",
    "float_exponent": "a float token containing an exponent",
    "float_negative_zero": "negative floating-point zero",
    "string_ascii": "ASCII string",
    "string_unicode": "Unicode string",
    "string_empty": "empty string",
    "array_empty": "empty array",
    "array_primitive": "array of scalar values",
    "array_object": "array of objects",
    "array_mixed": "array of different structural kinds",
    "array_nested": "array containing arrays",
    "array_all_null": "nonempty all-null array",
    "object_empty": "empty object",
    "object_nested": "nested nonempty object",
    "records_sparse": "records with different key sets",
}


def _classify(value: Any, in_array: bool = False) -> set[str]:
    """Classify recursive value categories without conflating nested arrays."""
    if value is None:
        return {"null_in_array" if in_array else "null_scalar"}
    if isinstance(value, bool):
        return {"bool_true" if value else "bool_false"}
    if isinstance(value, int):
        return {
            "int_positive" if value > 0 else "int_negative" if value < 0 else "int_zero"
        }
    if isinstance(value, float):
        tags = {"number_float"}
        if math.isfinite(value) and not value.is_integer():
            tags.add("float_fractional")
        if value == 0 and math.copysign(1, value) < 0:
            tags.add("float_negative_zero")
        return tags
    if isinstance(value, str):
        return {
            "string_empty"
            if not value
            else "string_ascii"
            if value.isascii()
            else "string_unicode"
        }
    tags: set[str] = set()
    if isinstance(value, list):
        if not value:
            return {"array_empty"}
        kinds = {
            "object"
            if isinstance(item, dict)
            else "array"
            if isinstance(item, list)
            else "scalar"
            for item in value
        }
        if len(kinds) > 1:
            tags.add("array_mixed")
        if "array" in kinds:
            tags.add("array_nested")
        if kinds == {"object"}:
            tags.add("array_object")
        if kinds == {"scalar"}:
            tags.add("array_primitive")
        if all(item is None for item in value):
            tags.add("array_all_null")
        for item in value:
            tags.update(_classify(item, in_array=True))
    elif isinstance(value, dict):
        if not value:
            tags.add("object_empty")
        for item in value.values():
            tags.update(_classify(item))
            if isinstance(item, dict) and item:
                tags.add("object_nested")
    return tags


def _classify_fixture(data: Any) -> set[str]:
    """Separate top-level record sequences from arrays inside records."""
    if not isinstance(data, list):
        return _classify(data)
    tags: set[str] = set()
    key_sets = {frozenset(record) for record in data if isinstance(record, dict)}
    if len(key_sets) > 1:
        tags.add("records_sparse")
    for record in data:
        tags.update(_classify(record))
    return tags


def _fixture_categories(path: Path) -> set[str]:
    """Parse one fixture while retaining the numeric syntax category."""
    syntax_tags: set[str] = set()

    def parse_float(token: str) -> float:
        if "e" in token.lower():
            syntax_tags.add("float_exponent")
        value = float(token)
        if not math.isfinite(value):
            raise ValueError(f"Non-finite JSON number in {path.name}: {token}")
        return value

    def reject_constant(token: str) -> Any:
        raise ValueError(f"Non-JSON constant in {path.name}: {token}")

    with path.open(encoding="utf-8") as handle:
        data = json.load(
            handle, parse_float=parse_float, parse_constant=reject_constant
        )
    return _classify_fixture(data) | syntax_tags


def audit() -> tuple[dict[str, set[str]], set[str]]:
    """Return each fixture's categories and categories absent from the corpus."""
    fixtures = {
        path.name: _fixture_categories(path)
        for path in sorted(CORPUS_DIR.glob("*.json"))
    }
    present = set().union(*fixtures.values())
    return fixtures, set(REQUIRED_TYPES) - present


def main() -> int:
    """Print inventory gaps without presenting type presence as correctness."""
    fixtures, missing = audit()
    for name, tags in fixtures.items():
        print(f"{name}: {', '.join(sorted(tags))}")
    covered = len(REQUIRED_TYPES) - len(missing)
    print(f"\nCorpus categories present: {covered}/{len(REQUIRED_TYPES)}")
    for tag in sorted(missing):
        print(f"MISSING {tag}: {REQUIRED_TYPES[tag]}")
    return int(bool(missing))


if __name__ == "__main__":
    raise SystemExit(main())
