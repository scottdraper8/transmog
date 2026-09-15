#!/usr/bin/env python3
"""Audit the characterization corpus for type diversity and completeness.

Checks that the corpus collectively exercises every JSON value type that
the codebase may encounter. Reports gaps so they are addressed by adding
fixtures, not by ignoring the missing type.

This script does not gate merges. It produces a report for human review.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

CORPUS_DIR = Path(__file__).resolve().parents[1] / "tests" / "corpus"

# Minimum type coverage required across the full corpus.
REQUIRED_TYPES = {
    "null_scalar": "null as a scalar object value",
    "null_in_array": "null as an array element",
    "bool_true": "boolean true",
    "bool_false": "boolean false",
    "int_positive": "positive integer",
    "int_zero": "integer zero",
    "float_fractional": "fractional float (critical for ijson Decimal)",
    "string_ascii": "ASCII string",
    "string_unicode": "non-ASCII / Unicode string",
    "string_empty": "empty string",
    "array_empty": "empty array",
    "array_primitive": "array of only primitives",
    "array_object": "array of only objects",
    "array_mixed": "array with mixed types (dict + primitive + null)",
    "object_empty": "empty object",
    "object_nested": "nested object (depth >= 2)",
    "records_sparse": "records with different key sets",
}


def _classify(value: Any, in_array: bool = False) -> set[str]:
    """Return the type tags present in a value, recursively."""
    tags: set[str] = set()

    if value is None:
        if in_array:
            tags.add("null_in_array")
        else:
            tags.add("null_scalar")
        return tags

    if isinstance(value, bool):
        tags.add("bool_true" if value else "bool_false")
        return tags

    if isinstance(value, int):
        if value > 0:
            tags.add("int_positive")
        elif value == 0:
            tags.add("int_zero")
        return tags

    if isinstance(value, float):
        tags.add("float_fractional")
        return tags

    if isinstance(value, str):
        if not value:
            tags.add("string_empty")
        elif all(ord(c) < 128 for c in value):
            tags.add("string_ascii")
        else:
            tags.add("string_unicode")
        return tags

    if isinstance(value, list):
        if not value:
            tags.add("array_empty")
        else:
            has_dict = any(isinstance(item, dict) for item in value)
            has_primitive = any(
                not isinstance(item, (dict, list)) and item is not None
                for item in value
            )
            has_null = any(item is None for item in value)

            if has_dict and (has_primitive or has_null):
                tags.add("array_mixed")
            elif has_dict:
                tags.add("array_object")
            else:
                tags.add("array_primitive")

            for item in value:
                tags |= _classify(item, in_array=True)
        return tags

    if isinstance(value, dict):
        if not value:
            tags.add("object_empty")
        for v in value.values():
            tags |= _classify(v)
            if isinstance(v, dict) and v:
                tags.add("object_nested")
        return tags

    return tags


def _classify_fixture(data: Any) -> set[str]:
    """Classify all types in a loaded JSON fixture."""
    tags: set[str] = set()
    if isinstance(data, list):
        # Multi-record fixture — check for sparse keys
        if len(data) >= 2:
            key_sets = [set(r.keys()) for r in data if isinstance(r, dict)]
            if len({frozenset(ks) for ks in key_sets}) > 1:
                tags.add("records_sparse")
        for item in data:
            tags |= _classify(item)
    else:
        tags |= _classify(data)
    return tags


def audit() -> tuple[dict[str, set[str]], set[str]]:
    """Audit the corpus and return (fixture_tags, missing_types)."""
    fixture_tags: dict[str, set[str]] = {}
    all_tags: set[str] = set()

    for name in sorted(os.listdir(CORPUS_DIR)):
        if not name.endswith(".json"):
            continue
        with open(CORPUS_DIR / name, encoding="utf-8") as f:
            data = json.load(f)
        tags = _classify_fixture(data)
        fixture_tags[name] = tags
        all_tags |= tags

    missing = set(REQUIRED_TYPES.keys()) - all_tags
    return fixture_tags, missing


def main() -> int:
    """Audit the corpus and print a coverage report."""
    fixture_tags, missing = audit()

    print("=== Corpus type audit ===\n")
    for name, tags in fixture_tags.items():
        tag_list = ", ".join(sorted(tags)) if tags else "(none)"
        print(f"  {name}: {tag_list}")

    print(
        f"\n=== Coverage: {len(REQUIRED_TYPES) - len(missing)}/{len(REQUIRED_TYPES)} required types ===\n"
    )

    if missing:
        print("MISSING types (add fixtures or justify omission):\n")
        for tag in sorted(missing):
            print(f"  ✗ {tag}: {REQUIRED_TYPES[tag]}")
        print()
        return 1

    print("All required types covered.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
