"""Covering array generator for the modeled config × shape space.

Generated at collection time. Not persisted. Cheap to regenerate.
"""

from __future__ import annotations

from typing import Any

PARAMS: dict[str, list[Any]] = {
    "array_mode": ["smart", "separate", "inline", "skip"],
    "include_nulls": [False, True],
    "stringify_values": [False, True],
    "id_generation": ["random", "natural", "hash", "composite"],
    "format": ["csv", "parquet", "orc", "avro"],
    "consolidate": [False, True],
    "coerce_schema": [False, True],
    "shape": [
        "scalar_nest",
        "simple_array",
        "object_array",
        "mixed_array",
        "sparse_records",
    ],
}

SHAPE_CORPUS = {
    "scalar_nest": "scalar_nest.json",
    "simple_array": "primitive_array.json",
    "object_array": "object_array.json",
    "mixed_array": "mixed_array.json",
    "sparse_records": "sparse_records.json",
}


def _normalize_tuple(
    combo: tuple[tuple[str, Any], ...], keys: list[str]
) -> tuple[tuple[str, Any], ...]:
    """Sort a parameter-value combination by key order for canonical form."""
    return tuple(sorted(combo, key=lambda kv: keys.index(kv[0])))


def _all_t_tuples(
    params: dict[str, list[Any]], t: int
) -> set[tuple[tuple[str, Any], ...]]:
    """Generate all t-way parameter-value combinations."""
    from itertools import combinations, product

    keys = list(params)
    tuples: set[tuple[tuple[str, Any], ...]] = set()
    for key_combo in combinations(keys, t):
        value_lists = [params[k] for k in key_combo]
        for value_combo in product(*value_lists):
            tup = tuple(zip(key_combo, value_combo, strict=True))
            tuples.add(_normalize_tuple(tup, keys))
    return tuples


def _row_tuples(
    row: dict[str, Any], keys: list[str], t: int
) -> set[tuple[tuple[str, Any], ...]]:
    """Extract all t-way tuples covered by a single row."""
    from itertools import combinations

    tuples: set[tuple[tuple[str, Any], ...]] = set()
    items = [(k, row[k]) for k in keys if k in row]
    for combo in combinations(items, t):
        tuples.add(_normalize_tuple(combo, keys))
    return tuples


def greedy_covering(
    params: dict[str, list[Any]] | None = None, t: int = 2
) -> list[dict[str, Any]]:
    """Build a deterministic t-way covering array by greedy extension."""
    params = params or PARAMS
    keys = list(params)
    uncovered = _all_t_tuples(params, t)
    rows: list[dict[str, Any]] = []

    while uncovered:
        target = sorted(uncovered, key=lambda c: tuple((k, str(v)) for k, v in c))[0]
        row: dict[str, Any] = {k: v for k, v in target}
        for key in keys:
            if key in row:
                continue
            best_value = params[key][0]
            best_count = -1
            for value in params[key]:
                candidate = dict(row)
                candidate[key] = value
                count = len(_row_tuples(candidate, keys, t) & uncovered)
                if count > best_count:
                    best_count = count
                    best_value = value
            row[key] = best_value
        rows.append({key: row[key] for key in keys})
        uncovered -= _row_tuples(row, keys, t)
    return rows


def covering_array(t: int = 2) -> list[dict[str, Any]]:
    """Return a t-way covering array over PARAMS. Regenerated each call."""
    return greedy_covering(t=t)


def tuple_coverage(
    rows: list[dict[str, Any]] | None = None, t: int = 2
) -> tuple[set[tuple[tuple[str, Any], ...]], set[tuple[tuple[str, Any], ...]]]:
    """Return (covered, uncovered) t-way tuple sets for the given rows."""
    rows = rows if rows is not None else covering_array(t=t)
    keys = list(PARAMS)
    all_tuples = _all_t_tuples(PARAMS, t)
    covered: set[tuple[tuple[str, Any], ...]] = set()
    for row in rows:
        covered |= _row_tuples(row, keys, t)
    return covered, all_tuples - covered


# Backward-compatible aliases
greedy_t2 = greedy_covering
pair_coverage = tuple_coverage


if __name__ == "__main__":
    rows = covering_array()
    covered, missing = pair_coverage(rows)
    print(f"rows={len(rows)} pairs_covered={len(covered)} missing={len(missing)}")
