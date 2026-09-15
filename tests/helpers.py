"""Helpers for characterization probes and sensors.

These functions load corpus fixtures, strip metadata, and decode writer
output. They do not assert correctness.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import transmog as tm
from transmog.config import TransmogConfig
from transmog.types import ArrayMode

CORPUS_DIR = Path(__file__).resolve().parent / "corpus"

METADATA_FIELDS = ("_id", "_parent_id", "_timestamp")


def corpus_path(name: str) -> Path:
    """Return the path to a checked-in corpus fixture."""
    path = CORPUS_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"corpus fixture not found: {path}")
    return path


def load_corpus(name: str) -> Any:
    """Load a JSON corpus fixture."""
    with corpus_path(name).open(encoding="utf-8") as handle:
        return json.load(handle)


def array_mode(value: str | ArrayMode) -> ArrayMode:
    """Coerce a string or enum to ArrayMode."""
    if isinstance(value, ArrayMode):
        return value
    return ArrayMode(value)


def hash_config(**overrides: Any) -> TransmogConfig:
    """Deterministic config: hash IDs, no timestamps."""
    params: dict[str, Any] = {
        "id_generation": "hash",
        "time_field": None,
    }
    params.update(overrides)
    if "array_mode" in params:
        params["array_mode"] = array_mode(params["array_mode"])
    return TransmogConfig(**params)


def payload(record: dict[str, Any], extra_drop: tuple[str, ...] = ()) -> dict[str, Any]:
    """Return a record without metadata fields."""
    drop = set(METADATA_FIELDS) | set(extra_drop)
    return {key: value for key, value in record.items() if key not in drop}


def payloads(
    records: list[dict[str, Any]], extra_drop: tuple[str, ...] = ()
) -> list[dict[str, Any]]:
    """Strip metadata from a list of records."""
    return [payload(record, extra_drop=extra_drop) for record in records]


def canonical_json(value: Any) -> str:
    """Stable JSON for sorting and comparison."""
    return json.dumps(value, sort_keys=True, default=str, ensure_ascii=False)


def sort_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort records by canonical JSON of their payload."""
    return sorted(records, key=canonical_json)


def tables_payload(
    result: tm.FlattenResult,
) -> dict[str, list[dict[str, Any]]]:
    """Main plus child tables with metadata stripped, children sorted."""
    out: dict[str, list[dict[str, Any]]] = {
        "main": payloads(result.main),
    }
    for name, rows in sorted(result.tables.items()):
        out[name] = sort_records(payloads(rows))
    return out


def parent_ids(result: tm.FlattenResult) -> dict[str, Any]:
    """Linkage facts that do not depend on ID strategy values."""
    if not result.main:
        return {"main_count": 0, "children": {}}
    main_id = result.main[0].get("_id")
    children: dict[str, Any] = {}
    for name, rows in sorted(result.tables.items()):
        children[name] = {
            "count": len(rows),
            "all_parent_ids_match_main": all(
                row.get("_parent_id") == main_id for row in rows
            ),
            "child_ids": [row.get("_id") for row in rows],
            "parent_ids": [row.get("_parent_id") for row in rows],
        }
    return {"main_id": main_id, "main_count": len(result.main), "children": children}


def flatten_corpus(
    name: str,
    *,
    entity: str = "entity",
    **config_overrides: Any,
) -> tm.FlattenResult:
    """Flatten a corpus fixture with a hash config plus overrides."""
    data = load_corpus(name)
    config = hash_config(**config_overrides)
    return tm.flatten(data, name=entity, config=config)


def decode_csv(path: Path) -> list[dict[str, str]]:
    """Decode a CSV file as row dicts (all values strings)."""
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def decode_parquet(path: Path) -> list[dict[str, Any]]:
    """Decode a Parquet file to a list of records."""
    import pyarrow.parquet as pq

    return pq.read_table(path).to_pylist()


def decode_orc(path: Path) -> list[dict[str, Any]]:
    """Decode an ORC file to a list of records."""
    import pyarrow.orc as orc

    return orc.read_table(path).to_pylist()


def decode_avro(path: Path) -> list[dict[str, Any]]:
    """Decode an Avro file to a list of records."""
    import fastavro

    with path.open("rb") as handle:
        return list(fastavro.reader(handle))


DECODERS = {
    "csv": decode_csv,
    "parquet": decode_parquet,
    "orc": decode_orc,
    "avro": decode_avro,
}


def decode_file(path: Path, output_format: str) -> list[dict[str, Any]]:
    """Decode a written table file by format name."""
    decoder = DECODERS[output_format]
    return decoder(path)


def files_by_stem(paths: list[Path] | list[str], suffix: str) -> dict[str, Path]:
    """Map file stem to path for a given suffix."""
    suffix = suffix if suffix.startswith(".") else f".{suffix}"
    out: dict[str, Path] = {}
    for raw in paths:
        path = Path(raw)
        if path.suffix == suffix:
            out[path.stem] = path
    return out


def save_paths_as_list(saved: list[str] | dict[str, str]) -> list[Path]:
    """Normalize FlattenResult.save return value to paths."""
    if isinstance(saved, dict):
        return [Path(path) for path in saved.values()]
    return [Path(path) for path in saved]


def logical_from_decoded(
    rows: list[dict[str, Any]],
    output_format: str,
) -> list[dict[str, Any]]:
    """Project decoded writer rows toward a format-independent logical record.

    This is a comparison aid, not a claim that formats are identical.
    Format-specific residues stay in the row; callers consult the exception
    list rather than collapsing them.
    """
    projected: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for key, value in row.items():
            if key in METADATA_FIELDS:
                continue
            item[key] = _logical_value(value, output_format)
        projected.append(item)
    return sort_records(projected)


def _logical_value(value: Any, output_format: str) -> Any:
    """Normalize a decoded cell for cross-format comparison notes."""
    if output_format == "csv":
        if value == "":
            return None
        return value
    return value
