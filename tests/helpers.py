"""Helpers for characterization probes and sensors.

These functions load corpus fixtures, strip metadata, and decode writer
output. They do not assert correctness.
"""

from __future__ import annotations

import csv
import json
from contextvars import ContextVar
from pathlib import Path
from typing import Any

import transmog as tm
from transmog.config import TransmogConfig
from transmog.types import ArrayMode

CORPUS_DIR = Path(__file__).resolve().parent / "corpus"

METADATA_FIELDS = ("_id", "_parent_id", "_timestamp")
ACTIVE_SENSOR: ContextVar[str | None] = ContextVar("active_sensor", default=None)
CORPUS_SENSORS: dict[str, set[str]] = {}


def corpus_path(name: str) -> Path:
    """Return the path to a checked-in corpus fixture."""
    path = CORPUS_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"corpus fixture not found: {path}")
    return path


def load_corpus(name: str) -> Any:
    """Load a JSON corpus fixture."""
    with corpus_path(name).open(encoding="utf-8") as handle:
        data = json.load(handle)
    sensor = ACTIVE_SENSOR.get()
    if sensor is not None:
        CORPUS_SENSORS.setdefault(name, set()).add(sensor)
    return data


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
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


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


def _arrow_rows(table: Any, logical: bool) -> list[dict[str, Any]]:
    """Decode only explicitly marked JSON fields, never guess from string content."""
    rows = table.to_pylist()
    if logical:
        for field in table.schema:
            if (field.metadata or {}).get(b"transmog.encoding") == b"json":
                for row in rows:
                    if row[field.name] is not None:
                        row[field.name] = json.loads(row[field.name])
    return rows


def read_arrow_table(path: Path, output_format: str) -> Any:
    """Read a physical table file without inferring directory partitions."""
    if output_format == "parquet":
        import pyarrow.parquet as pq

        return pq.ParquetFile(path).read()
    if output_format == "orc":
        import pyarrow.orc as orc

        return orc.read_table(path)
    raise ValueError(f"Not an Arrow format: {output_format}")


def decode_avro(path: Path) -> list[dict[str, Any]]:
    """Decode an Avro file to a list of records."""
    import fastavro

    with path.open("rb") as handle:
        return list(fastavro.reader(handle))


DECODERS = {
    "csv": decode_csv,
    "avro": decode_avro,
}


def decode_file(
    path: Path, output_format: str, *, logical: bool = False
) -> list[dict[str, Any]]:
    """Decode a table; JSON wire decoding is opt-in and requires field metadata."""
    if output_format in ("parquet", "orc"):
        return _arrow_rows(read_arrow_table(path, output_format), logical)
    return DECODERS[output_format](path)


def decode_directory(
    directory: Path, output_format: str, *, logical: bool = False
) -> dict[str, list[dict[str, Any]]]:
    """Decode every table and numbered part in file order."""
    tables: dict[str, list[dict[str, Any]]] = {}
    for path in sorted(directory.glob(f"*.{output_format}")):
        name = path.stem.rsplit("_part_", 1)[0]
        tables.setdefault(name, []).extend(
            decode_file(path, output_format, logical=logical)
        )
    return tables


def save_paths_as_list(saved: list[str] | dict[str, str]) -> list[Path]:
    """Normalize FlattenResult.save return value to paths."""
    if isinstance(saved, dict):
        return [Path(path) for path in saved.values()]
    return [Path(path) for path in saved]
