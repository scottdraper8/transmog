"""Differential sensors for comparable paths.

These tests pin actuality. Format-specific differences (CSV stringifies
all values, Parquet/ORC preserve native types, Avro serializes lists as
JSON strings) are asserted directly — the assertions are the observations.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import transmog as tm
from tests.helpers import (
    decode_directory,
    decode_file,
    hash_config,
    load_corpus,
    payload,
    payloads,
    save_paths_as_list,
)
from transmog.types import ArrayMode

FORMATS = ("csv", "parquet", "orc", "avro")


def _strip(tables: dict[str, list[dict]]) -> dict[str, list[dict]]:
    return {name: [payload(row) for row in rows] for name, rows in tables.items()}


def _ids(tables: dict[str, list[dict]]) -> dict[str, list[str | None]]:
    return {name: [row.get("_id") for row in rows] for name, rows in tables.items()}


@pytest.mark.claim("C-REL-001")
@pytest.mark.parametrize("output_format", FORMATS)
def test_flatten_save_vs_flatten_stream_decoded(tmp_path, output_format):
    """P: flatten+save and flatten_stream decode to the same tables.

    List columns are now consistent across both entry points:
    - CSV/Avro: lists serialized as JSON strings via json.dumps
    - Parquet/ORC: lists preserved as native list-typed columns
    """
    data = load_corpus("nested_object_arrays.json")
    config = hash_config(array_mode=ArrayMode.SMART)
    save_dir = tmp_path / "save"
    stream_dir = tmp_path / "stream"

    result = tm.flatten(data, name="company", config=config)
    saved = result.save(save_dir, output_format=output_format)
    save_paths = save_paths_as_list(saved)
    assert save_paths
    stream_paths = tm.flatten_stream(
        data,
        stream_dir,
        name="company",
        output_format=output_format,
        config=config,
        consolidate=True,
    )
    assert stream_paths

    save_tables = decode_directory(save_dir, output_format)
    stream_tables = decode_directory(stream_dir, output_format)
    assert set(save_tables) == {"company", "company_employees"}
    assert set(stream_tables) == {"company", "company_employees"}
    assert _ids(save_tables) == _ids(stream_tables)

    save_payload = _strip(save_tables)
    stream_payload = _strip(stream_tables)
    assert save_payload == stream_payload

    if output_format in ("csv", "avro"):
        # CSV and Avro serialize lists as JSON strings
        assert save_payload["company"][0]["tags"] == '["tech", "startup", "ai"]'
    else:
        # Parquet and ORC preserve native list columns
        assert save_payload["company"][0]["tags"] == ["tech", "startup", "ai"]


@pytest.mark.claim("C-REL-002")
def test_writer_decoded_forms_and_exceptions(tmp_path):
    """P: four writers produce per-format decoded forms with known differences.

    Scalars agree across formats (modulo CSV stringification). List columns:
    CSV/Avro serialize as JSON strings; Parquet/ORC preserve native lists.
    """
    data = load_corpus("nested_object_arrays.json")
    config = hash_config(array_mode=ArrayMode.SMART)
    result = tm.flatten(data, name="company", config=config)

    decoded: dict[str, dict[str, list[dict]]] = {}
    for output_format in FORMATS:
        dest = tmp_path / output_format
        result.save(dest, output_format=output_format)
        decoded[output_format] = _strip(decode_directory(dest, output_format))

    csv_company = decoded["csv"]["company"][0]
    parquet_company = decoded["parquet"]["company"][0]
    orc_company = decoded["orc"]["company"][0]
    avro_company = decoded["avro"]["company"][0]

    assert csv_company == {
        "id": "1",
        "name": "Company",
        "tags": '["tech", "startup", "ai"]',
    }
    assert parquet_company == {
        "id": 1,
        "name": "Company",
        "tags": ["tech", "startup", "ai"],
    }
    assert orc_company == parquet_company
    assert avro_company == {
        "id": 1,
        "name": "Company",
        "tags": '["tech", "startup", "ai"]',
    }

    # Scalar name agrees across formats; id type differs (CSV stringifies).
    # List encoding: CSV/Avro use JSON strings; Parquet/ORC use native lists.
    names = {fmt: decoded[fmt]["company"][0]["name"] for fmt in FORMATS}
    assert set(names.values()) == {"Company"}


@pytest.mark.claim("C-REL-002")
def test_csv_null_is_empty_string_typed_null_is_none(tmp_path):
    """P: CSV writes null-like as ''; parquet/orc/avro decode None."""
    data = [
        {"id": 1, "name": "Alice", "value": 100},
        {"id": 2, "name": "Bob", "value": None},
    ]
    config = hash_config(include_nulls=True)
    result = tm.flatten(data, name="people", config=config)

    csv_path = tmp_path / "people.csv"
    pq_path = tmp_path / "people.parquet"
    result.save(csv_path, output_format="csv")
    result.save(pq_path, output_format="parquet")
    csv_rows = decode_file(csv_path, "csv")
    pq_rows = decode_file(pq_path, "parquet")
    assert csv_rows[1]["value"] == ""
    assert csv_rows[1]["name"] == "Bob"
    assert pq_rows[1]["value"] is None
    assert pq_rows[1]["name"] == "Bob"
    assert pq_rows[0]["value"] == 100
    assert csv_rows[0]["value"] == "100"


@pytest.mark.claim("C-REL-004")
def test_hash_ids_stable_across_runs_and_entry_points(tmp_path):
    """P: hash IDs match across reruns and flatten vs flatten_stream (csv)."""
    data = load_corpus("object_array.json")
    config = hash_config()
    first = tm.flatten(data, name="company", config=config)
    second = tm.flatten(data, name="company", config=config)
    assert first.main[0]["_id"] == second.main[0]["_id"]
    assert first.main[0]["_id"] == "3fa890d6-7c73-5c40-acc6-38e77dbecd65"

    stream_dir = tmp_path / "stream"
    tm.flatten_stream(
        data, stream_dir, name="company", output_format="csv", config=config
    )
    stream_main = decode_file(stream_dir / "company.csv", "csv")
    assert stream_main[0]["_id"] == first.main[0]["_id"]


@pytest.mark.claim("C-REL-001")
def test_ijson_file_iterator_matches_in_memory_flatten(tmp_path):
    """P: flatten(json file) payload equals flatten(loaded list), including hash IDs.

    flatten_stream uses ijson for .json arrays; flatten uses full load.
    In-memory flatten of the same path uses full load. Equality of flatten(path)
    vs flatten(list) is the full-load side. Stream csv payload matches flatten
    csv save (same codec path).
    """
    records = [{"id": i, "name": f"n{i}", "tags": ["a", "b"]} for i in range(5)]
    path = tmp_path / "arr.json"
    path.write_text(json.dumps(records), encoding="utf-8")
    config = hash_config()

    mem = tm.flatten(records, name="r", config=config)
    file_flat = tm.flatten(str(path), name="r", config=config)
    assert payloads(mem.main) == payloads(file_flat.main)
    assert [row["_id"] for row in mem.main] == [row["_id"] for row in file_flat.main]

    save_path = tmp_path / "r.csv"
    stream_dir = tmp_path / "stream"
    mem.save(save_path, output_format="csv")
    tm.flatten_stream(
        str(path), stream_dir, name="r", output_format="csv", config=config
    )
    save_rows = [payload(row) for row in decode_file(save_path, "csv")]
    stream_rows = [payload(row) for row in decode_file(stream_dir / "r.csv", "csv")]
    assert save_rows == stream_rows


@pytest.mark.claim("C-REL-001")
def test_medium_array_flatten_vs_stream_equality(tmp_path):
    """P: flatten(list) payload equals flatten_stream(file) payload at 500 records.

    Exercises ijson streaming vs full JSON load equality at scale with
    floats (which ijson returns as Decimal). Hash IDs must match.
    """
    data = load_corpus("medium_array.json")
    config = hash_config(array_mode=ArrayMode.SMART)

    mem = tm.flatten(data, name="r", config=config)
    assert len(mem.main) == 500
    assert mem.tables == {}

    # Save in-memory result to CSV (single file, no child tables)
    save_path = tmp_path / "r_save.csv"
    mem.save(save_path, output_format="csv")
    save_rows = [payload(row) for row in decode_file(save_path, "csv")]

    # Stream from file to CSV
    corpus = Path(__file__).resolve().parent.parent / "corpus" / "medium_array.json"
    stream_dir = tmp_path / "stream"
    tm.flatten_stream(
        str(corpus),
        stream_dir,
        name="r",
        output_format="csv",
        config=config,
    )
    stream_rows = [payload(row) for row in decode_file(stream_dir / "r.csv", "csv")]

    assert len(save_rows) == 500
    assert len(stream_rows) == 500
    assert save_rows == stream_rows

    # Hash IDs must also match between the two paths
    save_ids = [row.get("_id") for row in decode_file(save_path, "csv")]
    stream_ids = [row.get("_id") for row in decode_file(stream_dir / "r.csv", "csv")]
    assert save_ids == stream_ids


@pytest.mark.claim("C-REL-001")
def test_consolidate_false_writes_part_files_and_schema_log(tmp_path):
    """P: consolidate=False retains numbered part files and _schema_log.json."""
    data = load_corpus("nested_object_arrays.json")
    config = hash_config(batch_size=5000)
    out = tmp_path / "parts"
    paths = tm.flatten_stream(
        data,
        out,
        name="company",
        output_format="csv",
        config=config,
        consolidate=False,
    )
    names = sorted(p.name for p in out.iterdir())
    assert names == [
        "_schema_log.json",
        "company_employees_part_0000.csv",
        "company_part_0000.csv",
    ]
    returned = sorted(Path(p).name for p in paths)
    assert returned == ["company_employees_part_0000.csv", "company_part_0000.csv"]


@pytest.mark.claim("C-REL-006")
def test_csv_consolidation_fills_sparse_fields(tmp_path):
    """P: CSV consolidation fills missing fields with empty string when
    parts have different columns.

    Exercises the sparse-fill path in CsvStreamingWriter._consolidate_parts.
    """
    # Two batches with different schemas to force schema deviation
    batch1 = [{"id": 1, "name": "Alice"}]
    batch2 = [{"id": 2, "name": "Bob", "extra": "yes"}]
    config = hash_config(batch_size=1)
    out = tmp_path / "sparse_csv"
    paths = tm.flatten_stream(
        batch1 + batch2,
        out,
        name="items",
        output_format="csv",
        config=config,
        consolidate=True,
    )
    assert paths
    rows = decode_file(out / "items.csv", "csv")
    assert len(rows) == 2
    # First row should have "extra" filled as empty string
    assert rows[0].get("extra") == ""
    assert rows[1].get("extra") == "yes"


@pytest.mark.claim("C-REL-007")
def test_writer_field_order_matches_insertion_order(tmp_path):
    """P: Writer output columns follow insertion order, not alphabetical.

    Field order matters for CSV consumers and for reproducibility of
    decoded comparisons. This sensor detects _collect_field_names reversal.
    """
    data = [{"zeta": 1, "alpha": 2, "mid": 3}]
    config = hash_config()
    result = tm.flatten(data, name="order", config=config)

    csv_path = tmp_path / "order.csv"
    result.save(csv_path, output_format="csv")

    import csv as csv_mod

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv_mod.reader(f)
        header = next(reader)
    # Metadata fields come from annotate_with_metadata; payload fields
    # must appear in insertion order: zeta, alpha, mid
    payload_cols = [c for c in header if c not in ("_id", "_parent_id", "_timestamp")]
    assert payload_cols == ["zeta", "alpha", "mid"]


@pytest.mark.claim("C-REL-008")
def test_arrow_consolidation_promotes_schema(tmp_path):
    """P: PyArrow consolidation promotes part tables to a unified schema
    when parts have different columns.

    Exercises _promote_table_to_schema in the consolidation path.
    """
    batch1 = [{"id": 1, "name": "Alice"}]
    batch2 = [{"id": 2, "name": "Bob", "score": 95}]
    config = hash_config(batch_size=1)
    out = tmp_path / "promote"
    paths = tm.flatten_stream(
        batch1 + batch2,
        out,
        name="items",
        output_format="parquet",
        config=config,
        consolidate=True,
    )
    assert paths
    rows = decode_file(out / "items.parquet", "parquet")
    assert len(rows) == 2
    # First row gets null for the missing "score" column
    assert rows[0].get("score") is None
    assert rows[1].get("score") == 95
