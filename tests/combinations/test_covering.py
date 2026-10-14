"""Complete modeled interactions with exact in-memory and decoded-table oracles."""

from __future__ import annotations

import json
import warnings
from itertools import combinations, product

import pytest

import transmog as tm
from tests.combinations.oracles import expected_tables, wire_tables
from tests.covering import PARAMS, SHAPE_CORPUS, covering_array
from tests.helpers import decode_directory, load_corpus, payload

CELLS = [(strength, row) for strength in (2, 3) for row in covering_array(t=strength)]


def cell_id(strength, row):
    """Name every parameter on failure."""
    return f"t={strength}|" + "|".join(f"{key}={value}" for key, value in row.items())


@pytest.mark.claim("C-COMB-COMPLETENESS")
@pytest.mark.parametrize("strength", (2, 3))
def test_covering_completeness_independently(strength):
    """P: every Cartesian t-tuple appears; validation does not reuse generator helpers."""
    rows = covering_array(t=strength)
    for keys in combinations(PARAMS, strength):
        expected = set(product(*(PARAMS[key] for key in keys)))
        actual = {tuple(row[key] for key in keys) for row in rows}
        assert actual == expected, keys


@pytest.mark.claim("C-COMB-FIDELITY")
@pytest.mark.parametrize(
    "strength,row", CELLS, ids=[cell_id(t, row) for t, row in CELLS]
)
def test_covering_cell(tmp_path, strength, row):
    """P: all modeled tables and values survive automatic encoding and schema evolution."""
    data = load_corpus(SHAPE_CORPUS[row["shape"]])
    if row["id_generation"] == "natural":
        records = data if isinstance(data, list) else [data]
        data = [
            {**record, "_id": f"natural-{index}"}
            for index, record in enumerate(records)
        ]
    expected = expected_tables(
        data, row["array_mode"], row["include_nulls"], row["stringify_values"]
    )
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Small batch_size", category=UserWarning
        )
        config = tm.TransmogConfig(
            array_mode=tm.ArrayMode(row["array_mode"]),
            include_nulls=row["include_nulls"],
            stringify_values=row["stringify_values"],
            id_generation=["id"]
            if row["id_generation"] == "composite"
            else row["id_generation"],
            batch_size=row["batch_size"],
            time_field=None,
        )
    result = tm.flatten(data, name="entity", config=config)
    actual = {"entity": [payload(record) for record in result.main]}
    actual.update(
        {
            name: [payload(record) for record in records]
            for name, records in result.tables.items()
        }
    )
    assert actual == expected

    save_dir = tmp_path / "save"
    save_destination = (
        save_dir if result.tables else save_dir / f"entity.{row['format']}"
    )
    stream_dir = tmp_path / "stream"

    def write_stream():
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", message="Schema deviations", category=UserWarning
            )
            return tm.flatten_stream(
                data,
                stream_dir,
                name="entity",
                config=config,
                output_format=row["format"],
                consolidate=row["consolidate"],
                coerce_schema=row["coerce_schema"],
            )

    result.save(save_destination, output_format=row["format"])
    paths = write_stream()
    saved = decode_directory(save_dir, row["format"], logical=True)
    streamed = decode_directory(stream_dir, row["format"], logical=True)
    encoded = wire_tables(expected, row["format"])
    assert set(saved) == set(streamed) == set(encoded)
    for name, rows in encoded.items():
        fields = set().union(*(record.keys() for record in rows))
        assert [payload(record) for record in saved[name]] == rows
        # Uncoerced parts have local schemas; only missing columns are filled.
        decoded = [
            {
                field: payload(record).get(
                    field, "" if row["format"] == "csv" else None
                )
                for field in fields
            }
            for record in streamed[name]
        ]
        assert decoded == rows
        assert all(set(payload(record)) <= fields for record in streamed[name])
        if row["id_generation"] in ("hash", "composite"):
            assert [
                (record["_id"], record.get("_parent_id")) for record in saved[name]
            ] == [
                (record["_id"], record.get("_parent_id")) for record in streamed[name]
            ]
    assert set(paths) == set(stream_dir.glob(f"*.{row['format']}"))
    if row["consolidate"]:
        assert sorted(path.stem for path in paths) == sorted(encoded)
    else:
        log = json.loads((stream_dir / "_schema_log.json").read_text(encoding="utf-8"))
        for name in encoded:
            table = "main" if name == "entity" else name
            parts = log["tables"][table]["parts"]
            assert {part["file"] for part in parts} == {
                path.name for path in paths if path.stem.rsplit("_part_", 1)[0] == name
            }
            if row["coerce_schema"]:
                field_sets = [set(record) for record in streamed[name]]
                assert all(fields == field_sets[0] for fields in field_sets)
                if any(part["deviations"] is not None for part in parts):
                    assert any("coerced_to" in part for part in parts)
