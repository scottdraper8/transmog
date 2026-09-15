"""Metamorphic sensors: relations that must hold without knowing the exact table."""

from __future__ import annotations

import json
import warnings

import pytest

import transmog as tm
from tests.helpers import (
    decode_file,
    hash_config,
    load_corpus,
    payload,
    payloads,
)
from transmog.types import ArrayMode


@pytest.mark.claim("C-REL-003")
@pytest.mark.parametrize("corpus", ["sparse_records.json", "nested_object_arrays.json"])
def test_flatten_batch_size_does_not_change_payload(corpus):
    """P: batch_size does not change non-metadata fields of flatten."""
    data = load_corpus(corpus)
    payloads_by_bs = []
    tables_by_bs = []
    for batch_size in (1, 2, 5000):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            config = hash_config(batch_size=batch_size, array_mode=ArrayMode.SMART)
            result = tm.flatten(data, name="entity", config=config)
        payloads_by_bs.append(payloads(result.main))
        tables_by_bs.append(
            {
                name: [payload(row) for row in rows]
                for name, rows in result.tables.items()
            }
        )
    assert payloads_by_bs[0] == payloads_by_bs[1] == payloads_by_bs[2]
    assert tables_by_bs[0] == tables_by_bs[1] == tables_by_bs[2]


@pytest.mark.claim("C-REL-003")
def test_flatten_stream_batch_size_does_not_change_csv_payload(tmp_path):
    """P: batch_size does not change decoded flatten_stream CSV payload."""
    data = load_corpus("sparse_records.json")
    decoded = []
    for batch_size in (1, 2, 5000):
        dest = tmp_path / f"b{batch_size}"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            config = hash_config(batch_size=batch_size)
            tm.flatten_stream(
                data,
                dest,
                name="people",
                output_format="csv",
                config=config,
                consolidate=True,
            )
        rows = decode_file(dest / "people.csv", "csv")
        decoded.append([payload(row) for row in rows])
    assert decoded[0] == decoded[1] == decoded[2]
    # CSV consolidation fills missing sparse fields with "".
    assert decoded[0] == [
        {
            "id": "1",
            "name": "Alice",
            "email": "alice@example.com",
            "phone": "",
        },
        {"id": "2", "name": "Bob", "email": "", "phone": "555-1234"},
        {
            "id": "3",
            "name": "",
            "email": "charlie@example.com",
            "phone": "555-5678",
        },
    ]


@pytest.mark.claim("C-ALG-005")
def test_inline_is_json_dumps_identity():
    """P: INLINE output equals json.dumps of the original array."""
    data = load_corpus("inline_types.json")
    result = tm.flatten(
        data, name="entity", config=hash_config(array_mode=ArrayMode.INLINE)
    )
    assert result.main[0]["arr"] == json.dumps(data["arr"], ensure_ascii=False)


@pytest.mark.claim("C-ALG-001")
def test_stringify_leaves_strings_and_null_like():
    """P: stringify_values leaves strings unchanged and null-like as omitted/None."""
    data = {"name": "x", "already": "keep", "num": 3, "nada": None, "empty": ""}
    as_is = tm.flatten(data, name="e", config=hash_config(stringify_values=False))
    strung = tm.flatten(data, name="e", config=hash_config(stringify_values=True))
    assert as_is.main[0]["already"] == "keep"
    assert strung.main[0]["already"] == "keep"
    assert strung.main[0]["num"] == "3"
    assert "nada" not in strung.main[0]
    assert "empty" not in strung.main[0]

    included = tm.flatten(
        data,
        name="e",
        config=hash_config(stringify_values=True, include_nulls=True),
    )
    assert included.main[0]["nada"] is None
    assert included.main[0]["empty"] is None
    assert included.main[0]["already"] == "keep"
