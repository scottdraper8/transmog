"""Pairwise covering of modeled parameters.

Each cell is named on failure. Phase 1 oracles that apply to the cell's
shape × array_mode are executed. Other cells still run flatten (and a
writer) so that every pair is observed.

This does not prove higher-order interactions are irrelevant.
"""

from __future__ import annotations

import json
import warnings
from typing import Any

import pytest

import transmog as tm
from tests.covering import SHAPE_CORPUS, covering_array
from tests.helpers import (
    array_mode,
    decode_file,
    load_corpus,
    payload,
)
from transmog.exceptions import ValidationError
from transmog.types import ArrayMode

ROWS = covering_array()


def _cell_id(row: dict[str, Any]) -> str:
    return (
        f"mode={row['array_mode']}|nulls={row['include_nulls']}|"
        f"str={row['stringify_values']}|id={row['id_generation']}|"
        f"fmt={row['format']}|cons={row['consolidate']}|"
        f"coerce={row['coerce_schema']}|shape={row['shape']}"
    )


def _id_generation(value: str) -> str | list[str]:
    if value == "composite":
        return ["id"]
    return value


def _inject_natural_ids(data: Any) -> Any:
    """Give natural strategy a parent id_field so the cell is executable."""
    if isinstance(data, dict):
        injected = dict(data)
        injected.setdefault("_id", "natural-parent")
        return injected
    if isinstance(data, list):
        out = []
        for index, record in enumerate(data):
            item = dict(record)
            item.setdefault("_id", f"natural-{index}")
            out.append(item)
        return out
    return data


def _config(row: dict[str, Any]) -> tm.TransmogConfig:
    return tm.TransmogConfig(
        array_mode=array_mode(row["array_mode"]),
        include_nulls=row["include_nulls"],
        stringify_values=row["stringify_values"],
        id_generation=_id_generation(row["id_generation"]),
        time_field=None,
        batch_size=5000,
    )


def _applicable_algebra(row: dict[str, Any], result: tm.FlattenResult) -> None:
    """Run Phase 1 oracles that apply to this cell's shape and array_mode.

    Where a cell matches corpus fixtures exactly, the oracle pins content
    rather than only checking structural predicates.
    """
    mode = row["array_mode"]
    shape = row["shape"]
    is_stringified = row["stringify_values"]
    main = result.main[0] if result.main else {}
    main_payload = payload(main)
    cell = _cell_id(row)

    if shape == "simple_array":
        if mode == "smart":
            assert "tags" in main_payload, cell
            assert isinstance(main_payload["tags"], list), cell
            # Exact content: corpus is ["tech", "startup", "ai"]
            if is_stringified:
                assert main_payload["tags"] == ["tech", "startup", "ai"], cell
            else:
                assert main_payload["tags"] == ["tech", "startup", "ai"], cell
            assert not any(name.endswith("_tags") for name in result.tables), cell
        elif mode == "separate":
            assert "tags" not in main_payload, cell
            tag_tables = [n for n in result.tables if n.endswith("_tags")]
            assert len(tag_tables) == 1, cell
            child_values = sorted(
                payload(r).get("value") for r in result.tables[tag_tables[0]]
            )
            assert child_values == ["ai", "startup", "tech"], cell
        elif mode == "inline":
            assert isinstance(main_payload.get("tags"), str), cell
            parsed = json.loads(main_payload["tags"])
            assert parsed == ["tech", "startup", "ai"], cell
            assert result.tables == {}, cell
        elif mode == "skip":
            assert "tags" not in main_payload, cell
            assert main_payload == {"name": "Acme"}, cell
            assert result.tables == {}, cell

    elif shape == "object_array":
        if mode == "smart":
            assert "employees" not in main_payload, cell
            emp_tables = [n for n in result.tables if n.endswith("_employees")]
            assert len(emp_tables) == 1, cell
            children = sorted(
                (payload(r) for r in result.tables[emp_tables[0]]),
                key=lambda r: str(r.get("name", "")),
            )
            assert len(children) == 2, cell
            if is_stringified:
                assert children[0]["name"] == "Alice", cell
                assert children[0]["id"] == "101", cell
            else:
                assert children[0]["name"] == "Alice", cell
                assert children[0]["id"] == 101, cell
        elif mode == "separate":
            assert "employees" not in main_payload, cell
            emp_tables = [n for n in result.tables if n.endswith("_employees")]
            assert len(emp_tables) == 1, cell
            assert len(result.tables[emp_tables[0]]) == 2, cell
        elif mode == "inline":
            assert isinstance(main_payload.get("employees"), str), cell
            parsed = json.loads(main_payload["employees"])
            assert len(parsed) == 2, cell
            assert result.tables == {}, cell
        elif mode == "skip":
            assert "employees" not in main_payload, cell
            assert main_payload == {"name": "Acme"}, cell
            assert result.tables == {}, cell

    elif shape == "mixed_array":
        if mode in ("smart", "separate"):
            assert "items" not in main_payload, cell
            item_tables = [n for n in result.tables if n.endswith("_items")]
            assert len(item_tables) == 1, cell
            items = result.tables[item_tables[0]]
            # dict + str + int + list; None only if include_nulls
            expected = 5 if row["include_nulls"] else 4
            assert len(items) == expected, cell
            # Pin that the dict item has key "k"
            dict_items = [payload(r) for r in items if "k" in payload(r)]
            assert len(dict_items) == 1, cell
            assert dict_items[0]["k"] == "obj", cell
        elif mode == "inline":
            assert isinstance(main_payload.get("items"), str), cell
            parsed = json.loads(main_payload["items"])
            assert len(parsed) == 5, cell
            assert result.tables == {}, cell
        elif mode == "skip":
            assert "items" not in main_payload, cell
            assert main_payload == {"name": "mixed"}, cell
            assert result.tables == {}, cell

    elif shape == "scalar_nest":
        # Exact: corpus has id, meta.created, meta.flags.active
        if is_stringified:
            assert main_payload.get("id") == "1", cell
            assert main_payload.get("meta_created") == "2023-01-01", cell
            assert main_payload.get("meta_flags_active") == "True", cell
        else:
            assert main_payload.get("id") == 1, cell
            assert main_payload.get("meta_created") == "2023-01-01", cell
            assert main_payload.get("meta_flags_active") is True, cell
        assert result.tables == {}, cell

    elif shape == "sparse_records":
        assert len(result.main) == 3, cell
        # Pin names if not stringified and nulls excluded
        names = [payload(r).get("name") for r in result.main]
        non_none = [n for n in names if n is not None]
        if not row["include_nulls"]:
            assert "Alice" in non_none, cell
            assert "Bob" in non_none, cell


@pytest.mark.claim("C-COMB-001")
def test_covering_array_is_complete():
    """P: every modeled pair appears in at least one generated row."""
    from tests.covering import pair_coverage

    covered, missing = pair_coverage(ROWS)
    assert not missing
    # Row count depends on greedy covering; completeness is the invariant, not exact count.
    assert len(ROWS) >= 20


@pytest.mark.claim("C-COMB-001")
@pytest.mark.parametrize("row", ROWS, ids=[_cell_id(r) for r in ROWS])
def test_pairwise_cell(tmp_path, row):
    """P: the named cell executes; applicable algebra oracles hold; writer runs."""
    corpus = SHAPE_CORPUS[row["shape"]]
    data = load_corpus(corpus)
    if row["id_generation"] == "natural":
        data = _inject_natural_ids(data)
    config = _config(row)
    cell = _cell_id(row)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        try:
            result = tm.flatten(data, name="entity", config=config)
        except ValidationError as exc:
            raise AssertionError(f"{cell} unexpected ValidationError: {exc}") from exc

    assert result.main, cell
    _applicable_algebra(row, result)

    dest = tmp_path / "out"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        paths = tm.flatten_stream(
            data,
            dest,
            name="entity",
            output_format=row["format"],
            config=config,
            consolidate=row["consolidate"],
            coerce_schema=row["coerce_schema"],
        )
    assert paths, cell
    suffix = f".{row['format']}"
    written = [p for p in dest.iterdir() if p.suffix == suffix]
    assert written, cell
    if not row["consolidate"]:
        assert (dest / "_schema_log.json").exists(), cell
        assert any("_part_" in p.name for p in written), cell
    else:
        assert not any("_part_" in p.name for p in written), cell

    # Decode at least the main-looking file so the codec is actually read.
    main_candidates = [
        p for p in written if p.stem == "entity" or p.stem.startswith("entity_part_")
    ]
    assert main_candidates, cell
    decoded = decode_file(main_candidates[0], row["format"])
    assert decoded, cell
