"""Triple-wise (t=3) covering of modeled parameters.

Exercises every three-way interaction among config and shape parameters.
Uses the same algebra oracles as the pairwise suite. Catches failures
that only manifest when three specific settings combine.
"""

from __future__ import annotations

import warnings
from typing import Any

import pytest

import transmog as tm
from tests.combinations.test_pairwise import (
    _applicable_algebra,
    _config,
    _inject_natural_ids,
)
from tests.covering import SHAPE_CORPUS, covering_array, tuple_coverage
from tests.helpers import decode_file, load_corpus
from transmog.exceptions import ValidationError

ROWS = covering_array(t=3)


def _cell_id(row: dict[str, Any]) -> str:
    return (
        f"mode={row['array_mode']}|nulls={row['include_nulls']}|"
        f"str={row['stringify_values']}|id={row['id_generation']}|"
        f"fmt={row['format']}|cons={row['consolidate']}|"
        f"coerce={row['coerce_schema']}|shape={row['shape']}"
    )


@pytest.mark.claim("C-COMB-002")
def test_t3_covering_array_is_complete():
    """P: every modeled triple appears in at least one generated row."""
    covered, missing = tuple_coverage(ROWS, t=3)
    assert not missing


@pytest.mark.claim("C-COMB-002")
@pytest.mark.parametrize("row", ROWS, ids=[_cell_id(r) for r in ROWS])
def test_triplewise_cell(tmp_path, row):
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

    main_candidates = [
        p for p in written if p.stem == "entity" or p.stem.startswith("entity_part_")
    ]
    assert main_candidates, cell
    decoded = decode_file(main_candidates[0], row["format"])
    assert decoded, cell
