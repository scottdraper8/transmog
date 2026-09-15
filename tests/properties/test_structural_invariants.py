"""Property-based sensors for structural invariants of the flattening algebra.

These encode assumptions that must hold regardless of input shape. The
properties are different assumption sources from characterization or
metamorphic tests (paper §7.1, §7.7): the generator explores inputs
the corpus does not contain.

Properties were identified from Phase 1 characterization observations, not
invented from source inspection (plan §6 Phase 6, rule 1).
"""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import given, settings, strategies as st

import transmog as tm
from tests.helpers import hash_config, payload
from transmog.types import ArrayMode

# --- Strategies ---

_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-10_000, max_value=10_000),
    st.floats(allow_nan=False, allow_infinity=False, min_value=-1e6, max_value=1e6),
    st.text(max_size=30),
)

_scalar_dicts = st.dictionaries(
    keys=st.from_regex(r"[a-z][a-z0-9_]{0,9}", fullmatch=True),
    values=_scalars,
    min_size=1,
    max_size=8,
)

_primitive_lists = st.lists(_scalars, min_size=1, max_size=6)

_object_items = st.fixed_dictionaries(
    {"name": st.text(min_size=1, max_size=10)},
    optional={"val": _scalars},
)

_object_lists = st.lists(_object_items, min_size=1, max_size=5)

_records_with_arrays = st.fixed_dictionaries(
    {"label": st.text(min_size=1, max_size=10)},
    optional={
        "tags": _primitive_lists,
        "items": _object_lists,
    },
)

# --- Properties ---


@pytest.mark.claim("C-PROP-001")
@given(data=_scalar_dicts)
@settings(max_examples=200, deadline=2000)
def test_scalar_map_round_trips_through_flatten(data: dict[str, Any]) -> None:
    """P: A flat scalar dict survives flatten with all non-null-like keys intact.

    No arrays means no child tables regardless of array_mode.
    """
    config = hash_config(include_nulls=True)
    result = tm.flatten(data, name="entity", config=config)
    assert len(result.main) == 1
    assert result.tables == {}
    row = payload(result.main[0])
    for key, value in data.items():
        if value is None or value == "":
            assert row.get(key) is None, (
                f"key={key!r} expected None, got {row.get(key)!r}"
            )
        elif isinstance(value, float) and value != value:
            # NaN — is_null_like returns True
            pass
        else:
            assert key in row, f"key={key!r} missing from flattened output"


@pytest.mark.claim("C-PROP-002")
@given(data=_records_with_arrays)
@settings(max_examples=200, deadline=2000)
def test_skip_removes_all_array_keys(data: dict[str, Any]) -> None:
    """P: SKIP mode removes every array key; no child tables are produced."""
    config = hash_config(array_mode=ArrayMode.SKIP)
    result = tm.flatten(data, name="entity", config=config)
    assert result.tables == {}
    row = payload(result.main[0])
    for key, value in data.items():
        if isinstance(value, list):
            assert key not in row, f"array key {key!r} survived SKIP"


@pytest.mark.claim("C-PROP-003")
@given(data=_records_with_arrays)
@settings(max_examples=200, deadline=2000)
def test_parent_id_equals_child_parent_id(data: dict[str, Any]) -> None:
    """P: Parent _id matches every child row's _parent_id under hash IDs."""
    config = hash_config(array_mode=ArrayMode.SMART)
    result = tm.flatten(data, name="entity", config=config)
    if not result.main:
        return
    parent_id = result.main[0].get("_id")
    assert parent_id is not None
    for table_name, rows in result.tables.items():
        for row in rows:
            assert row.get("_parent_id") == parent_id, (
                f"child in {table_name!r} has _parent_id={row.get('_parent_id')!r}, "
                f"expected {parent_id!r}"
            )


@pytest.mark.claim("C-PROP-003")
@given(data=_records_with_arrays)
@settings(max_examples=200, deadline=2000)
def test_parent_id_equals_child_parent_id_separate(data: dict[str, Any]) -> None:
    """P: Parent _id matches every child row's _parent_id under SEPARATE + hash."""
    config = hash_config(array_mode=ArrayMode.SEPARATE)
    result = tm.flatten(data, name="entity", config=config)
    if not result.main:
        return
    parent_id = result.main[0].get("_id")
    assert parent_id is not None
    for table_name, rows in result.tables.items():
        for row in rows:
            assert row.get("_parent_id") == parent_id, (
                f"child in {table_name!r} has _parent_id={row.get('_parent_id')!r}, "
                f"expected {parent_id!r}"
            )


@pytest.mark.claim("C-PROP-004")
@given(data=_records_with_arrays)
@settings(max_examples=200, deadline=2000)
def test_inline_produces_no_child_tables(data: dict[str, Any]) -> None:
    """P: INLINE mode never produces child tables; arrays become JSON strings."""
    config = hash_config(array_mode=ArrayMode.INLINE)
    result = tm.flatten(data, name="entity", config=config)
    assert result.tables == {}
    row = payload(result.main[0])
    for key, value in data.items():
        if isinstance(value, list) and value:
            assert isinstance(row.get(key), str), (
                f"INLINE key {key!r} should be a string, got {type(row.get(key))}"
            )


@pytest.mark.claim("C-PROP-005")
@given(data=_scalar_dicts)
@settings(max_examples=200, deadline=2000)
def test_hash_id_determinism(data: dict[str, Any]) -> None:
    """P: hash IDs are deterministic — two calls with identical input produce identical _id."""
    config = hash_config()
    first = tm.flatten(data, name="entity", config=config)
    second = tm.flatten(data, name="entity", config=config)
    assert first.main[0]["_id"] == second.main[0]["_id"]


@pytest.mark.claim("C-PROP-006")
@given(data=_records_with_arrays)
@settings(max_examples=200, deadline=2000)
def test_child_ids_unique_under_random_strategy(data: dict[str, Any]) -> None:
    """P: Every child row _id is unique under random ID strategy.

    Hash strategy produces duplicate IDs for identical child records by
    design (content hash). Random strategy generates a fresh uuid4 per
    row and must be unique.
    """
    config = tm.TransmogConfig(
        id_generation="random",
        time_field=None,
        array_mode=ArrayMode.SMART,
    )
    result = tm.flatten(data, name="entity", config=config)
    all_ids: list[str] = []
    for rows in result.tables.values():
        for row in rows:
            child_id = row.get("_id")
            assert child_id is not None
            all_ids.append(child_id)
    assert len(all_ids) == len(set(all_ids)), "duplicate child _id values"


@pytest.mark.claim("C-PROP-006")
def test_hash_ids_duplicate_for_identical_children() -> None:
    """P: Hash IDs collide when child records have identical content.

    This is actuality, not a verdict. Hash strategy hashes the original
    record content; identical source dicts → identical hash.
    Uses a deterministic fixture rather than Hypothesis because the
    property depends on pre-flatten content identity, which generated
    optional fields can break (e.g., {name: '0'} vs {name: '0', val: None}
    have different content but can look identical after null stripping).
    """
    data = {"label": "x", "items": [{"name": "dup"}, {"name": "dup"}]}
    config = hash_config(array_mode=ArrayMode.SMART)
    result = tm.flatten(data, name="entity", config=config)
    for rows in result.tables.values():
        ids = [row.get("_id", "") for row in rows]
        assert len(ids) == 2
        assert ids[0] == ids[1], (
            f"identical child content produced different hash IDs: {ids}"
        )
