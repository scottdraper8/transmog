"""Generated value, array, ID, and hierarchy relations over explicit domains."""

from __future__ import annotations

import copy
import json
from typing import Any

import pytest
from hypothesis import given, settings, strategies as st

import transmog as tm
from tests.helpers import hash_config, payload
from transmog.types import ArrayMode

SCALARS = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-10_000, max_value=10_000),
    st.floats(allow_nan=False, allow_infinity=False, min_value=-1e6, max_value=1e6),
    st.text(max_size=30),
)
SCALAR_MAPS = st.dictionaries(
    keys=st.from_regex(r"[a-z][a-z0-9_]{0,9}", fullmatch=True),
    values=SCALARS,
    min_size=1,
    max_size=8,
)
OBJECTS = st.fixed_dictionaries(
    {"name": st.text(min_size=1, max_size=10)},
    optional={"val": SCALARS},
)
OBJECT_LISTS = st.lists(OBJECTS, min_size=1, max_size=5)
ARRAY_RECORDS = st.fixed_dictionaries(
    {"label": st.text(min_size=1, max_size=10)},
    optional={"tags": st.lists(SCALARS, min_size=1, max_size=6), "items": OBJECT_LISTS},
)
CHILD_RECORDS = st.fixed_dictionaries(
    {"label": st.just("parent"), "items": OBJECT_LISTS}
)


@pytest.mark.claim("C-PROP-SCALARS")
@given(data=SCALAR_MAPS)
@settings(max_examples=200, deadline=2000)
def test_scalar_map_round_trips_through_flatten(data: dict[str, Any]) -> None:
    """P: scalar values and types survive, except documented null-like normalization."""
    result = tm.flatten(data, name="entity", config=hash_config(include_nulls=True))
    assert len(result.main) == 1
    assert result.tables == {}
    row = payload(result.main[0])
    expected = {
        key: None if value is None or value == "" else value
        for key, value in data.items()
    }
    assert row == expected
    assert {key: type(value) for key, value in row.items()} == {
        key: type(value) for key, value in expected.items()
    }


@pytest.mark.claim("C-PROP-SKIP")
@given(data=ARRAY_RECORDS)
@settings(max_examples=200, deadline=2000)
def test_skip_removes_all_array_keys(data) -> None:
    """P: SKIP removes arrays without changing the scalar parent payload."""
    result = tm.flatten(
        data, name="entity", config=hash_config(array_mode=ArrayMode.SKIP)
    )
    assert len(result.main) == 1
    assert result.tables == {}
    assert payload(result.main[0]) == {"label": data["label"]}


@pytest.mark.claim("C-PROP-PARENTAGE")
@pytest.mark.parametrize("mode", (ArrayMode.SMART, ArrayMode.SEPARATE))
@given(data=CHILD_RECORDS)
@settings(max_examples=200, deadline=2000)
def test_parent_id_equals_child_parent_id(mode, data) -> None:
    """P: every source child exists and references the parent under hash IDs."""
    result = tm.flatten(data, name="entity", config=hash_config(array_mode=mode))
    assert len(result.main) == 1
    assert set(result.tables) == {"entity_items"}
    rows = result.tables["entity_items"]
    assert len(rows) == len(data["items"])
    assert [row["_parent_id"] for row in rows] == [result.main[0]["_id"]] * len(rows)
    assert [payload(row) for row in rows] == [
        {key: value for key, value in item.items() if value is not None and value != ""}
        for item in data["items"]
    ]


@pytest.mark.claim("C-PROP-INLINE")
@given(data=ARRAY_RECORDS)
@settings(max_examples=200, deadline=2000)
def test_inline_produces_exact_json(data) -> None:
    """P: INLINE preserves every array's JSON values and creates no children."""
    result = tm.flatten(
        data, name="entity", config=hash_config(array_mode=ArrayMode.INLINE)
    )
    assert len(result.main) == 1
    assert result.tables == {}
    expected = {
        key: json.dumps(value, ensure_ascii=False) if isinstance(value, list) else value
        for key, value in data.items()
    }
    assert payload(result.main[0]) == expected
    for key, value in data.items():
        if isinstance(value, list):
            assert json.loads(result.main[0][key]) == value


@pytest.mark.claim("C-PROP-HASH")
@given(data=SCALAR_MAPS)
@settings(max_examples=200, deadline=2000)
def test_hash_id_determinism(data) -> None:
    """P: repeated identical source maps have identical hash IDs."""
    first = tm.flatten(data, config=hash_config())
    second = tm.flatten(data, config=hash_config())
    assert first.main[0]["_id"] == second.main[0]["_id"]


@pytest.mark.claim("C-PROP-RANDOM")
@given(data=CHILD_RECORDS)
@settings(max_examples=200, deadline=2000)
def test_child_ids_unique_under_random_strategy(data) -> None:
    """P: random IDs distinguish all source child occurrences."""
    config = tm.TransmogConfig(id_generation="random", time_field=None)
    result = tm.flatten(data, name="entity", config=config)
    rows = result.tables["entity_items"]
    ids = [row["_id"] for row in rows]
    assert len(ids) == len(data["items"])
    assert len(ids) == len(set(ids))


@pytest.mark.claim("C-PROP-HASH-DUPLICATES")
@given(child=OBJECTS)
@settings(max_examples=200, deadline=2000)
def test_hash_ids_duplicate_for_identical_children(child) -> None:
    """P: identical source children share hash IDs, including optional null fields."""
    data = {"items": [copy.deepcopy(child), copy.deepcopy(child)]}
    result = tm.flatten(data, name="entity", config=hash_config())
    assert set(result.tables) == {"entity_items"}
    rows = result.tables["entity_items"]
    assert len(rows) == 2
    assert rows[0]["_id"] == rows[1]["_id"]
    assert payload(rows[0]) == payload(rows[1])


@pytest.mark.claim("C-PROP-NESTED-PARENTAGE")
@pytest.mark.parametrize("strategy", ("random", "hash", "natural", ["id"]))
@given(
    skills=st.lists(
        st.lists(st.integers(), min_size=1, max_size=4), min_size=2, max_size=4
    )
)
@settings(max_examples=100, deadline=2000)
def test_nested_children_reference_their_exact_owner(strategy, skills) -> None:
    """P: nested array rows retain occurrence ownership through each employee."""
    employees = [
        {"_id": f"employee-{index}", "id": index, "skills": values}
        for index, values in enumerate(skills)
    ]
    data = {"_id": "root", "id": -1, "employees": employees}
    config = hash_config(id_generation=strategy, array_mode=ArrayMode.SEPARATE)
    result = tm.flatten(data, name="company", config=config)
    assert set(result.tables) == {"company_employees", "company_skills"}
    owners = {row["id"]: row["_id"] for row in result.tables["company_employees"]}
    assert set(owners) == set(range(len(employees)))
    actual = [
        (row["_parent_id"], row["value"]) for row in result.tables["company_skills"]
    ]
    expected = [
        (owners[index], value)
        for index, values in enumerate(skills)
        for value in values
    ]
    assert actual == expected
