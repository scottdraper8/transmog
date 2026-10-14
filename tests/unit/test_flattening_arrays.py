"""Direct array-extraction boundaries, without vacuous table checks."""

import pytest

from tests.helpers import hash_config, payload
from transmog.flattening import flatten_json
from transmog.types import ArrayMode, ProcessingContext


@pytest.mark.claim("C-ARRAY-DIRECT")
@pytest.mark.parametrize("mode", tuple(ArrayMode))
def test_direct_array_modes(mode):
    """P: each mode has an exact payload and exact extracted table set."""
    data = {"name": "root", "tags": ["a", 2, None], "items": [{"name": "child"}]}
    config = hash_config(array_mode=mode, include_nulls=True)
    flattened, arrays = flatten_json(
        data,
        config,
        _collect_arrays=True,
        _parent_id="parent",
        _entity_name="entity",
    )
    if mode == ArrayMode.SKIP:
        assert flattened == {"name": "root"}
        assert arrays == {}
    elif mode == ArrayMode.INLINE:
        assert flattened == {
            "name": "root",
            "tags": '["a", 2, null]',
            "items": '[{"name": "child"}]',
        }
        assert arrays == {}
    else:
        expected = {"entity_items": [{"name": "child"}]}
        if mode == ArrayMode.SMART:
            assert flattened == {"name": "root", "tags": ["a", 2, None]}
        else:
            assert flattened == {"name": "root"}
            expected["entity_tags"] = [{"value": "a"}, {"value": 2}, {"value": None}]
        assert {
            name: [payload(row) for row in rows] for name, rows in arrays.items()
        } == expected
        assert all(
            row["_parent_id"] == "parent" for rows in arrays.values() for row in rows
        )


@pytest.mark.claim("C-ARRAY-CUSTOM-NATURAL")
def test_custom_natural_fallback_is_reused_for_nested_parentage():
    """P: a child without a custom natural ID uses its fallback ID for its own children."""
    data = {
        "employees": [
            {"name": "Alice", "external_id": "alice", "skills": ["sql"]},
            {"name": "Bob", "skills": ["figma", "design"]},
        ]
    }
    config = hash_config(
        array_mode=ArrayMode.SEPARATE,
        id_generation="natural",
        id_field="external_id",
        parent_field="owner",
    )
    flattened, arrays = flatten_json(
        data,
        config,
        _collect_arrays=True,
        _parent_id="root",
        _entity_name="company",
    )
    assert flattened == {}
    assert set(arrays) == {"company_employees", "company_skills"}
    employees = {row["name"]: row for row in arrays["company_employees"]}
    assert employees["Alice"]["external_id"] == "alice"
    assert "external_id" not in employees["Bob"]
    assert "_id" in employees["Bob"]
    assert all(row["owner"] == "root" for row in employees.values())
    assert [(row["value"], row["owner"]) for row in arrays["company_skills"]] == [
        ("sql", "alice"),
        ("figma", employees["Bob"]["_id"]),
        ("design", employees["Bob"]["_id"]),
    ]


@pytest.mark.claim("C-ARRAY-DEPTH")
def test_depth_limit_retains_child_scalars_and_warns():
    """P: truncating a child's nested dictionary retains that child's root scalars."""
    config = hash_config(array_mode=ArrayMode.SEPARATE, max_depth=2)
    with pytest.warns(UserWarning, match="max_depth=2 reached at deep"):
        flattened, arrays = flatten_json(
            {"items": [{"name": "child", "deep": {"value": 42}}]},
            config,
            ProcessingContext(),
            _collect_arrays=True,
            _parent_id="root",
            _entity_name="entity",
        )
    assert flattened == {}
    assert set(arrays) == {"entity_items"}
    assert [payload(row) for row in arrays["entity_items"]] == [{"name": "child"}]
    assert arrays["entity_items"][0]["_parent_id"] == "root"


@pytest.mark.claim("C-ARRAY-EMPTY")
@pytest.mark.parametrize("mode", tuple(ArrayMode))
def test_empty_containers_produce_no_child_occurrences(mode):
    """P: empty containers disappear, while false and zero remain."""
    flattened, arrays = flatten_json(
        {"empty_list": [], "empty_dict": {}, "zero": 0, "flag": False},
        hash_config(array_mode=mode),
        _collect_arrays=True,
        _entity_name="entity",
    )
    assert flattened == {"zero": 0, "flag": False}
    assert arrays == {}
