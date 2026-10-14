"""Exact-table sensors for the flattening algebra.

Continuity sensors, not a specification. If a pinned table changes, the
test fails — that failure is the signal. The fix goes in the code or the
pin is updated with a reviewed change, not silently.

Claim markers identify propositions; assertions define their oracles.
"""

from __future__ import annotations

import json

import pytest

import transmog as tm
from tests.helpers import (
    flatten_corpus,
    hash_config,
    load_corpus,
    payload,
    tables_payload,
)
from transmog.exceptions import ValidationError
from transmog.types import ArrayMode

# Hash-ID compatibility pins for timestamp-free corpus fixtures.
ID_PRIMITIVE = "4038da2c-e57a-53f3-b969-a4226e9d6b01"
ID_OBJECT = "3fa890d6-7c73-5c40-acc6-38e77dbecd65"
ID_MIXED = "13b44abb-cada-5871-adab-16d72180bf81"
ID_EMPTY = "e2f04cc1-8ce7-53bb-8c34-336153076bad"
ID_COLLISION = "54076cef-6792-51fc-aa88-1798bdbdef50"
ID_MAX_DEPTH = "45cd29b3-ff0a-555f-945c-4a805cec5bbd"
ID_NESTED = "614aaeec-5ff0-524c-a2b1-621e156b66bc"
ID_INLINE_TYPES = "699bd031-0611-5c8c-a7b5-a74dd24abedf"
ID_UNICODE = "ec81bd85-59b1-5ae1-a85e-56410531e407"
ID_NULL_LIKE_ARRAY = "a7bb342c-1acf-5a75-85fd-354031f39f47"
ID_EMP_ALICE = "1a56d269-dcc5-5ea0-812d-96d4ace2183b"
ID_EMP_BOB = "efd1b24b-7aea-51d9-9d43-7c4d34eb35c7"
ID_EMP_ALICE_SKILLS = "5137f188-bb20-56bd-a5c9-1ce95ff8389b"
ID_EMP_BOB_SKILLS = "a2666487-123f-5f73-b021-637fc84b137a"


@pytest.mark.claim("C-ALG-001")
def test_smart_primitive_array_stays_on_parent():
    """P: SMART keeps primitive arrays on the parent; no child table."""
    result = flatten_corpus(
        "primitive_array.json", entity="company", array_mode=ArrayMode.SMART
    )
    assert tables_payload(result) == {
        "main": [{"name": "Acme", "tags": ["tech", "startup", "ai"]}]
    }
    assert result.tables == {}
    assert result.main[0]["_id"] == ID_PRIMITIVE


@pytest.mark.claim("C-ALG-002")
def test_smart_object_array_becomes_child_table():
    """P: SMART extracts object arrays to {entity}_{field}; parent has no array column."""
    result = flatten_corpus(
        "object_array.json", entity="company", array_mode=ArrayMode.SMART
    )
    tables = tables_payload(result)
    assert tables["main"] == [{"name": "Acme"}]
    assert "employees" not in result.main[0]
    assert tables["company_employees"] == [
        {"id": 101, "name": "Alice", "role": "Engineer"},
        {"id": 102, "name": "Bob", "role": "Designer"},
    ]
    assert result.main[0]["_id"] == ID_OBJECT
    child_by_name = {row["name"]: row for row in result.tables["company_employees"]}
    assert child_by_name["Alice"]["_id"] == ID_EMP_ALICE
    assert child_by_name["Bob"]["_id"] == ID_EMP_BOB
    assert child_by_name["Alice"]["_parent_id"] == ID_OBJECT
    assert child_by_name["Bob"]["_parent_id"] == ID_OBJECT


@pytest.mark.claim("C-ALG-003")
def test_separate_primitive_array_value_rows():
    """P: SEPARATE extracts primitive arrays to {value: item} child rows."""
    result = flatten_corpus(
        "primitive_array.json", entity="company", array_mode=ArrayMode.SEPARATE
    )
    tables = tables_payload(result)
    assert tables["main"] == [{"name": "Acme"}]
    assert "tags" not in result.main[0]
    assert tables["company_tags"] == [
        {"value": "ai"},
        {"value": "startup"},
        {"value": "tech"},
    ]
    assert result.main[0]["_id"] == ID_PRIMITIVE
    for row in result.tables["company_tags"]:
        assert row["_parent_id"] == ID_PRIMITIVE
        assert "value" in row


@pytest.mark.claim("C-ALG-004")
def test_smart_mixed_array_exact_layout_include_nulls_false():
    """P: SMART mixed [dict, str, int, None, list] — exact layout, nulls omitted."""
    result = flatten_corpus(
        "mixed_array.json",
        entity="entity",
        array_mode=ArrayMode.SMART,
        include_nulls=False,
    )
    tables = tables_payload(result)
    assert tables["main"] == [{"name": "mixed"}]
    assert "items" not in result.main[0]
    assert tables["entity_items"] == [
        {"k": "obj"},
        {"value": "str"},
        {"value": 7},
        {"value": [1, 2]},
    ]
    assert result.main[0]["_id"] == ID_MIXED
    for row in result.tables["entity_items"]:
        assert row["_parent_id"] == ID_MIXED


@pytest.mark.claim("C-ALG-004")
def test_smart_mixed_array_include_nulls_true_keeps_none_row():
    """P: SMART mixed with include_nulls=True adds {value: None} child row."""
    result = flatten_corpus(
        "mixed_array.json",
        entity="entity",
        array_mode=ArrayMode.SMART,
        include_nulls=True,
    )
    tables = tables_payload(result)
    assert tables["entity_items"] == [
        {"k": "obj"},
        {"value": "str"},
        {"value": 7},
        {"value": [1, 2]},
        {"value": None},
    ]


@pytest.mark.claim("C-ALG-004")
def test_separate_mixed_array_matches_smart_extraction():
    """P: SEPARATE mixed extraction matches SMART once the array is complex."""
    result = flatten_corpus(
        "mixed_array.json",
        entity="entity",
        array_mode=ArrayMode.SEPARATE,
        include_nulls=False,
    )
    assert tables_payload(result)["entity_items"] == [
        {"k": "obj"},
        {"value": "str"},
        {"value": 7},
        {"value": [1, 2]},
    ]


@pytest.mark.claim("C-ALG-004")
def test_inline_mixed_array_is_json_dumps_of_original():
    """P: INLINE mixed array is json.dumps of the original list including null."""
    original = load_corpus("mixed_array.json")
    result = flatten_corpus(
        "mixed_array.json", entity="entity", array_mode=ArrayMode.INLINE
    )
    assert result.tables == {}
    assert result.main[0]["items"] == json.dumps(original["items"], ensure_ascii=False)
    assert result.main[0]["items"] == '[{"k": "obj"}, "str", 7, null, [1, 2]]'


@pytest.mark.claim("C-ALG-004")
def test_skip_mixed_array_omits_items():
    """P: SKIP mixed array omits the array key and creates no child table."""
    result = flatten_corpus(
        "mixed_array.json", entity="entity", array_mode=ArrayMode.SKIP
    )
    assert tables_payload(result) == {"main": [{"name": "mixed"}]}
    assert result.tables == {}


@pytest.mark.claim("C-ALG-005")
def test_inline_preserves_types_in_json_string():
    """P: INLINE is json.dumps of the original array, ensure_ascii=False."""
    original = load_corpus("inline_types.json")
    result = flatten_corpus(
        "inline_types.json", entity="entity", array_mode=ArrayMode.INLINE
    )
    dumped = json.dumps(original["arr"], ensure_ascii=False)
    assert result.main[0]["arr"] == dumped
    assert result.main[0]["arr"] == '[1, "two", true, null]'
    assert result.main[0]["_id"] == ID_INLINE_TYPES
    assert result.tables == {}


@pytest.mark.claim("C-ALG-005")
def test_inline_unicode_not_escaped():
    """P: INLINE does not ASCII-escape non-ASCII array items."""
    original = load_corpus("unicode_array.json")
    result = flatten_corpus(
        "unicode_array.json", entity="entity", array_mode=ArrayMode.INLINE
    )
    assert result.main[0]["name"] == "café"
    assert result.main[0]["tags"] == json.dumps(original["tags"], ensure_ascii=False)
    assert "日本語" in result.main[0]["tags"]
    assert "\\u" not in result.main[0]["tags"]
    assert result.main[0]["_id"] == ID_UNICODE


@pytest.mark.claim("C-ALG-006")
def test_skip_omits_primitive_and_object_arrays():
    """P: SKIP omits array keys; scalars remain; no child tables."""
    primitive = flatten_corpus(
        "primitive_array.json", entity="company", array_mode=ArrayMode.SKIP
    )
    objects = flatten_corpus(
        "object_array.json", entity="company", array_mode=ArrayMode.SKIP
    )
    assert tables_payload(primitive) == {"main": [{"name": "Acme"}]}
    assert tables_payload(objects) == {"main": [{"name": "Acme"}]}
    assert primitive.tables == {}
    assert objects.tables == {}


@pytest.mark.claim("C-ALG-007")
def test_empty_containers_omitted_and_nulls_skipped_by_default():
    """P: empty dict/list omitted; None and '' omitted; 0 and False kept."""
    result = flatten_corpus("empty_and_nulls.json", include_nulls=False)
    row = payload(result.main[0])
    assert row == {"name": "x", "zero": 0, "false_val": False}
    assert "empty_dict" not in row
    assert "empty_list" not in row
    assert "null_field" not in row
    assert "empty_str" not in row
    assert result.main[0]["_id"] == ID_EMPTY


@pytest.mark.claim("C-ALG-007")
def test_include_nulls_stores_null_like_scalars_as_none():
    """P: include_nulls=True keeps null-like scalar keys as None, not ''.

    Empty dict/list remain omitted. Empty string is stored as None because
    is_null_like('') is True.
    """
    result = flatten_corpus("empty_and_nulls.json", include_nulls=True)
    row = payload(result.main[0])
    assert row == {
        "name": "x",
        "null_field": None,
        "empty_str": None,
        "zero": 0,
        "false_val": False,
    }
    assert "empty_dict" not in row
    assert "empty_list" not in row


@pytest.mark.claim("C-ALG-008")
def test_nested_path_collision_last_write_wins():
    """P: user_name and user.name share the parent key; nested value wins.

    Corpus insertion order is user_name then user.
    """
    result = flatten_corpus("nested_path_collision.json")
    assert payload(result.main[0]) == {"user_name": "nested"}
    assert result.main[0]["_id"] == ID_COLLISION


@pytest.mark.claim("C-ALG-009")
def test_max_depth_2_omits_subtree_and_warns_at_a_b():
    """P: max_depth=2 remaining keys are only root scalars; warning at a.b."""
    with pytest.warns(UserWarning, match=r"max_depth=2 reached at a\.b"):
        result = flatten_corpus("max_depth.json", max_depth=2)
    row = payload(result.main[0])
    assert row == {"keep": "root"}
    assert "a_b_c_d" not in result.main[0]
    assert result.tables == {}
    assert result.main[0]["_id"] == ID_MAX_DEPTH


@pytest.mark.claim("C-ALG-009")
def test_max_depth_1_omits_subtree_and_warns_at_a():
    """P: max_depth=1 remaining keys are only root scalars; warning at a."""
    with pytest.warns(UserWarning, match=r"max_depth=1 reached at a;"):
        result = flatten_corpus("max_depth.json", max_depth=1)
    assert payload(result.main[0]) == {"keep": "root"}
    assert result.main[0]["_id"] == ID_MAX_DEPTH


@pytest.mark.claim("C-ALG-010")
def test_natural_id_parent_present_child_missing_gets_generated_id():
    """P: natural parent uses existing _id; child without _id gets a generated _id."""
    result = flatten_corpus(
        "natural_id_parent.json",
        entity="entity",
        id_generation="natural",
        array_mode=ArrayMode.SMART,
    )
    assert result.main[0]["_id"] == "parent-1"
    assert payload(result.main[0]) == {"name": "P"}
    kids = {row["name"]: row for row in result.tables["entity_kids"]}
    assert kids["c2"]["_id"] == "child-2"
    assert kids["c1"]["_id"] != "child-2"
    assert kids["c1"]["_id"]  # generated, non-empty
    assert "id" not in kids["c1"]
    assert kids["c1"]["_parent_id"] == "parent-1"
    assert kids["c2"]["_parent_id"] == "parent-1"


@pytest.mark.claim("C-ALG-010")
def test_natural_id_parent_missing_raises():
    """P: natural strategy raises ValidationError when parent lacks id_field."""
    config = hash_config(id_generation="natural")
    with pytest.raises(ValidationError, match="requires field '_id'"):
        tm.flatten(
            load_corpus("natural_id_parent_missing.json"),
            name="entity",
            config=config,
        )


@pytest.mark.claim("C-ALG-010")
def test_natural_custom_id_field_does_not_invent_id_on_child():
    """P: child missing configured id_field gets _id, not an invented id field."""
    result = flatten_corpus(
        "natural_id_field.json",
        entity="entity",
        id_generation="natural",
        id_field="id",
        array_mode=ArrayMode.SMART,
    )
    assert result.main[0]["id"] == "parent-1"
    assert "_id" not in result.main[0]
    kids = {row["name"]: row for row in result.tables["entity_kids"]}
    assert "id" not in kids["c1"]
    assert kids["c1"]["_id"]
    assert kids["c2"]["id"] == "child-2"
    assert "_id" not in kids["c2"]
    assert kids["c1"]["_parent_id"] == "parent-1"
    assert kids["c2"]["_parent_id"] == "parent-1"


@pytest.mark.claim("C-ALG-011")
@pytest.mark.parametrize("include_nulls", [False, True])
def test_smart_simple_array_retains_null_like_items(include_nulls):
    """P: SMART simple arrays keep the original list, including None and ''."""
    result = flatten_corpus(
        "null_like_in_simple_array.json",
        array_mode=ArrayMode.SMART,
        include_nulls=include_nulls,
    )
    assert result.main[0]["tags"] == ["a", None, "", "b"]
    assert result.tables == {}
    assert result.main[0]["_id"] == ID_NULL_LIKE_ARRAY


@pytest.mark.claim("C-ALG-012")
def test_smart_nested_object_arrays_keep_simple_skills_on_child():
    """P: SMART extracts employees; nested simple skills stay on the child row."""
    result = flatten_corpus(
        "nested_object_arrays.json",
        entity="company",
        array_mode=ArrayMode.SMART,
    )
    tables = tables_payload(result)
    assert tables["main"] == [
        {"id": 1, "name": "Company", "tags": ["tech", "startup", "ai"]}
    ]
    assert tables["company_employees"] == [
        {
            "id": 101,
            "name": "Alice",
            "role": "Engineer",
            "skills": ["python", "sql", "docker"],
        },
        {
            "id": 102,
            "name": "Bob",
            "role": "Designer",
            "skills": ["figma", "photoshop"],
        },
    ]
    assert "company_skills" not in result.tables
    assert result.main[0]["_id"] == ID_NESTED
    by_name = {row["name"]: row for row in result.tables["company_employees"]}
    assert by_name["Alice"]["_id"] == ID_EMP_ALICE_SKILLS
    assert by_name["Bob"]["_id"] == ID_EMP_BOB_SKILLS
    assert by_name["Alice"]["_parent_id"] == ID_NESTED


@pytest.mark.claim("C-ALG-012")
def test_separate_nested_arrays_parent_to_intermediate():
    """P: SEPARATE nested skills rows parent to their employee, not the root.

    Intermediate parentage is preserved: skills._parent_id matches the
    employee._id, enabling join paths through the full hierarchy.
    """
    result = flatten_corpus(
        "nested_object_arrays.json",
        entity="company",
        array_mode=ArrayMode.SEPARATE,
    )
    tables = tables_payload(result)
    assert tables["main"] == [{"id": 1, "name": "Company"}]
    assert tables["company_employees"] == [
        {"id": 101, "name": "Alice", "role": "Engineer"},
        {"id": 102, "name": "Bob", "role": "Designer"},
    ]
    assert tables["company_tags"] == [
        {"value": "ai"},
        {"value": "startup"},
        {"value": "tech"},
    ]
    assert tables["company_skills"] == [
        {"value": "docker"},
        {"value": "figma"},
        {"value": "photoshop"},
        {"value": "python"},
        {"value": "sql"},
    ]
    employee_ids = {
        row["name"]: row["_id"] for row in result.tables["company_employees"]
    }
    assert {
        row["value"]: row["_parent_id"] for row in result.tables["company_skills"]
    } == {
        "python": employee_ids["Alice"],
        "sql": employee_ids["Alice"],
        "docker": employee_ids["Alice"],
        "figma": employee_ids["Bob"],
        "photoshop": employee_ids["Bob"],
    }
    assert all(
        row["_parent_id"] == ID_NESTED for row in result.tables["company_employees"]
    )


@pytest.mark.claim("C-ALG-002")
def test_stringify_object_array_stringifies_leaf_values_not_parent_id():
    """P: stringify_values converts employee id 101 to '101'; parent hash unchanged."""
    result = flatten_corpus(
        "object_array.json",
        entity="company",
        array_mode=ArrayMode.SMART,
        stringify_values=True,
    )
    assert result.main[0]["_id"] == ID_OBJECT
    tables = tables_payload(result)
    assert tables["company_employees"] == [
        {"id": "101", "name": "Alice", "role": "Engineer"},
        {"id": "102", "name": "Bob", "role": "Designer"},
    ]


@pytest.mark.claim("C-ALG-001")
def test_stringify_already_string_tags_unchanged():
    """P: stringify_values leaves already-string SMART array items unchanged."""
    result = flatten_corpus(
        "primitive_array.json",
        entity="company",
        array_mode=ArrayMode.SMART,
        stringify_values=True,
    )
    assert result.main[0]["tags"] == ["tech", "startup", "ai"]
    assert result.main[0]["_id"] == ID_PRIMITIVE
