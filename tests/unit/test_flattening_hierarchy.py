"""Exact hierarchy ownership, batch accumulation, and processing boundaries."""

import pytest

import transmog as tm
from tests.helpers import decode_directory, hash_config, payload
from transmog.flattening import _process_structure, process_record_batch
from transmog.types import ProcessingContext


@pytest.mark.claim("C-HIERARCHY-BOUNDARY")
@pytest.mark.parametrize("data", ({}, None))
def test_empty_structure_has_no_rows(data):
    """P: the internal empty-structure boundary produces neither payload nor children."""
    assert _process_structure(data, "entity", hash_config(), ProcessingContext()) == (
        {},
        {},
    )


@pytest.mark.claim("C-HIERARCHY-METADATA")
def test_structure_preserves_explicit_parent_and_timestamp(simple_data):
    """P: metadata annotations retain an explicit processing context and parent."""
    config = hash_config(time_field="_timestamp")
    main, children = _process_structure(
        simple_data,
        "entity",
        config,
        ProcessingContext(extract_time="fixed-time"),
        parent_id="parent-123",
    )
    assert children == {}
    assert main["_parent_id"] == "parent-123"
    assert main["_timestamp"] == "fixed-time"
    assert payload(main) == {
        "id": 1,
        "name": "Test Entity",
        "status": "active",
        "metadata_created_at": "2023-01-01T00:00:00Z",
        "metadata_updated_at": "2023-01-02T00:00:00Z",
        "metadata_version": 1,
    }


@pytest.mark.claim("C-HIERARCHY-BATCH")
def test_batch_accumulates_every_source_occurrence():
    """P: repeated source parents retain distinct random IDs and child ownership."""
    records = [{"name": "root", "items": [{"name": "child"}]}] * 3
    config = tm.TransmogConfig(time_field=None)
    main, children = process_record_batch(
        records, "entity", config, ProcessingContext()
    )
    assert [payload(row) for row in main] == [{"name": "root"}] * 3
    assert set(children) == {"entity_items"}
    assert [payload(row) for row in children["entity_items"]] == [{"name": "child"}] * 3
    parent_ids = [row["_id"] for row in main]
    assert len(set(parent_ids)) == 3
    assert [row["_parent_id"] for row in children["entity_items"]] == parent_ids


@pytest.mark.claim("C-HIERARCHY-BATCH")
def test_empty_batch_has_no_tables():
    """P: an empty batch has no parent or child rows."""
    assert process_record_batch([], "entity", hash_config(), ProcessingContext()) == (
        [],
        {},
    )


@pytest.mark.claim("C-HIERARCHY-MULTILEVEL")
def test_complete_hierarchy_ownership_in_memory_and_stream(
    tmp_path, complex_nested_data
):
    """P: departments, teams, and members retain their exact immediate owners."""
    data = complex_nested_data
    config = hash_config(id_generation="natural", id_field="id")
    result = tm.flatten(data, name="company", config=config)
    departments = data["organization"]["departments"]
    expected = {
        "company": [
            {
                "id": 1,
                "name": "Complex Entity",
                "organization_id": "org-1",
                "organization_name": "Main Org",
            }
        ],
        "company_organization_departments": [
            {"id": department["id"], "name": department["name"], "_parent_id": 1}
            for department in departments
        ],
        "company_teams": [
            {"id": team["id"], "name": team["name"], "_parent_id": department["id"]}
            for department in departments
            for team in department["teams"]
        ],
        "company_members": [
            {**member, "_parent_id": team["id"]}
            for department in departments
            for team in department["teams"]
            for member in team["members"]
        ],
    }
    assert result.all_tables == expected
    paths = tm.flatten_stream(
        data, tmp_path, name="company", config=config, output_format="csv"
    )
    assert set(paths) == set(tmp_path.glob("*.csv"))
    decoded = decode_directory(tmp_path, "csv")
    assert decoded == {
        name: [{key: str(value) for key, value in row.items()} for row in rows]
        for name, rows in expected.items()
    }


@pytest.mark.claim("C-HIERARCHY-DEPTH")
def test_deep_scalar_path_is_exact():
    """P: deep nesting preserves the entire joined key, not merely a suffix."""
    data = {"root": {}}
    current = data["root"]
    for index in range(20):
        current[f"level_{index}"] = {}
        current = current[f"level_{index}"]
    current["value"] = "deep"
    result = tm.flatten(data, config=hash_config(max_depth=25))
    key = "_".join(["root", *(f"level_{index}" for index in range(20)), "value"])
    assert [payload(row) for row in result.main] == [{key: "deep"}]
    assert result.tables == {}
