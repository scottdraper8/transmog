"""Metadata overwrites, timestamp semantics, and context default isolation."""

from datetime import datetime, timezone
from uuid import UUID

import pytest

from transmog.config import TransmogConfig
from transmog.flattening import annotate_with_metadata, get_current_timestamp
from transmog.types import ProcessingContext


@pytest.mark.claim("C-METADATA-TIME")
def test_timestamp_represents_current_utc_with_microseconds():
    """P: the timestamp uses the documented UTC format and falls within the observation."""
    before = datetime.now(timezone.utc)
    timestamp = get_current_timestamp()
    after = datetime.now(timezone.utc)
    parsed = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f").replace(
        tzinfo=timezone.utc
    )
    assert before <= parsed <= after
    assert parsed.strftime("%Y-%m-%d %H:%M:%S.%f") == timestamp


@pytest.mark.claim("C-METADATA-ANNOTATION")
@pytest.mark.parametrize(
    "record", ({}, {"name": "test", "value": 42}, {"_id": "old", "_timestamp": "old"})
)
def test_explicit_metadata_overwrites_in_place(record):
    """P: explicit metadata replaces existing fields without changing payload."""
    original = dict(record)
    config = TransmogConfig()
    annotated = annotate_with_metadata(
        record,
        config,
        parent_id="parent",
        transmog_time="fixed-time",
        record_id="fixed-id",
    )
    assert annotated is record
    assert annotated == {
        **original,
        "_id": "fixed-id",
        "_timestamp": "fixed-time",
        "_parent_id": "parent",
    }


@pytest.mark.claim("C-METADATA-ANNOTATION")
def test_random_annotation_has_fresh_uuid_and_disabled_timestamp():
    """P: random metadata generates a UUID4 without inventing a disabled timestamp."""
    config = TransmogConfig(time_field=None)
    first = annotate_with_metadata({"name": "x"}, config)
    second = annotate_with_metadata({"name": "x"}, config)
    assert set(first) == set(second) == {"name", "_id"}
    assert first["name"] == second["name"] == "x"
    assert first["_id"] != second["_id"]
    assert UUID(first["_id"]).version == UUID(second["_id"]).version == 4


@pytest.mark.claim("C-METADATA-CONTEXT")
def test_context_defaults_are_independent():
    """P: contexts do not share path state or invent a caller-supplied timestamp."""
    first, second = ProcessingContext(), ProcessingContext()
    assert first.current_depth == second.current_depth == 0
    assert first.path_components == second.path_components == []
    first.path_components.append("child")
    assert second.path_components == []
    assert first.extract_time == second.extract_time == ""
