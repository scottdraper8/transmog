import json
from collections.abc import Iterator

import pytest

import transmog as tm
from transmog.config import TransmogConfig
from transmog.error import ConfigurationError


def test_processor_handles_single_record(simple_data):
    config = TransmogConfig()
    result = tm.flatten(simple_data, name="single", config=config)

    assert len(result.main) == 1
    assert result.main[0]["id"] == simple_data["id"]


def test_processor_handles_list(batch_data):
    config = TransmogConfig()
    result = tm.flatten(batch_data, name="list", config=config)

    assert len(result.main) == len(batch_data)


def test_processor_respects_batch_size(batch_data):
    config = TransmogConfig(batch_size=3)
    result = tm.flatten(batch_data, name="batched", config=config)

    assert len(result.main) == len(batch_data)


def test_processor_accepts_generator(batch_data):
    def record_generator() -> Iterator[dict[str, int]]:
        yield from batch_data

    config = TransmogConfig(batch_size=4)
    result = tm.flatten(record_generator(), name="generator", config=config)

    assert len(result.main) == len(batch_data)


def test_processor_accepts_json_string(batch_data):
    json_data = json.dumps(batch_data)
    config = TransmogConfig(batch_size=5)
    result = tm.flatten(json_data, name="json_string", config=config)

    assert len(result.main) == len(batch_data)


def test_processor_accepts_bytes_json(batch_data):
    byte_data = json.dumps(batch_data).encode("utf-8")
    config = TransmogConfig()
    result = tm.flatten(byte_data, name="bytes", config=config)

    assert len(result.main) == len(batch_data)


def test_processor_handles_json_file(json_file):
    config = TransmogConfig()
    result = tm.flatten(json_file, name="file", config=config)

    assert len(result.main) >= 1


def test_processor_handles_jsonl_file(jsonl_file, batch_data):
    config = TransmogConfig(batch_size=2)
    result = tm.flatten(jsonl_file, name="jsonl", config=config)

    assert len(result.main) == len(batch_data)


def test_processor_rejects_none_input():
    config = TransmogConfig()

    with pytest.raises(ConfigurationError):
        tm.flatten(None, name="invalid", config=config)  # type: ignore[arg-type]


def test_processor_rejects_non_mapping_records():
    def bad_generator():
        yield {"id": 1}
        yield ["not", "a", "dict"]

    config = TransmogConfig()

    with pytest.raises(ConfigurationError):
        tm.flatten(list(bad_generator()), name="bad_records", config=config)
