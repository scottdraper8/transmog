"""Every declared Avro codec is exercised; capability failures fail tests."""

from __future__ import annotations

import json
import warnings

import fastavro
import pytest

import transmog as tm
from tests.helpers import decode_file, hash_config, payload
from transmog.writers.avro import AVRO_CODECS


@pytest.mark.claim("C-AVRO-CODEC-MANIFEST")
def test_declared_codecs_match_documented_support():
    """P: changing the declared codec set cannot silently shrink collection."""
    assert set(AVRO_CODECS) == {
        "null",
        "deflate",
        "snappy",
        "zstandard",
        "lz4",
        "bzip2",
        "xz",
    }


@pytest.mark.claim("C-AVRO-CODEC")
@pytest.mark.parametrize("codec", AVRO_CODECS)
@pytest.mark.parametrize("batch_size", (1, 5000))
@pytest.mark.parametrize("consolidate", (False, True))
@pytest.mark.parametrize("coerce_schema", (False, True))
def test_avro_codec_metadata_and_value_fidelity(
    tmp_path, codec, batch_size, consolidate, coerce_schema
):
    """P: requested codecs and payloads survive save, parts, rewrite, and merge."""
    data = [
        {"id": 1, "name": "Alice", "score": 95, "tags": [1, 2.5]},
        {"id": 2, "name": "Bob", "score": 87.5, "tags": [None, "日本語"]},
    ]
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Small batch_size", category=UserWarning
        )
        warnings.filterwarnings(
            "ignore", message="Schema deviations", category=UserWarning
        )
        config = hash_config(batch_size=batch_size)
        save_path = tmp_path / "save.avro"
        tm.flatten(data, name="items", config=config).save(
            save_path,
            output_format="avro",
            compression=codec,
        )
        paths = tm.flatten_stream(
            data,
            tmp_path / "stream",
            name="items",
            output_format="avro",
            config=config,
            compression=codec,
            consolidate=consolidate,
            coerce_schema=coerce_schema,
        )
    expected = [
        {**record, "tags": json.dumps(record["tags"], ensure_ascii=False)}
        for record in data
    ]
    saved = [payload(row) for row in decode_file(save_path, "avro")]
    streamed = [
        payload(row) for path in sorted(paths) for row in decode_file(path, "avro")
    ]
    assert saved == streamed == expected
    assert len(paths) == (1 if consolidate or batch_size > len(data) else len(data))
    for path in [save_path, *paths]:
        with path.open("rb") as handle:
            reader = fastavro.reader(handle)
            assert reader.metadata["avro.codec"] == codec
            assert list(reader)
