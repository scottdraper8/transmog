"""Avro codec sensors across all codecs available in the current environment.

Each codec exercises the full round-trip: flatten → save or flatten_stream
with that codec → decode → compare payload. Only codecs that are actually
usable are parametrized — the test count reflects what was measured.
"""

from __future__ import annotations

import io

import fastavro
import pytest

import transmog as tm
from tests.helpers import decode_file, hash_config, payload
from transmog.writers.avro import AVRO_CODECS


def _available_codecs() -> list[str]:
    """Return codecs that fastavro can actually use in this environment."""
    schema = {
        "type": "record",
        "name": "Probe",
        "fields": [{"name": "x", "type": "int"}],
    }
    parsed = fastavro.parse_schema(schema)
    available = []
    for codec in AVRO_CODECS:
        buf = io.BytesIO()
        try:
            fastavro.writer(buf, parsed, [{"x": 1}], codec=codec)
            available.append(codec)
        except (ValueError, ImportError):
            pass
    return available


CODECS = _available_codecs()


@pytest.mark.claim("C-AVRO-CODEC")
@pytest.mark.parametrize("codec", CODECS)
def test_avro_codec_round_trip_save(tmp_path, codec):
    """P: flatten + save round-trips through this Avro codec."""
    data = [{"id": 1, "name": "Alice", "score": 95}]
    config = hash_config()
    result = tm.flatten(data, name="items", config=config)

    dest = tmp_path / "items.avro"
    result.save(dest, output_format="avro", compression=codec)

    rows = decode_file(dest, "avro")
    assert len(rows) == 1
    assert payload(rows[0]) == {"id": 1, "name": "Alice", "score": 95}


@pytest.mark.claim("C-AVRO-CODEC")
@pytest.mark.parametrize("codec", CODECS)
def test_avro_codec_round_trip_stream(tmp_path, codec):
    """P: flatten_stream round-trips through this Avro codec."""
    data = [{"id": 1, "name": "Alice", "score": 95}]
    config = hash_config()
    dest = tmp_path / "stream"

    paths = tm.flatten_stream(
        data,
        dest,
        name="items",
        output_format="avro",
        config=config,
        consolidate=True,
        codec=codec,
    )
    assert paths

    rows = decode_file(dest / "items.avro", "avro")
    assert len(rows) == 1
    assert payload(rows[0]) == {"id": 1, "name": "Alice", "score": 95}


@pytest.mark.claim("C-AVRO-CODEC")
@pytest.mark.parametrize("codec", CODECS)
def test_avro_codec_save_vs_stream_equality(tmp_path, codec):
    """P: save and stream produce identical decoded payloads for this codec."""
    data = [
        {"id": 1, "name": "Alice", "score": 95},
        {"id": 2, "name": "Bob", "score": 87},
    ]
    config = hash_config()
    result = tm.flatten(data, name="items", config=config)

    save_path = tmp_path / "save.avro"
    result.save(save_path, output_format="avro", compression=codec)
    save_rows = [payload(r) for r in decode_file(save_path, "avro")]

    stream_dir = tmp_path / "stream"
    tm.flatten_stream(
        data,
        stream_dir,
        name="items",
        output_format="avro",
        config=config,
        consolidate=True,
        codec=codec,
    )
    stream_rows = [payload(r) for r in decode_file(stream_dir / "items.avro", "avro")]

    assert save_rows == stream_rows
