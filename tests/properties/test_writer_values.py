"""Generated JSON values exercise writers directly, without flattening normalization."""

import math
import tempfile
import warnings
from pathlib import Path

import pytest
from hypothesis import given, settings, strategies as st

from tests.helpers import decode_file
from transmog.writers import create_streaming_writer, create_writer

JSON_VALUES = st.recursive(
    st.one_of(
        st.none(),
        st.booleans(),
        st.integers(-(2**80), 2**80),
        st.floats(allow_nan=False, allow_infinity=False),
        st.text(max_size=8),
    ),
    lambda values: st.one_of(
        st.lists(values, max_size=4),
        st.dictionaries(st.text(max_size=5), values, max_size=4),
    ),
    max_leaves=15,
)


def assert_value_preserved(source, decoded):
    """Allow exact numeric promotion, not changed types, keys, signs, or payloads."""
    assert source == decoded
    if isinstance(source, dict):
        assert type(decoded) is dict
        assert source.keys() == decoded.keys()
        for key, value in source.items():
            assert_value_preserved(value, decoded[key])
    elif isinstance(source, list):
        assert type(decoded) is list
        assert len(source) == len(decoded)
        for value, actual in zip(source, decoded, strict=True):
            assert_value_preserved(value, actual)
    elif type(source) is int:
        assert type(decoded) in (int, float)
    else:
        assert type(decoded) is type(source)
        if isinstance(source, float) and source == 0:
            assert math.copysign(1, source) == math.copysign(1, decoded)


@pytest.mark.claim("C-WRITER-GENERATED-VALUES")
@pytest.mark.parametrize("output_format", ("parquet", "orc"))
@given(
    values=st.lists(JSON_VALUES, min_size=1, max_size=8),
    batch_size=st.sampled_from((1, 3)),
)
@settings(max_examples=60, deadline=None)
def test_json_values_round_trip_automatically(output_format, values, batch_size):
    """P: automatic native/JSON storage preserves values and nested key presence."""
    records = [{"value": value} for value in values]
    with tempfile.TemporaryDirectory(prefix="transmog-writer-property-") as directory:
        root = Path(directory)
        saved = root / f"saved.{output_format}"
        create_writer(output_format).write(records, str(saved))
        writer = create_streaming_writer(
            output_format,
            destination=str(root / "stream"),
            entity_name="items",
            batch_size=batch_size,
        )
        for record in records:
            writer.write_main_records([record])
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", message="Schema deviations", category=UserWarning
            )
            paths = writer.close()
        for decoded in (
            decode_file(saved, output_format, logical=True),
            [
                row
                for path in paths
                for row in decode_file(path, output_format, logical=True)
            ],
        ):
            assert len(decoded) == len(records)
            for source, actual in zip(records, decoded, strict=True):
                assert_value_preserved(source, actual)
