"""Parser relations preserve number token types before hashing and writing."""

from __future__ import annotations

import math

import pytest

import transmog as tm
from tests.helpers import corpus_path, hash_config, load_corpus
from transmog.exceptions import ValidationError
from transmog.iterators import (
    get_json_data_iterator,
    get_json_file_iterator,
    get_json_file_iterator_streaming,
    get_jsonl_data_iterator,
)


@pytest.mark.claim("C-PARSER-NUMBERS")
def test_numeric_corpus_streaming_matches_full_load():
    """P: exponent, fractional, integer, and nested number types agree."""
    path = str(corpus_path("numeric_edges.json"))
    full = list(get_json_file_iterator(path))
    streaming = list(get_json_file_iterator_streaming(path))
    assert streaming == full == load_corpus("numeric_edges.json")
    assert type(streaming[0]["value"]) is float
    assert type(streaming[2]["value"]) is int
    assert [type(value) for value in streaming[0]["nested"]["values"]] == [float] * 3
    assert math.copysign(1, streaming[0]["nested"]["values"][1]) == -1
    config = hash_config()
    assert (
        tm.flatten(streaming, config=config).main
        == tm.flatten(full, config=config).main
    )


@pytest.mark.claim("C-PARSER-NUMBERS")
@pytest.mark.parametrize(
    "token",
    ("3", "3.0", "1e0", "1.0e1", "-0.0e1", "-0.0", "2.5", "1E-3", str(2**80 + 1)),
)
def test_number_tokens_preserve_types_and_hashes(tmp_path, token):
    """P: Decimal normalization does not infer token type from normalized spelling."""
    path = tmp_path / "numbers.json"
    path.write_text('[{"value": ' + token + "}]", encoding="utf-8")
    full = list(get_json_file_iterator(str(path)))
    streaming = list(get_json_file_iterator_streaming(str(path)))
    assert streaming == full
    assert type(streaming[0]["value"]) is type(full[0]["value"])
    assert math.copysign(1, streaming[0]["value"]) == math.copysign(1, full[0]["value"])
    assert (
        tm.flatten(streaming, config=hash_config()).main
        == tm.flatten(full, config=hash_config()).main
    )


@pytest.mark.claim("C-PARSER-PRECISION")
def test_large_integer_is_exact_across_text_and_file_paths(tmp_path):
    """P: a valid JSON integer is not reinterpreted as an approximate float."""
    value = 2**80 + 1
    text = '{"value": ' + str(value) + "}"
    path = tmp_path / "numbers.json"
    path.write_text("[" + text + "]", encoding="utf-8")
    for rows in (
        list(get_json_data_iterator(text)),
        list(get_json_data_iterator(text.encode())),
        list(get_jsonl_data_iterator(text)),
        list(get_json_file_iterator(str(path))),
        list(get_json_file_iterator_streaming(str(path))),
    ):
        assert rows == [{"value": value}]
        assert type(rows[0]["value"]) is int


@pytest.mark.claim("C-PARSER-JSON-BOUNDS")
@pytest.mark.parametrize("token", ("NaN", "Infinity", "-Infinity", "1e400"))
def test_nonfinite_json_numbers_are_rejected_consistently(tmp_path, token):
    """P: invalid constants and overflowing float tokens do not silently become null."""
    text = '[{"value": ' + token + "}]"
    path = tmp_path / "numbers.json"
    path.write_text(text, encoding="utf-8")
    for iterator in (get_json_file_iterator, get_json_file_iterator_streaming):
        with pytest.raises(ValidationError):
            list(iterator(str(path)))
    with pytest.raises(ValidationError):
        list(get_json_data_iterator(text))
