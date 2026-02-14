"""Tests for progress_callback parameter in flatten() and flatten_stream()."""

import json

import pytest

import transmog as tm
from transmog import TransmogConfig


def _make_records(n: int) -> list[dict]:
    return [{"id": i, "name": f"Record {i}"} for i in range(n)]


class TestFlattenProgressCallback:
    """Test progress_callback behavior in flatten()."""

    def test_callback_called_per_batch(self):
        """10 records with batch_size=3 produces 4 callbacks (3+3+3+1)."""
        calls: list[tuple[int, int | None]] = []
        config = TransmogConfig(batch_size=3)

        tm.flatten(
            _make_records(10),
            config=config,
            progress_callback=lambda p, t: calls.append((p, t)),
        )

        assert len(calls) == 4

    def test_callback_total_for_list(self):
        """total_records equals list length in every call."""
        calls: list[tuple[int, int | None]] = []
        data = _make_records(5)
        config = TransmogConfig(batch_size=2)

        tm.flatten(
            data,
            config=config,
            progress_callback=lambda p, t: calls.append((p, t)),
        )

        assert all(t == 5 for _, t in calls)

    def test_callback_total_for_dict(self):
        """total_records is 1 for dict input."""
        calls: list[tuple[int, int | None]] = []

        tm.flatten(
            {"id": 1, "name": "single"},
            progress_callback=lambda p, t: calls.append((p, t)),
        )

        assert len(calls) == 1
        assert calls[0] == (1, 1)

    def test_callback_total_none_for_file(self, tmp_path):
        """total_records is None for file path input."""
        calls: list[tuple[int, int | None]] = []
        file_path = tmp_path / "test.json"
        file_path.write_text(json.dumps([{"id": 1}]))

        tm.flatten(
            str(file_path),
            progress_callback=lambda p, t: calls.append((p, t)),
        )

        assert len(calls) >= 1
        assert all(t is None for _, t in calls)

    def test_callback_records_increase_monotonically(self):
        """records_processed strictly increases across calls."""
        calls: list[tuple[int, int | None]] = []
        config = TransmogConfig(batch_size=2)

        tm.flatten(
            _make_records(7),
            config=config,
            progress_callback=lambda p, t: calls.append((p, t)),
        )

        processed_values = [p for p, _ in calls]
        assert all(
            processed_values[i] < processed_values[i + 1]
            for i in range(len(processed_values) - 1)
        )

    def test_no_callback_for_empty_input(self):
        """flatten([]) never invokes the callback."""
        calls: list[tuple[int, int | None]] = []

        tm.flatten(
            [],
            progress_callback=lambda p, t: calls.append((p, t)),
        )

        assert len(calls) == 0

    def test_none_callback_is_noop(self):
        """progress_callback=None does not raise."""
        result = tm.flatten(_make_records(3), progress_callback=None)
        assert len(result.main) == 3


class TestFlattenStreamProgressCallback:
    """Test progress_callback behavior in flatten_stream()."""

    def test_stream_callback_called_per_batch(self, tmp_path):
        """10 records with batch_size=3 produces 4 callbacks."""
        calls: list[tuple[int, int | None]] = []
        config = TransmogConfig(batch_size=3)

        tm.flatten_stream(
            _make_records(10),
            output_path=tmp_path / "out",
            config=config,
            progress_callback=lambda p, t: calls.append((p, t)),
        )

        assert len(calls) == 4

    def test_stream_callback_total_for_list(self, tmp_path):
        """total_records equals list length in every call."""
        calls: list[tuple[int, int | None]] = []
        data = _make_records(6)
        config = TransmogConfig(batch_size=2)

        tm.flatten_stream(
            data,
            output_path=tmp_path / "out",
            config=config,
            progress_callback=lambda p, t: calls.append((p, t)),
        )

        assert all(t == 6 for _, t in calls)

    def test_stream_callback_total_none_for_file(self, tmp_path):
        """total_records is None for file input."""
        calls: list[tuple[int, int | None]] = []
        file_path = tmp_path / "test.json"
        file_path.write_text(json.dumps([{"id": 1}, {"id": 2}]))

        tm.flatten_stream(
            str(file_path),
            output_path=tmp_path / "out",
            progress_callback=lambda p, t: calls.append((p, t)),
        )

        assert len(calls) >= 1
        assert all(t is None for _, t in calls)

    def test_stream_final_count_matches_input(self, tmp_path):
        """Final records_processed equals total input size."""
        calls: list[tuple[int, int | None]] = []
        config = TransmogConfig(batch_size=3)

        tm.flatten_stream(
            _make_records(8),
            output_path=tmp_path / "out",
            config=config,
            progress_callback=lambda p, t: calls.append((p, t)),
        )

        assert calls[-1][0] == 8


class TestProgressCallbackEdgeCases:
    """Test edge cases for progress callback."""

    def test_callback_exception_propagates(self):
        """User callback errors are not swallowed."""

        def bad_callback(p, t):
            raise ValueError("user error")

        with pytest.raises(ValueError, match="user error"):
            tm.flatten(_make_records(3), progress_callback=bad_callback)

    def test_batch_size_one(self):
        """batch_size=1 with 3 records produces 3 callbacks."""
        calls: list[tuple[int, int | None]] = []
        config = TransmogConfig(batch_size=1)

        tm.flatten(
            _make_records(3),
            config=config,
            progress_callback=lambda p, t: calls.append((p, t)),
        )

        assert len(calls) == 3

    def test_batch_size_larger_than_data(self):
        """batch_size=1000 with 5 records produces 1 callback."""
        calls: list[tuple[int, int | None]] = []
        config = TransmogConfig(batch_size=1000)

        tm.flatten(
            _make_records(5),
            config=config,
            progress_callback=lambda p, t: calls.append((p, t)),
        )

        assert len(calls) == 1

    def test_exact_batch_multiple(self):
        """6 records with batch_size=3 produces exactly 2 callbacks."""
        calls: list[tuple[int, int | None]] = []
        config = TransmogConfig(batch_size=3)

        tm.flatten(
            _make_records(6),
            config=config,
            progress_callback=lambda p, t: calls.append((p, t)),
        )

        assert len(calls) == 2
        assert calls[-1][0] == 6
