"""Tests for progress_callback parameter in flatten() and flatten_stream()."""

import json

import pytest

import transmog as tm
from transmog import TransmogConfig


def _make_records(n: int) -> list[dict]:
    return [{"id": i, "name": f"Record {i}"} for i in range(n)]


def _collect_calls(
    n: int, batch_size: int, tmp_path=None
) -> list[tuple[int, int | None]]:
    calls: list[tuple[int, int | None]] = []

    def callback(processed: int, total: int | None) -> None:
        calls.append((processed, total))

    config = TransmogConfig(batch_size=batch_size)
    if tmp_path is None:
        tm.flatten(_make_records(n), config=config, progress_callback=callback)
    else:
        tm.flatten_stream(
            _make_records(n),
            output_path=tmp_path / "out",
            config=config,
            progress_callback=callback,
        )
    return calls


class TestFlattenProgressCallback:
    """Test progress_callback behavior in flatten()."""

    @pytest.mark.parametrize(
        "n,batch_size,expected_calls",
        [
            (10, 3, 4),
            (3, 1, 3),
            (5, 1000, 1),
            (6, 3, 2),
        ],
        ids=["uneven", "size_one", "larger_than_data", "exact_multiple"],
    )
    def test_callback_called_per_batch(self, n, batch_size, expected_calls):
        """Callback fires once per flushed batch."""
        calls = _collect_calls(n, batch_size)
        assert len(calls) == expected_calls
        assert calls[-1][0] == n
        processed = [p for p, _ in calls]
        assert processed == sorted(processed)
        assert len(set(processed)) == len(processed)

    def test_callback_total_for_list(self):
        """total_records equals list length in every call."""
        calls = _collect_calls(5, 2)
        assert all(total == 5 for _, total in calls)

    def test_callback_total_for_dict(self):
        """total_records is 1 for dict input."""
        calls: list[tuple[int, int | None]] = []
        tm.flatten(
            {"id": 1, "name": "single"},
            progress_callback=lambda p, t: calls.append((p, t)),
        )
        assert calls == [(1, 1)]

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
        assert all(total is None for _, total in calls)

    def test_no_callback_for_empty_input(self):
        """flatten([]) never invokes the callback."""
        calls: list[tuple[int, int | None]] = []
        tm.flatten([], progress_callback=lambda p, t: calls.append((p, t)))
        assert calls == []

    def test_none_callback_is_noop(self):
        """progress_callback=None does not raise."""
        result = tm.flatten(_make_records(3), progress_callback=None)
        assert len(result.main) == 3


class TestFlattenStreamProgressCallback:
    """Test progress_callback behavior in flatten_stream()."""

    @pytest.mark.parametrize(
        "n,batch_size,expected_calls",
        [
            (10, 3, 4),
            (8, 3, 3),
        ],
        ids=["uneven", "final_count"],
    )
    def test_stream_callback_called_per_batch(
        self, tmp_path, n, batch_size, expected_calls
    ):
        """Streaming callback fires once per flushed batch."""
        calls = _collect_calls(n, batch_size, tmp_path=tmp_path)
        assert len(calls) == expected_calls
        assert calls[-1][0] == n

    def test_stream_callback_total_for_list(self, tmp_path):
        """total_records equals list length in every call."""
        calls = _collect_calls(6, 2, tmp_path=tmp_path)
        assert all(total == 6 for _, total in calls)

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
        assert all(total is None for _, total in calls)


class TestProgressCallbackEdgeCases:
    """Test edge cases for progress callback."""

    def test_callback_exception_propagates(self):
        """User callback errors are not swallowed."""

        def bad_callback(processed, total):
            raise ValueError("user error")

        with pytest.raises(ValueError, match="user error"):
            tm.flatten(_make_records(3), progress_callback=bad_callback)
