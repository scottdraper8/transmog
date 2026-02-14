"""Tests for logging support across transmog modules."""

import logging
from collections.abc import Iterator

import pytest
import transmog as tm
from transmog.config import TransmogConfig
from transmog.flattening import process_record_batch
from transmog.iterators import get_data_iterator
from transmog.streaming import stream_process
from transmog.types import ProcessingContext


class TestNullHandlerConfiguration:
    """Verify the root transmog logger has a NullHandler."""

    def test_transmog_logger_has_null_handler(self):
        """Verify NullHandler is attached to the transmog logger."""
        root_logger = logging.getLogger("transmog")
        handler_types = [type(h) for h in root_logger.handlers]
        assert logging.NullHandler in handler_types


class TestFlattenLogging:
    """Verify flatten() emits INFO start/complete messages."""

    def test_flatten_logs_start_and_complete(self, caplog):
        """Verify both start and complete INFO messages are emitted."""
        with caplog.at_level(logging.DEBUG, logger="transmog"):
            tm.flatten({"key": "value"}, name="test")

        messages = [r.message for r in caplog.records if r.name == "transmog.api"]
        start_msgs = [m for m in messages if "flatten started" in m]
        complete_msgs = [m for m in messages if "flatten completed" in m]
        assert len(start_msgs) == 1
        assert len(complete_msgs) == 1

    def test_flatten_logs_input_type(self, caplog):
        """Verify input type appears in the start message."""
        with caplog.at_level(logging.DEBUG, logger="transmog"):
            tm.flatten({"key": "value"}, name="test")

        messages = [r.message for r in caplog.records if r.name == "transmog.api"]
        start_msg = next(m for m in messages if "flatten started" in m)
        assert "input_type=dict" in start_msg

    def test_flatten_logs_entity_name(self, caplog):
        """Verify entity name appears in the start message."""
        with caplog.at_level(logging.DEBUG, logger="transmog"):
            tm.flatten({"key": "value"}, name="my_entity")

        messages = [r.message for r in caplog.records if r.name == "transmog.api"]
        start_msg = next(m for m in messages if "flatten started" in m)
        assert "name=my_entity" in start_msg

    def test_flatten_logs_record_counts(self, caplog):
        """Verify record counts appear in the completion message."""
        data = [{"a": 1}, {"a": 2}, {"a": 3}]
        with caplog.at_level(logging.DEBUG, logger="transmog"):
            tm.flatten(data, name="test")

        messages = [r.message for r in caplog.records if r.name == "transmog.api"]
        complete_msg = next(m for m in messages if "flatten completed" in m)
        assert "main_records=3" in complete_msg
        assert "child_tables=0" in complete_msg


class TestFlattenStreamLogging:
    """Verify flatten_stream() emits INFO start/complete messages."""

    def test_flatten_stream_logs_start_and_complete(self, caplog, tmp_path):
        """Verify both start and complete INFO messages are emitted."""
        with caplog.at_level(logging.DEBUG, logger="transmog"):
            tm.flatten_stream(
                [{"x": 1}],
                str(tmp_path / "out"),
                name="test",
                output_format="csv",
            )

        messages = [r.message for r in caplog.records if r.name == "transmog.api"]
        start_msgs = [m for m in messages if "flatten_stream started" in m]
        complete_msgs = [m for m in messages if "flatten_stream completed" in m]
        assert len(start_msgs) == 1
        assert len(complete_msgs) == 1

    def test_flatten_stream_logs_format(self, caplog, tmp_path):
        """Verify output format appears in the start message."""
        with caplog.at_level(logging.DEBUG, logger="transmog"):
            tm.flatten_stream(
                [{"x": 1}],
                str(tmp_path / "out"),
                name="test",
                output_format="csv",
            )

        messages = [r.message for r in caplog.records if r.name == "transmog.api"]
        start_msg = next(m for m in messages if "flatten_stream started" in m)
        assert "format=csv" in start_msg


class TestStreamBatchLogging:
    """Verify streaming batch progress logging."""

    def test_batch_progress_logged(self, caplog, tmp_path):
        """Verify correct number of batch progress messages."""
        data = [{"v": i} for i in range(5)]
        config = TransmogConfig(batch_size=2)
        with caplog.at_level(logging.DEBUG, logger="transmog"):
            tm.flatten_stream(
                data,
                str(tmp_path / "out"),
                name="test",
                output_format="csv",
                config=config,
            )

        messages = [
            r.message
            for r in caplog.records
            if r.name == "transmog.streaming" and "stream batch" in r.message
        ]
        # 5 records with batch_size=2 yields 3 batches (2+2+1)
        assert len(messages) == 3

    def test_batch_total_records_accurate(self, caplog, tmp_path):
        """Verify total record count is accurate in completion message."""
        data = [{"v": i} for i in range(7)]
        config = TransmogConfig(batch_size=3)
        with caplog.at_level(logging.DEBUG, logger="transmog"):
            tm.flatten_stream(
                data,
                str(tmp_path / "out"),
                name="test",
                output_format="csv",
                config=config,
            )

        messages = [
            r.message
            for r in caplog.records
            if r.name == "transmog.streaming" and "stream completed" in r.message
        ]
        assert len(messages) == 1
        assert "total_records=7" in messages[0]

    def test_stream_completed_not_logged_on_exception(self, caplog, tmp_path):
        """Verify 'stream completed' is not emitted when processing raises."""

        def failing_iterator() -> Iterator[dict]:
            yield {"a": 1}
            raise RuntimeError("simulated mid-stream failure")

        config = TransmogConfig()
        with caplog.at_level(logging.DEBUG, logger="transmog"):
            with pytest.raises(RuntimeError, match="simulated mid-stream failure"):
                stream_process(
                    config=config,
                    data=failing_iterator(),
                    entity_name="test",
                    output_format="csv",
                    output_destination=str(tmp_path / "out"),
                )

        stream_completed = [
            r.message
            for r in caplog.records
            if r.name == "transmog.streaming" and "stream completed" in r.message
        ]
        assert len(stream_completed) == 0


class TestDebugLevelLogging:
    """Verify DEBUG-level logging from internal modules."""

    def test_process_record_batch_logs_at_debug(self, caplog):
        """Verify process_record_batch emits DEBUG batch message."""
        records = [{"a": 1}]
        config = TransmogConfig()
        context = ProcessingContext(extract_time="2025-01-01 00:00:00.000000")
        with caplog.at_level(logging.DEBUG, logger="transmog"):
            process_record_batch(
                records=records,
                entity_name="test",
                config=config,
                _context=context,
            )

        messages = [
            r.message for r in caplog.records if r.name == "transmog.flattening"
        ]
        batch_msgs = [m for m in messages if "processing batch" in m]
        assert len(batch_msgs) == 1
        assert "records=1" in batch_msgs[0]

    def test_format_detection_logs_at_debug(self, caplog):
        """Verify format detection emits DEBUG message."""
        json_str = '{"key": "value"}'
        with caplog.at_level(logging.DEBUG, logger="transmog"):
            iterator = get_data_iterator(json_str)
            list(iterator)

        messages = [r.message for r in caplog.records if r.name == "transmog.iterators"]
        detection_msgs = [m for m in messages if "string format detected" in m]
        assert len(detection_msgs) == 1


class TestLoggerModuleNames:
    """Verify each module's logger uses __name__."""

    def test_api_logger_name(self, caplog):
        """Verify transmog.api logger name appears in records."""
        with caplog.at_level(logging.DEBUG, logger="transmog"):
            tm.flatten({"a": 1}, name="test")

        logger_names = {r.name for r in caplog.records}
        assert "transmog.api" in logger_names

    def test_streaming_logger_name(self, caplog, tmp_path):
        """Verify transmog.streaming logger name appears in records."""
        with caplog.at_level(logging.DEBUG, logger="transmog"):
            tm.flatten_stream(
                [{"a": 1}],
                str(tmp_path / "out"),
                name="test",
                output_format="csv",
            )

        logger_names = {r.name for r in caplog.records}
        assert "transmog.streaming" in logger_names

    def test_flattening_logger_name(self, caplog):
        """Verify transmog.flattening logger name appears in records."""
        with caplog.at_level(logging.DEBUG, logger="transmog"):
            tm.flatten({"a": 1}, name="test")

        logger_names = {r.name for r in caplog.records}
        assert "transmog.flattening" in logger_names
