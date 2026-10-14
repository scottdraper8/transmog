"""Evidence bookkeeping distinguishes identification, execution, and correctness."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from tests import conftest as protocol


@pytest.mark.claim("C-EVIDENCE-PROTOCOL")
@pytest.mark.parametrize(
    "claim,doc", ((None, "P: example"), ("", "P: example"), ("C-EXAMPLE", None))
)
def test_unidentified_or_undocumented_sensors_fail_collection(claim, doc):
    """P: evidence-channel tests cannot silently omit claim identification."""
    marker = pytest.mark.claim(claim).mark if claim is not None else None
    item = SimpleNamespace(
        path=Path(protocol.__file__).parent / "relations" / "test_probe.py",
        nodeid="probe",
        obj=SimpleNamespace(__doc__=doc),
        get_closest_marker=lambda name: marker,
    )
    with pytest.raises(pytest.UsageError, match="claim ID and a proposition docstring"):
        protocol.pytest_collection_modifyitems([item])


@pytest.mark.claim("C-EVIDENCE-PROTOCOL")
@pytest.mark.parametrize(
    "exitstatus",
    (pytest.ExitCode.OK, pytest.ExitCode.INTERRUPTED, pytest.ExitCode.INTERNAL_ERROR),
)
@pytest.mark.parametrize("completed", (False, True))
def test_corpus_load_requires_actual_call_and_preserves_runner_errors(
    tmp_path, monkeypatch, exitstatus, completed
):
    """P: setup-only loads do not count, and audit gaps do not mask runner errors."""
    (tmp_path / "record.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(protocol, "CORPUS_DIR", tmp_path)
    monkeypatch.setattr(protocol, "CORPUS_SENSORS", {"record.json": {"probe"}})
    monkeypatch.setattr(
        protocol, "COMPLETED_SENSORS", {"probe"} if completed else set()
    )
    monkeypatch.setattr(protocol, "audit", lambda: ({}, set()))
    session = SimpleNamespace(
        config=SimpleNamespace(
            getoption=lambda name: True,
            pluginmanager=SimpleNamespace(get_plugin=lambda name: None),
        ),
        exitstatus=exitstatus,
    )
    protocol.pytest_sessionfinish(session, exitstatus)
    expected = (
        pytest.ExitCode.TESTS_FAILED
        if not completed and exitstatus == pytest.ExitCode.OK
        else exitstatus
    )
    assert session.exitstatus == expected


@pytest.mark.claim("C-EVIDENCE-PROTOCOL")
@pytest.mark.parametrize(
    "when,skipped,expected",
    (("setup", False, set()), ("call", True, set()), ("call", False, {"probe"})),
)
def test_only_executed_bodies_register(when, skipped, expected, monkeypatch):
    """P: skipped bodies and successful setup alone do not establish execution."""
    monkeypatch.setattr(protocol, "COMPLETED_SENSORS", set())
    protocol.pytest_runtest_logreport(
        SimpleNamespace(when=when, skipped=skipped, nodeid="probe")
    )
    assert protocol.COMPLETED_SENSORS == expected


@pytest.mark.claim("C-EVIDENCE-PROTOCOL")
def test_each_session_discards_old_fixture_use(monkeypatch):
    """P: a second pytest session cannot reuse a prior session's corpus evidence."""
    monkeypatch.setattr(protocol, "CORPUS_SENSORS", {"record.json": {"probe"}})
    monkeypatch.setattr(protocol, "COMPLETED_SENSORS", {"probe"})
    protocol.pytest_sessionstart(None)
    assert protocol.CORPUS_SENSORS == {}
    assert protocol.COMPLETED_SENSORS == set()
