"""Mutation tooling cannot report collection/runner failures as detection."""

import pytest

from scripts.audit_mutations import (
    MUTATIONS,
    ROOT,
    Mutation,
    apply_mutation,
    classify_run,
)


@pytest.mark.claim("C-MUTATION-INTEGRITY")
@pytest.mark.parametrize(
    "returncode,xml,expected",
    [
        (0, '<testsuite><testcase name="ok"/></testsuite>', "survived"),
        (
            1,
            '<testsuite><testcase name="bad"><failure/></testcase></testsuite>',
            "killed",
        ),
        (
            1,
            '<testsuite><testcase name="setup"><error/></testcase></testsuite>',
            "runner_error",
        ),
        (
            2,
            '<testsuite><testcase name="collection"><failure/></testcase></testsuite>',
            "runner_error",
        ),
        (5, "<testsuite/>", "runner_error"),
        (1, '<testsuite><testcase name="no-failure"/></testsuite>', "runner_error"),
        (0, "<invalid", "runner_error"),
        (
            0,
            '<testsuite><testcase name="omitted"><skipped/></testcase></testsuite>',
            "runner_error",
        ),
        (
            0,
            '<!DOCTYPE suite [<!ENTITY value "injected">]><testsuite><testcase name="&value;"/></testsuite>',
            "runner_error",
        ),
    ],
)
def test_only_completed_test_failures_count_as_detection(
    tmp_path, returncode, xml, expected
):
    """P: infrastructure and malformed evidence cannot inflate sensitivity results."""
    path = tmp_path / "result.xml"
    path.write_text(xml, encoding="utf-8")
    status, failures = classify_run(returncode, path)
    assert status == expected
    assert bool(failures) == (expected == "killed")


@pytest.mark.claim("C-MUTATION-INTEGRITY")
@pytest.mark.parametrize("mutation", MUTATIONS, ids=lambda mutation: mutation.name)
def test_declared_targets_apply_to_current_source(mutation):
    """P: declared operators remain unique, applicable, and syntactically valid."""
    source = (ROOT / "src" / "transmog" / mutation.target).read_text(encoding="utf-8")
    assert apply_mutation(source, mutation) != source


@pytest.mark.claim("C-MUTATION-INTEGRITY")
def test_missing_report_is_a_runner_error(tmp_path):
    """P: a subprocess exit alone is not evidence of detection."""
    assert classify_run(1, tmp_path / "absent.xml") == ("runner_error", [])


@pytest.mark.claim("C-MUTATION-INTEGRITY")
def test_mutation_is_function_scoped():
    """P: repeated needles in different functions do not mutate unrelated code."""
    source = "def first():\n    return 1\n\ndef second():\n    return 1\n"
    mutation = Mutation("probe", "module.py", "first", "return 1", "return 2", ())
    assert (
        apply_mutation(source, mutation)
        == "def first():\n    return 2\n\ndef second():\n    return 1\n"
    )


@pytest.mark.claim("C-MUTATION-INTEGRITY")
@pytest.mark.parametrize(
    "needle,replacement", (("absent", "pass"), ("return 1", "return ("))
)
def test_unapplied_or_invalid_mutants_fail(needle, replacement):
    """P: stale needles and invalid syntax cannot be counted as executed mutants."""
    mutation = Mutation("probe", "module.py", "first", needle, replacement, ())
    with pytest.raises((ValueError, SyntaxError)):
        apply_mutation("def first():\n    return 1\n", mutation)
