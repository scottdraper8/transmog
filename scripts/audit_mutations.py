#!/usr/bin/env python3
"""Run scoped fault injections in a disposable source/test snapshot.

Results are JSON observations, not survivor verdicts or a merge score.
The checkout is never mutated. Missing targets, invalid mutants, collection
errors, and runner failures are reported separately from test detection.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.metadata
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from defusedxml.common import DefusedXmlException
from defusedxml.ElementTree import ParseError, parse

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Mutation:
    """One function-scoped source perturbation and its sensor selection."""

    name: str
    target: str
    function: str
    needle: str
    replacement: str
    tests: tuple[str, ...]


ALGEBRA = ("tests/algebra", "tests/properties/test_structural_invariants.py")
WRITERS = (
    "tests/relations/test_writer_fidelity.py",
    "tests/relations/test_container_fidelity.py",
    "tests/relations/test_automatic_schema.py",
    "tests/relations/test_structured_writer_fidelity.py",
    "tests/relations/test_native_promotion.py",
    "tests/relations/test_boolean_fidelity.py",
    "tests/relations/test_physical_file_reads.py",
    "tests/properties/test_writer_values.py",
    "tests/combinations",
)
MUTATIONS = (
    Mutation(
        "decimal_integer_coercion",
        "iterators.py",
        "_normalize_ijson_value",
        "native = float(value)",
        "native = int(value)",
        ("tests/relations/test_parser_fidelity.py",),
    ),
    Mutation(
        "root_instead_of_intermediate_parent",
        "flattening.py",
        "_process_array_items",
        "_parent_id=item_id if _collect_arrays else _parent_id,",
        "_parent_id=_parent_id,",
        ALGEBRA,
    ),
    Mutation(
        "drop_nested_arrays",
        "flattening.py",
        "_process_array_items",
        "arrays.setdefault(nested_table_name, []).extend(nested_records)",
        "pass",
        ALGEBRA,
    ),
    Mutation(
        "omit_array_null_filter",
        "flattening.py",
        "_process_array_items",
        "if is_null_like(item) and not config.include_nulls:",
        "if False:",
        ALGEBRA,
    ),
    Mutation(
        "skip_array_passthrough",
        "flattening.py",
        "flatten_json",
        "if config.array_mode == ArrayMode.SKIP:\n                continue",
        "if config.array_mode == ArrayMode.SKIP:\n                result[current_path] = value\n                continue",
        ALGEBRA,
    ),
    Mutation(
        "ignore_depth_limit",
        "flattening.py",
        "flatten_json",
        "if _context.current_depth >= config.max_depth:",
        "if False:",
        ALGEBRA,
    ),
    Mutation(
        "escape_inline_unicode",
        "flattening.py",
        "flatten_json",
        "json.dumps(value, ensure_ascii=False)",
        "json.dumps(value, ensure_ascii=True)",
        ALGEBRA,
    ),
    Mutation(
        "empty_string_not_null",
        "flattening.py",
        "is_null_like",
        'value is None or value == ""',
        "value is None",
        ALGEBRA,
    ),
    Mutation(
        "csv_special_float_passthrough",
        "writers/csv.py",
        "_prepare_record",
        '_normalize_special_floats(value, null_replacement="")',
        "value",
        WRITERS,
    ),
    Mutation(
        "list_special_float_passthrough",
        "writers/base.py",
        "_normalize_special_floats",
        "[_normalize_special_floats(item, null_replacement) for item in value]",
        "value",
        WRITERS,
    ),
    Mutation(
        "reverse_field_order",
        "writers/base.py",
        "_collect_field_names",
        "return list(seen)",
        "return list(reversed(seen))",
        ("tests/relations/test_differential.py",),
    ),
    Mutation(
        "arrow_first_record_only",
        "writers/arrow_base.py",
        "_records_to_arrow_table",
        "for record in records]",
        "for record in records[:1]]",
        WRITERS,
    ),
    Mutation(
        "arrow_disable_numeric_promotion",
        "writers/arrow_base.py",
        "_build_unified_schema",
        'promote_options="permissive"',
        'promote_options="default"',
        WRITERS,
    ),
    Mutation(
        "arrow_boolean_to_number",
        "writers/arrow_base.py",
        "_native_shape_is_lossless",
        "return bool(pa.types.is_boolean(data_type))",
        "return True",
        WRITERS,
    ),
    Mutation(
        "arrow_json_marker_missing",
        "writers/arrow_base.py",
        "_json_field",
        "{**(field.metadata or {}), **_JSON_ENCODING}",
        "field.metadata",
        WRITERS,
    ),
    Mutation(
        "arrow_use_target_encoding_for_source",
        "writers/arrow_base.py",
        "_promote_table_to_schema",
        "table.schema.field(field.name)",
        "field",
        WRITERS,
    ),
    Mutation(
        "arrow_unsafe_numeric_cast",
        "writers/arrow_base.py",
        "_cast_native_column",
        "return column.cast(data_type, safe=True)",
        "return column.cast(data_type, safe=False)",
        WRITERS,
    ),
    Mutation(
        "arrow_skip_schema_promotion",
        "writers/arrow_base.py",
        "_consolidate_parts",
        "table = self._promote_table_to_schema(table, schema)",
        "pass",
        WRITERS,
    ),
    Mutation(
        "avro_first_part_types_only",
        "writers/avro.py",
        "_build_unified_schema",
        "for schema in schemas:",
        "for schema in schemas[:1]:",
        WRITERS,
    ),
    Mutation(
        "avro_first_union_branch",
        "writers/avro.py",
        "_coerce_value_to_schema",
        "return (native_type, normalized)",
        'return (next(t for t in types if t != "null"), normalized)',
        WRITERS,
    ),
    Mutation(
        "coercion_no_rewrite",
        "writers/base.py",
        "_coerce_part_files",
        "self._rewrite_part(file_path, target_schema)",
        "pass",
        WRITERS,
    ),
)


def apply_mutation(source: str, mutation: Mutation) -> str:
    """Require one function and one needle, then validate the resulting syntax."""
    tree = ast.parse(source)
    functions = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == mutation.function
    ]
    if len(functions) != 1:
        raise ValueError(f"expected one function, found {len(functions)}")
    function = functions[0]
    lines = source.splitlines(keepends=True)
    start, end = function.lineno - 1, function.end_lineno
    block = "".join(lines[start:end])
    if block.count(mutation.needle) != 1:
        raise ValueError(f"expected one needle, found {block.count(mutation.needle)}")
    modified = (
        "".join(lines[:start])
        + block.replace(mutation.needle, mutation.replacement, 1)
        + "".join(lines[end:])
    )
    compile(modified, mutation.target, "exec")
    return modified


def classify_run(returncode: int, junit: Path) -> tuple[str, list[str]]:
    """Count only completed assertion/test failures as mutant detection."""
    if returncode not in (0, 1) or not junit.is_file():
        return "runner_error", []
    try:
        root = parse(junit).getroot()
    except (OSError, ParseError, DefusedXmlException):
        return "runner_error", []
    if root.findall(".//error"):
        return "runner_error", []
    cases = root.findall(".//testcase")
    if not cases or all(case.find("skipped") is not None for case in cases):
        return "runner_error", []
    failures = [
        case.attrib.get("classname", "") + "::" + case.attrib.get("name", "")
        for case in cases
        if case.find("failure") is not None
    ]
    if returncode == 0 and not failures:
        return "survived", []
    if returncode == 1 and failures:
        return "killed", failures
    return "runner_error", failures


def run_sensors(snapshot: Path, tests: tuple[str, ...], label: str) -> dict:
    """Run a fresh interpreter against snapshot code, with bounded execution time."""
    junit = snapshot / f"{label}.xml"
    env = {
        **os.environ,
        "PYTHONPATH": str(snapshot / "src"),
        "PYTHONDONTWRITEBYTECODE": "1",
        "HYPOTHESIS_STORAGE_DIRECTORY": str(snapshot / "hypothesis"),
    }
    command = [
        sys.executable,
        "-B",
        "-m",
        "pytest",
        *tests,
        "-q",
        "--tb=short",
        "-p",
        "no:cacheprovider",
        f"--junitxml={junit}",
    ]
    try:
        result = subprocess.run(
            command,
            cwd=snapshot,
            env=env,
            text=True,
            capture_output=True,
            timeout=180,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"status": "runner_error", "detail": "sensor timeout", "tests": tests}
    status, failures = classify_run(result.returncode, junit)
    return {
        "status": status,
        "returncode": result.returncode,
        "failures": failures,
        "tests": tests,
        "output": (result.stdout + result.stderr)[-8000:],
    }


def provenance() -> dict:
    """Record revision, actual source/test content, interpreter, and dependencies."""
    git = shutil.which("git")
    if git is None:
        raise RuntimeError("git is required to record mutation provenance")
    revision = subprocess.run(
        [git, "rev-parse", "HEAD"], cwd=ROOT, text=True, capture_output=True, check=True
    ).stdout.strip()
    digest = hashlib.sha256()
    for directory in ("src", "tests", "scripts"):
        for path in sorted((ROOT / directory).rglob("*")):
            if path.suffix in (".py", ".json") and not any(
                part.startswith(".") or part == "__pycache__"
                for part in path.relative_to(ROOT).parts
            ):
                digest.update(str(path.relative_to(ROOT)).encode())
                digest.update(path.read_bytes())
    for name in ("pyproject.toml", "uv.lock"):
        digest.update(name.encode())
        digest.update((ROOT / name).read_bytes())
    return {
        "revision": revision,
        "content_sha256": digest.hexdigest(),
        "python": sys.version,
        "platform": platform.platform(),
        "dependencies": dict(
            sorted(
                (distribution.metadata["Name"], distribution.version)
                for distribution in importlib.metadata.distributions()
            )
        ),
    }


def audit() -> dict:
    """Measure mutations without changing source, tests, or caches in the checkout."""
    report = {"provenance": provenance(), "mutations": []}
    with tempfile.TemporaryDirectory(prefix="transmog-mutations-") as temp:
        snapshot = Path(temp)
        ignore = shutil.ignore_patterns("__pycache__", ".*", "*.pyc", "*.so")
        for directory in ("src", "tests", "scripts"):
            shutil.copytree(ROOT / directory, snapshot / directory, ignore=ignore)
        shutil.copy2(ROOT / "pyproject.toml", snapshot / "pyproject.toml")
        baseline = run_sensors(snapshot, ("tests", "--audit-corpus"), "baseline")
        report["baseline"] = baseline
        if baseline["status"] != "survived":
            return report
        for mutation in MUTATIONS:
            target = snapshot / "src" / "transmog" / mutation.target
            original = target.read_text(encoding="utf-8")
            try:
                modified = apply_mutation(original, mutation)
                target.write_text(modified, encoding="utf-8")
                result = run_sensors(snapshot, mutation.tests, mutation.name)
                result["mutant_sha256"] = hashlib.sha256(modified.encode()).hexdigest()
            except (ValueError, SyntaxError) as exc:
                result = {"status": "invalid_mutant", "detail": str(exc)}
            finally:
                target.write_text(original, encoding="utf-8")
            report["mutations"].append(
                {
                    "name": mutation.name,
                    "target": mutation.target,
                    "function": mutation.function,
                    **result,
                }
            )
            print(f"{mutation.name}: {result['status']}", file=sys.stderr)
    return report


def main() -> int:
    """Emit fresh JSON evidence; optionally write it outside the source tree."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.output and args.output.resolve().is_relative_to(ROOT):
        parser.error("store generated observations outside the checkout")
    report = audit()
    rendered = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    invalid = report["baseline"]["status"] != "survived" or any(
        row["status"] in ("runner_error", "invalid_mutant")
        for row in report["mutations"]
    )
    return 1 if invalid else 0


if __name__ == "__main__":
    raise SystemExit(main())
