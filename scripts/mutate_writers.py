#!/usr/bin/env python3
"""Targeted mutation audit of writer bases against relational and algebra sensors.

Complements mutate_flattening.py by probing writer-level code paths:
schema inference, null handling, list serialization, consolidation.

A killed mutant is sensitivity evidence at that location. A survivor is
classified, not scored. This script does not gate merges.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src" / "transmog" / "writers"
REPORT = ROOT / "tests" / "mutation_report_writers.md"
PYTEST = ROOT / ".venv" / "bin" / "pytest"

# Run relational and algebra sensors — both exercise writer code paths.
TEST_PATHS = [
    "tests/relations/test_differential.py",
    "tests/algebra/test_flattening.py",
]

# Each mutant: (name, description, target_file, needle, replacement)
MUTANTS: list[tuple[str, str, str, str, str]] = [
    (
        "arrow_infer_all_string",
        "Schema inference always returns string type, ignoring bool/int/float.",
        "arrow_base.py",
        """            elif isinstance(value, bool):
                pa_type = pa.bool_()
            elif isinstance(value, int):
                pa_type = pa.int64()
            elif isinstance(value, float):
                pa_type = pa.float64()
            else:
                pa_type = pa.string()""",
        """            elif isinstance(value, bool):
                pa_type = pa.string()  # MUTANT: all string
            elif isinstance(value, int):
                pa_type = pa.string()  # MUTANT: all string
            elif isinstance(value, float):
                pa_type = pa.string()  # MUTANT: all string
            else:
                pa_type = pa.string()""",
    ),
    (
        "arrow_nan_not_normalized",
        "NaN/Inf values used for type inference instead of being skipped.",
        "arrow_base.py",
        """    if not isinstance(value, float):
        return False
    return not (math.isnan(value) or math.isinf(value))""",
        """    if not isinstance(value, float):
        return False
    return True  # MUTANT: NaN/Inf treated as valid for inference""",
    ),
    (
        "csv_no_nan_normalize",
        "CSV writer does not normalize NaN/Inf to empty string.",
        "csv.py",
        """    return {
        key: _normalize_special_floats(value, null_replacement="")
        for key, value in record.items()
    }""",
        """    return dict(record.items())  # MUTANT: skip NaN normalization""",
    ),
    (
        "csv_consolidate_no_fill",
        "CSV consolidation does not fill missing sparse fields with empty string.",
        "csv.py",
        """                    for row in reader:
                        writer.writerow({f: row.get(f, "") for f in unified_fields})""",
        """                    for row in reader:
                        writer.writerow({f: row.get(f) for f in unified_fields})  # MUTANT""",
    ),
    (
        "avro_schema_always_string",
        "Avro schema inference maps all values to string type.",
        "avro.py",
        """    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "long"
    if isinstance(value, float):
        return "double"
    if isinstance(value, bytes):
        return "bytes"
    return "string\"""",
        """    if isinstance(value, bool):
        return "string"  # MUTANT
    if isinstance(value, int):
        return "string"  # MUTANT
    if isinstance(value, float):
        return "string"  # MUTANT
    if isinstance(value, bytes):
        return "bytes"
    return "string\"""",
    ),
    (
        "base_collect_fields_reverse",
        "Field name collection reverses insertion order.",
        "base.py",
        """    return list(seen)""",
        """    return list(reversed(seen))  # MUTANT: reversed field order""",
    ),
    (
        "base_sanitize_no_collapse",
        "Filename sanitization does not collapse repeated underscores.",
        "base.py",
        """    sanitized = re.sub(r"_{2,}", "_", sanitized)""",
        """    pass  # MUTANT: no underscore collapse""",
    ),
    (
        "arrow_consolidate_skip_promote",
        "Consolidation does not promote part tables to unified schema.",
        "arrow_base.py",
        """            table = self._promote_table_to_schema(table, schema)
            self._write_to_format_writer(writer, table)""",
        """            self._write_to_format_writer(writer, table)  # MUTANT: skip promote""",
    ),
]


def _clear_pycache() -> None:
    for cache in SRC_DIR.rglob("__pycache__"):
        shutil.rmtree(cache)


def _run_sensors() -> subprocess.CompletedProcess[str]:
    _clear_pycache()
    cmd = [str(PYTEST), *TEST_PATHS, "-q", "--tb=line", "-p", "no:cacheprovider"]
    return subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True)


def main() -> int:
    originals: dict[str, str] = {}
    for py_file in SRC_DIR.glob("*.py"):
        originals[py_file.name] = py_file.read_text(encoding="utf-8")

    baseline = _run_sensors()
    if baseline.returncode != 0:
        sys.stderr.write("Baseline sensors failed; aborting mutation.\n")
        sys.stderr.write(baseline.stdout)
        sys.stderr.write(baseline.stderr)
        return 1

    results: list[dict[str, str]] = []
    try:
        for name, description, target_file, needle, replacement in MUTANTS:
            source = originals.get(target_file)
            if source is None:
                results.append(
                    {
                        "name": name,
                        "description": description,
                        "target": target_file,
                        "status": "apply-failed",
                        "note": f"{target_file} not found",
                    }
                )
                continue
            if needle not in source:
                results.append(
                    {
                        "name": name,
                        "description": description,
                        "target": target_file,
                        "status": "apply-failed",
                        "note": "needle not found",
                    }
                )
                continue
            if source.count(needle) != 1:
                results.append(
                    {
                        "name": name,
                        "description": description,
                        "target": target_file,
                        "status": "apply-failed",
                        "note": f"needle matched {source.count(needle)} times",
                    }
                )
                continue

            target_path = SRC_DIR / target_file
            target_path.write_text(
                source.replace(needle, replacement, 1), encoding="utf-8"
            )
            proc = _run_sensors()
            target_path.write_text(source, encoding="utf-8")
            _clear_pycache()

            killed = proc.returncode != 0
            failing = [
                line
                for line in (proc.stdout + proc.stderr).splitlines()
                if "FAILED" in line or "error" in line.lower()
            ][:8]
            results.append(
                {
                    "name": name,
                    "description": description,
                    "target": target_file,
                    "status": "killed" if killed else "survived",
                    "note": "\n".join(failing)
                    if failing
                    else (proc.stdout[-500:] if proc.stdout else ""),
                }
            )
            print(f"{name} ({target_file}): {'KILLED' if killed else 'SURVIVED'}")
    finally:
        for filename, content in originals.items():
            (SRC_DIR / filename).write_text(content, encoding="utf-8")
        _clear_pycache()

    killed_n = sum(1 for r in results if r["status"] == "killed")
    survived = [r for r in results if r["status"] == "survived"]
    lines = [
        "# Mutation report: writer bases",
        "",
        "Targeted fault injection against `tests/relations/` and `tests/algebra/`.",
        "Mutation score is not a quality definition (plan §15.5). Survivors are classified.",
        "",
        f"Baseline sensors: passed. Mutants run: {len(results)}. Killed: {killed_n}.",
        "",
        "| Mutant | Target | Status | What it changes |",
        "| --- | --- | --- | --- |",
    ]
    for row in results:
        lines.append(
            f"| `{row['name']}` | `{row['target']}` | {row['status']} | {row['description']} |"
        )
    lines.extend(["", "## Survivor classification", ""])
    if not survived:
        lines.append(
            "No survivors. Seeded writer faults above were observationally visible."
        )
    else:
        for row in survived:
            lines.extend(
                [
                    f"### `{row['name']}` ({row['target']})",
                    "",
                    row["description"],
                    "",
                    "Classification: unmeasured until inspected. Probe output:",
                    "",
                    "```",
                    row["note"] or "(no output)",
                    "```",
                    "",
                ]
            )
    lines.extend(
        [
            "## Notes",
            "",
            "- Equivalent mutants are possible; a survivor is an investigation, not a pass.",
            "- Streaming consolidate coercion mutants included (arrow_consolidate_skip_promote).",
            "- Do not gate merges on this score.",
            "",
        ]
    )
    REPORT.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
