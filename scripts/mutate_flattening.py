#!/usr/bin/env python3
"""Targeted mutation audit of flattening.py against characterization sensors.

A killed mutant is sensitivity evidence at that location. A survivor is
classified, not scored. This script does not gate merges.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_FILE = ROOT / "src" / "transmog" / "flattening.py"
REPORT = ROOT / "tests" / "mutation_report.md"
PYTEST = ROOT / ".venv" / "bin" / "pytest"
TEST_PATH = "tests/algebra/test_flattening.py"

MUTANTS: list[tuple[str, str, str, str]] = [
    (
        "drop_nested_arrays",
        "Do not extend nested item_arrays into child tables.",
        """            if isinstance(item, dict):
                for nested_table_name, nested_records in item_arrays.items():
                    arrays.setdefault(nested_table_name, []).extend(nested_records)
""",
        """            if isinstance(item, dict):
                pass
""",
    ),
    (
        "ignore_array_mode",
        "Force the SMART branch regardless of array_mode.",
        """            if config.array_mode == ArrayMode.SKIP:
                continue
            elif config.array_mode == ArrayMode.INLINE:
                result[current_path] = json.dumps(value, ensure_ascii=False)
            elif config.array_mode == ArrayMode.SMART:
""",
        """            if False:
                continue
            elif False:
                result[current_path] = json.dumps(value, ensure_ascii=False)
            elif True:
""",
    ),
    (
        "is_simple_always_true",
        "Never classify an array as complex (object arrays stay on parent).",
        "            is_simple = False  # Array contains complex objects\n",
        "            is_simple = True  # MUTANT: never complex\n",
    ),
    (
        "is_simple_always_false",
        "Classify every non-empty array as complex (primitive arrays extracted).",
        "    is_simple = True\n",
        "    is_simple = False  # MUTANT: never simple\n",
    ),
    (
        "skip_null_filter_in_arrays",
        "Do not skip null-like items while extracting array children.",
        "        if is_null_like(item) and not config.include_nulls:\n            continue\n",
        "        if False and is_null_like(item) and not config.include_nulls:\n            continue\n",
    ),
    (
        "disable_max_depth",
        "Never truncate on max_depth.",
        "    if _context.current_depth >= config.max_depth:\n",
        "    if False and _context.current_depth >= config.max_depth:\n",
    ),
    (
        "inline_ensure_ascii_true",
        "INLINE json.dumps with ensure_ascii=True.",
        "                result[current_path] = json.dumps(value, ensure_ascii=False)\n",
        "                result[current_path] = json.dumps(value, ensure_ascii=True)\n",
    ),
    (
        "empty_string_not_null_like",
        "is_null_like no longer treats empty string as null-like.",
        '    if value is None or value == "":\n        return True\n',
        "    if value is None:\n        return True\n",
    ),
]


def _clear_pycache() -> None:
    cache = SRC_FILE.parent / "__pycache__"
    if cache.exists():
        shutil.rmtree(cache)


def _run_sensors() -> subprocess.CompletedProcess[str]:
    _clear_pycache()
    return subprocess.run(
        [
            str(PYTEST),
            TEST_PATH,
            "-q",
            "--tb=line",
            "-p",
            "no:cacheprovider",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )


def main() -> int:
    original = SRC_FILE.read_text(encoding="utf-8")
    baseline = _run_sensors()
    if baseline.returncode != 0:
        sys.stderr.write("Baseline characterization tests failed; aborting mutation.\n")
        sys.stderr.write(baseline.stdout)
        sys.stderr.write(baseline.stderr)
        return 1

    results: list[dict[str, str]] = []
    try:
        for name, description, needle, replacement in MUTANTS:
            if needle not in original:
                results.append(
                    {
                        "name": name,
                        "description": description,
                        "status": "apply-failed",
                        "note": "needle not found in flattening.py",
                    }
                )
                continue
            if original.count(needle) != 1:
                results.append(
                    {
                        "name": name,
                        "description": description,
                        "status": "apply-failed",
                        "note": f"needle matched {original.count(needle)} times",
                    }
                )
                continue
            SRC_FILE.write_text(
                original.replace(needle, replacement, 1), encoding="utf-8"
            )
            proc = _run_sensors()
            SRC_FILE.write_text(original, encoding="utf-8")
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
                    "status": "killed" if killed else "survived",
                    "note": "\n".join(failing)
                    if failing
                    else (proc.stdout[-500:] if proc.stdout else ""),
                }
            )
            print(f"{name}: {'KILLED' if killed else 'SURVIVED'}")
    finally:
        SRC_FILE.write_text(original, encoding="utf-8")
        _clear_pycache()

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    killed_n = sum(1 for r in results if r["status"] == "killed")
    survived = [r for r in results if r["status"] == "survived"]
    lines = [
        "# Mutation report: flattening.py",
        "",
        "Targeted fault injection against `tests/algebra/test_flattening.py`.",
        "Mutation score is not a quality definition (plan §15.5). Survivors are classified.",
        "",
        f"Baseline sensors: passed. Mutants run: {len(results)}. Killed: {killed_n}.",
        "",
        "| Mutant | Status | What it changes |",
        "| --- | --- | --- |",
    ]
    for row in results:
        lines.append(f"| `{row['name']}` | {row['status']} | {row['description']} |")
    lines.extend(["", "## Survivor classification", ""])
    if not survived:
        lines.append(
            "No survivors. Seeded algebra faults above were observationally visible."
        )
    else:
        for row in survived:
            lines.extend(
                [
                    f"### `{row['name']}`",
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
            "- Writer-base and streaming consolidate mutants are not in this run.",
            "- Do not gate merges on this score.",
            "",
        ]
    )
    REPORT.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
