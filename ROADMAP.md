# Transmog — Audit Roadmap

Living roadmap generated from codebase audit. Items are
picked off, planned, implemented, and marked done. Use
`/audit` to interact with this document via Claude Code.

**Status key:** `TODO` | `IN_PROGRESS` | `DONE`
**Size key:** `S` = single file | `M` = multi-file |
`L` = many files | `XL` = system-wide

---

## Context

### What Transmog Is

A configurable data flattening library that transforms
nested JSON into flat, tabular formats while preserving
parent-child relationships. Ships with CSV, Parquet, ORC,
and Avro writers, streaming support, and multiple ID
generation strategies.

### Strengths to Preserve

- **Clean public API** — `flatten()` and
  `flatten_stream()` cover all use cases with a single
  `TransmogConfig` dataclass.
- **Writer abstraction** — Base classes
  (`ArrowBaseWriter`, `ArrowBaseStreamingWriter`) share
  logic across Parquet/ORC. Avro and CSV stand alone with
  format-appropriate implementations.
- **Array mode system** — SMART/SEPARATE/INLINE/SKIP
  covers the full spectrum of array handling needs.
- **ID generation strategies** — random, natural, hash,
  and composite key modes handle real-world identity
  requirements.
- **Optional dependency gating** —
  `AVRO_AVAILABLE`, `ORC_AVAILABLE` flags with
  `MissingDependencyError` for clean degradation.

---

## Performance

### PERF-1: Arrow schema inference is two-pass

`DONE` · Size: **M**

`_create_schema()` in `arrow_base.py` scans all records
to determine field types. Then `_records_to_table()`
scans again with nested loops (records x fields) doing
type checks on every iteration. O(n\*m) even when types
are already known.

**Proposed fix:** Combine schema inference and type
conversion into a single pass. Pre-compute field indices
and type converters outside the main loop.

- Progress: Pre-computed converter functions replace per-cell type comparisons in streaming Arrow writer.

### PERF-2: Arrow column buffer allocation per batch

`DONE` · Size: **S**

`_records_to_table()` at line 309 creates a new dict
comprehension `{field.name: [] for field in schema}` for
every batch. Could reuse column lists across batches in
streaming mode.

**Proposed fix:** Allocate column buffers once and clear
between batches instead of reallocating.

- Progress: Column buffers are now allocated once and cleared between batches in streaming Arrow writers.

### PERF-3: JSON files fully parsed into memory

`DONE` · Size: **M**

`iterators.py:129,254,260` — all JSON files are fully
loaded before iteration. Works for reasonable sizes but
problematic for multi-GB files.

**Proposed fix:** Add streaming JSON parser option for
large files. JSONL already streams line-by-line; standard
JSON could use incremental parsing (e.g., `ijson`).

- Progress: Added `get_json_file_iterator_streaming()`
  using ijson for constant-memory parsing of JSON arrays.
  Available via `streaming=True` on `get_data_iterator()`
  and auto-enabled in `stream_process()`. ijson is an
  optional dependency (`pip install transmog[streaming]`).

---

## Writers

### WRT-1: Avro streaming to file-like objects unsupported

`TODO` · Size: **M**

`avro.py:526-544` — multiple batch writes to file-like
object destinations fail because append mode (`a+b`)
cannot be applied to arbitrary file-like objects. Only
file path destinations work.

**Proposed fix:** Buffer records internally and flush to
the file-like object on close, or document the limitation
explicitly and raise a clear error early.

### WRT-2: CSV schema drift has no recovery option

`TODO` · Size: **M**

`csv.py:378-386` — CSV streaming writer raises an error
when new fields appear after the header is emitted. This
is correct but inflexible.

**Proposed fix:** Add configurable drift handling:
`strict` (current, raise error), `warn` (log and skip
new fields), `extend` (rewrite header). Default to
`strict`.

### WRT-3: Broad exception catching in writers

`DONE` · Size: **S**

Multiple writer files (`csv.py:191`, `avro.py:342`,
`arrow_base.py:124`) use `except Exception` blocks that
could mask system exceptions like `MemoryError` or
`KeyboardInterrupt`.

**Proposed fix:** Catch `OSError` and format-specific
exceptions instead of bare `Exception`. Let system
exceptions propagate.

- Progress: Narrowed to specific types in all three writers; fixed latent ConfigurationError bug in CsvWriter.

---

## Testing

### TEST-1: CSV injection prevention coverage

`DONE` · Size: **S**

`_sanitize_csv_value()` in `csv.py:20-53` exists but test
coverage for bypass attempts (leading whitespace, Unicode
formula characters) is minimal.

**Proposed fix:** Add parameterized tests for known CSV
injection patterns including edge cases.

- Progress: Added 5 parametrized direct unit tests covering
  dangerous chars, Unicode bypasses, whitespace, embedded
  chars, and non-string passthrough.

### TEST-2: Streaming writer exception cleanup

`DONE` · Size: **M**

Limited testing for file handle cleanup when streaming
writers encounter exceptions mid-batch. Could leak file
handles or leave partial files.

**Proposed fix:** Add tests that inject failures during
write and verify resources are cleaned up and partial
files are handled.

- Progress: Added 14 tests across CSV, Arrow, and Avro
  streaming writers covering context manager cleanup,
  no-context-manager leak behavior, schema drift, close
  idempotency, buffer retention, and partial file
  survival.

### TEST-3: Iterator format detection edge cases

`DONE` · Size: **S**

`_detect_string_format()` in `iterators.py:314-336` uses
heuristics (checks 5 lines, counts hits). Could be fooled
by files with many empty lines at start or ambiguous
content.

**Proposed fix:** Add edge case tests for format detection
with unusual inputs.

- Progress: Added 12 edge case tests covering empty/whitespace
  input, boundary hit counts, pretty-printed JSON, blank line
  interleaving, leading whitespace, non-object lines, bytes
  input, and the checked-limit threshold.

---

## Documentation

### DOC-1: Close GitHub issues #12 and #14

`DONE` · Size: **S**

- Progress: Fixed admonition formatting and badge versions.

### DOC-2: Missing Avro format in docs

`DONE` · Size: **S**

- Progress: Added Avro references to api.md, streaming.md,
  and output format descriptions.

### DOC-3: Logging and verbose mode undocumented

`TODO` · Size: **S** · Depends on: ENH-1

No documentation for debugging or monitoring processing.
Blocked until logging is implemented.

---

## Enhancements

### ENH-1: Add logging support

`TODO` · Size: **M**

No logging or verbose mode. Users have no visibility into
processing progress, type inference decisions, schema
drift events, or batch statistics.

**Proposed fix:** Add `logging` module integration with a
`transmog` logger. No output by default; users opt in via
standard `logging.getLogger("transmog").setLevel(...)`.

### ENH-2: Progress callbacks

`TODO` · Size: **S**

No way to track progress on large jobs. Useful for UI
integration and monitoring.

**Proposed fix:** Add optional
`progress_callback(records_processed, total_records)`
parameter to `flatten()` and `flatten_stream()`.
`total_records` is `None` when unknown (streaming from
iterator).

### ENH-3: Avro zstandard/lz4 codec documentation

`DONE` · Size: **S**

`snappy`, `bzip2`, `xz` work via `cramjam`. `zstandard`
and `lz4` require separate packages but this is not
documented.

**Proposed fix:** Add a note in docs/outputs.md and
api.md about installing `python-zstandard` or `lz4` for
those codecs.

- Progress: Install instructions and explanatory note
  already present in outputs.md. Added cross-reference
  from api.md format_options parameter.
