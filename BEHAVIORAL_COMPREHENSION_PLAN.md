# Behavioral Comprehension Plan

This document is the working program for understanding transmog as a
**behavioral system**, not as a folder of modules. It is derived from
*Interrogating the Codebase: A Critical Synthesis of Evidence for Behavioral
Comprehension of Existing Software Systems* (critical integrative research
synthesis, September 2026). That paper is cited below as **the paper**.

The paper’s central thesis:

> Whole-codebase behavioral comprehension is best treated as triangulation
> under bounded evidence, not as exhaustive documentation or a single
> testing technique.

This plan exists because that thesis is not the current state of this
repository. The repository has source, Sphinx docs, and a large pytest
suite. Those are instruments. They are not interchangeable descriptions of
truth (paper, Abstract). The goal of this plan is to build the evidence
architecture the paper describes, scoped to transmog, and to stop at an
**explicit boundary** rather than chasing a fake percentage of “behavior
covered” (paper §11.1).

This is not a named methodology invented here. The paper rejects
terminological invention (paper §1). The architecture below is the paper’s
synthesis applied to one library.

---

## 1. What “the stage” is

The paper’s success criterion is not “all behavior known.” It is:

> material uncertainty reduced and explicitly bounded (paper §11).

A defensible statement for transmog, adapted from the paper’s own example
(§11), would look like this:

> Public entry points `flatten`, `flatten_stream`, and `FlattenResult.save`
> were exercised across csv/parquet/orc/avro for a pairwise model of
> `TransmogConfig` and stream options, against a generated corpus of
> specified data-shape classes. In-memory tables were compared to streamed
> decoded files. Writers were compared differentially with format-semantic
> exceptions recorded as observations, not collapsed into “consistent.”
> Mutation of `flattening.py` and the writer bases showed the oracles detect
> selected transformation faults. Concurrency schedules, multi-GB inputs,
> Glue/Hive consumers, and unspecified plugin codecs remain outside the
> current evidence.

Until a statement of that form can be made **with artifacts attached**,
transmog is not at the paper’s stage. Green CI is not that statement.
“606 tests” is not that statement. A prose architecture diagram is not that
statement.

### 1.1 What the stage is not

The paper spends as much time on false targets as on true ones. This plan
refuses the following, with the paper’s reasons:

| False target | Why (paper ref) |
| --- | --- |
| Prose description of every function | Understanding = answering questions, not possessing descriptions (§3) |
| Maximizing line/branch coverage | Weak standalone proxy for fault detection (§8) |
| Tests as definition of correctness | "This happened" ≠ "this should happen" (§4, §15.1) |
| One scalar like "93% covered" | Indefensible unless "behavior" is narrowly formalized (§11.1) |
| Exhaustive enumeration | State space is often undefined as a finite task (§11) |
| Assumption-free bad-behavior discovery | Calling a behavior bad requires a criterion (§5) |
| Hand-maintained concept→file graph | Drifts while appearing authoritative (§9, §15.6) |
| Agent mocks encoding the agent's theory | Self-confirming analysis (§16) |
| Infinite "understand the whole codebase" | Depth must be risk-weighted (§12, §15.8) |

### 1.2 The four objects that must not be collapsed

The paper (§3) warns that “what the code does” confuses four distinct
objects. For transmog they map as follows.

**Mechanism.** Call/data/control flow capable of producing effects:
`get_data_iterator` → `process_record_batch` / `flatten_json` →
`FlattenResult` or `StreamingWriter` part/coerce/consolidate → format
writers. Config reads (`array_mode`, `include_nulls`, `stringify_values`,
`id_generation`, `max_depth`, `batch_size`). Side-effect sites: mkdir,
part files, `_schema_log.json`, logs, `progress_callback`.

**Actuality.** What occurred under a specified `(input, config, entry
point, format, consolidate, coerce_schema, dependency versions)`. Example:
SMART mode + `[1, None, 2]` + `include_nulls=False` produced *this* list
on the parent row, not “arrays were handled.”

**Intent.** Docs, docstrings, issue history, compatibility promises
(single-file-per-table for data lakes; raw values in Parquet/ORC/Avro;
parent-child linkage). Intent is fallible and can be stale (paper Table 1).

**Correctness.** Whether actuality satisfies a spec, compatibility
commitment, or domain judgment. Correctness requires an oracle (paper §5;
Barr et al. 2015). Transmog has no formal spec. Docs are not a spec until
they are tied to observations.

A whole-codebase effort that collapses these four systematically overstates
what it knows (paper §3). This plan keeps them separate in every artifact.

---

## 2. Claim-evidence tuple (working unit)

Every behavioral claim this project makes, in tests, docs, or agent
summaries, is supposed to be writable as the paper’s tuple (§14.1):

```text
C = (P, D, R, O, S, V)
```

| Field | Meaning | Transmog example |
| --- | --- | --- |
| **P** | Proposition | "SMART keeps primitive arrays on parent, extracts object arrays." |
| **D** | Exercised domain | Hand fixtures only vs generated shapes × pairwise config. |
| **R** | Included reality | Real `csv`/`fastavro`/`pyarrow` vs mocked `open`. |
| **O** | Oracle | Exact decoded tables vs `len(tables) > 0`. |
| **S** | Sensitivity | Mutation killed a flattening mutant that dropped nested arrays. Unknown if never measured. |
| **V** | Provenance | Commit, Python, pyarrow/fastavro versions, OS. |

The paper’s uses of the tuple are the review standard for this repo:

- A mocked writer cannot support an unqualified “Parquet round-trip”
  claim: **R** excluded PyArrow.
- A thousand generated records cannot establish correctness without **O**.
- A snapshot can preserve behavior without proving desirability: **P** is
  continuity, not normative correctness.
- A passing suite can be untrustworthy: **S** unknown.

**Evidence proportionality** (paper §14.2): the scope of a conclusion must
not exceed the scope of the evidence. The opposite error is also
forbidden: a tight unit test of `is_null_like(float("nan"))` is extremely
strong for that local proposition and must not be dismissed for not being
end-to-end.

**Observations before verdicts** (paper §14.3): a difference record may say
CSV wrote `""` and Parquet wrote `None` for the same logical null. A
verdict later classifies that as intended format semantics, not a defect.
If docs change, the verdict can change without rewriting the observation.

For agents, **unknown is first-class** (paper §16). If source, docs, and
runtime disagree, the output is an unresolved hypothesis plus a proposed
probe, not a fluent story.

---

## 3. Current evidence state (proportionate)

This section is an observation about the repository at the time this plan
was written, not a verdict that “tests are bad.”

### 3.1 Channels present

| Channel | Present | Blind spot |
| --- | --- | --- |
| Structural | Small surface: api, flattening, writers, config | Source ≠ execution |
| Controlled dynamic | ~600 pytest, real codecs | Small hand fixtures, weak oracles |
| Relational | One metamorphic (batch_size) | No flatten-vs-stream or writer-to-writer |
| History / docs | Sphinx + README | Can encode myths, lag code |
| Real-boundary | Writers hit real codecs | Not Glue/Hive/DuckDB consumers |
| Sensitivity | Absent | Weak oracle survival is inferred |
| Formal / semantic | Types, config `__post_init__`. | No spec of the flattening algebra. |

Mocks that exist inject `OSError` / `MemoryError`. That is a legitimate
evidence boundary (paper §6; Spadini et al. 2017): they ask whether errors
are wrapped, not whether CSV is CSV. They do not substitute away the
claim under test. That part of the suite is already closer to the paper
than the flattening tests.

### 3.2 Why volume is not comprehension

The paper’s cartoon of a weak oracle is `result == result` and the
end-to-end test that only asserts HTTP 200 (paper §8.1). The local
equivalents are:

- `assert isinstance(result, dict)` after `flatten_json`
- `assert len(result.tables) >= 1` on mixed-type arrays
- `assert len(paths) >= 1` under the name “consistent across formats”

Those tests **execute** the interesting path and then check a predicate
that almost any implementation would satisfy. Coverage would mark
`flatten_json` executed. The observation apparatus would not notice if
SMART and SEPARATE swapped, if mixed arrays lost primitives, or if nested
`skills` arrays vanished.

That is the paper’s second-order question (§8): *would the tests care if
the relevant behavior were wrong?* For stringify, ID strategies, iterator
routing, NaN/Inf scalars, part-file naming, and coerce_schema, often yes.
For the flattening algebra — the product — often no.

### 3.3 Starting unknown ledger

These are unknowns in the paper’s sense: not “untested files,” but
propositions without adequate `(D, O)` or with `S` unmeasured.

1. Exact parent/child tables for SMART/SEPARATE/INLINE/SKIP on mixed
   arrays (dict + primitive + `None` + nested list).
2. Whether SMART simple arrays retain null-like items when
   `include_nulls=False`.
3. Observational equality of `flatten` then `save` vs `flatten_stream`
   for the same `(data, config, format)`.
4. Writer decoded-record equality except documented format semantics
   (CSV strings vs Parquet types vs Avro unions vs ORC).
5. `max_depth` truncation **contents** (which keys remain), not merely
   “fewer keys.”
6. Natural-ID child rows when `id_field` is absent (behavior is being
   pinned in code; characterization must follow the code, not the old
   docs).
7. Sensitivity of current asserts to mutants in `flattening.py`.
8. Pairwise interactions among `array_mode × include_nulls ×
   stringify_values × id_generation × format × consolidate`.
9. ijson streaming vs full JSON load observational equality on large
   arrays.
10. Concurrent `save` / multi-process stream to one directory.

Item 10 may stay out of scope (paper §12: allocate attention). It belongs
on the ledger so it is not silently treated as known.

---

## 4. Evidence architecture to build

The paper (§14) describes complementary channels, not a waterfall. For a
library this size, the implementation must stay small (paper §15.6, §15.7):
reproducible relations, not a graph database; persist expensive findings,
regenerate cheap ones.

### 4.1 Artifacts

One tree: `tests/` is the measurement apparatus (paper §10, §15.6, §17).
There is no parallel `evidence/` product. Cheap **O** is regenerated by
running the sensors. Human text is only intent, verdicts, and the unknown
ledger.

| Artifact | Role | Paper mapping |
| --- | --- | --- |
| This plan | Intent for the investigation | Intent; revisable |
| `tests/README.md` | Map of the apparatus, protocol, bounded statement | §11, §16 |
| `tests/README.md` "Not claimed" | Unclaimed propositions named as intent | §11 bounded uncertainty |
| `tests/corpus/` | Checked-in inputs (**I**) | Characterization corpus, §10 |
| `tests/algebra/` | Exact-table sensors for the flattening product | Controlled dynamic, §4 |
| `tests/relations/` | Differential and metamorphic sensors | Relational, §7.2–7.3 |
| `tests/combinations/` | t=2 covering of modeled config × shape | Combinatorial, §7.4 |
| `tests/unit/` | Local propositions (strong where **O** is tight) | Evidence proportionality, §14.2 |
| `tests/integration/` | Real codecs and workflows | Real-boundary, §6 |
| test assertions | Observations and classifications live on the sensors | §14.3 |
| `tests/mutation_report.md` | Did algebra oracles notice seeded faults? | Sensitivity, §8 |
| `tests/covering.py` | Generated covering array (not stored) | §15.7 regenerate cheap facts |

Claims (**P**, **D**, **R**, **O**) live on the sensors, not in a wiki.
Do **not** hand-author a call-graph wiki. If a graph is needed, generate it
from source or traces (paper §9).

### 4.2 Provenance formula

Every stored observation should be reconstructible as (paper §4):

> Under program version **V**, entry point **E**, input **I**, pre-state
> **S**, configuration **C**, dependency/environment **D**, execution
> conditions **T**, the observable result was **O**.

For transmog, a concrete encoding:

```text
V  = git SHA + transmog.__version__
E  = flatten | flatten_stream | FlattenResult.save
I  = corpus id (not an inline novel each time)
S  = empty (pure transform; no durable pre-state)
C  = TransmogConfig fields + consolidate + coerce_schema + format + format_options
D  = pyarrow/fastavro/orjson/ijson versions, Python version
T  = batch_size as processing condition (metamorphic: should not affect payload)
O  = decoded tables (main + named children), file names, warnings, exceptions
```

Omit a field only when it is truly irrelevant to **P**. Omitting **C** is
how contingent results get mistaken for unconditional properties.

### 4.3 Oracle classes (assumption-diverse, not assumption-free)

The paper (§5, §7.7) wants diversified assumptions, not a single generator.

**Characterization oracles.** Exact tables for a frozen corpus. These are
continuity sensors. Their epistemic status is “this is what happened,”
never “this is what should happen,” until a verdict says so. Changing a
golden file is a reviewed verdict, not a chore (paper §10, §15.1).

**Metamorphic oracles** (Chen et al. 2018; paper §7.3). Relations that must
hold without knowing the exact table:

- `batch_size` does not change non-metadata fields (`flatten` already has
  this; extend to `flatten_stream` payload).
- Hash IDs are stable across runs and across `flatten` vs `flatten_stream`.
- `stringify_values` then identity: already-strings unchanged; null-like
  stay `None`.
- INLINE output is `json.dumps` of the original array (including types).

**Differential oracles** (McKeeman 1998; Yang et al. 2011; paper §7.2).
Natural in this codebase because there are multiple allegedly comparable
paths:

- `flatten` + `save(format=F)` vs `flatten_stream(..., output_format=F)`
- csv vs parquet vs orc vs avro on the **logical** record, with an
  explicit exception list (CSV null → `""`, typed formats → `None`;
  numbers become strings in CSV). Disagreement outside the exception list
  is an anomaly, not automatic proof of which side is wrong.

**Combinatorial oracles** (NIST SP 800-142; paper §7.4). `t=2` covering
array over:

`array_mode × include_nulls × stringify_values × id_generation ∈
{random, natural, hash, composite} × format × consolidate`

Do not claim all higher-order interactions are irrelevant. The covering
array reduces a **modeled** space. The model must include mixed arrays and
null-like values as **parameters**, not only as one lucky fixture.

**Property-based oracles** (Claessen & Hughes 2000; paper §7.1) later, once
characterization has named the properties. Candidate properties: round-trip
of scalar maps through flatten (no arrays); SKIP removes all array keys;
parent `_id` equals child `_parent_id` under random/hash/natural.

**Mutation** (Just et al. 2014; paper §8) on `src/transmog/flattening.py`
first, then writer bases. A killed mutant is sensitivity evidence at that
location. A survivor is an investigation: equivalent, outside the claim,
observationally invisible, or missing oracle. Do not treat mutation score
as the definition of quality (paper §15.5).

**Fuzzing / symbolic** (paper §7.5–7.6) are optional later. Fuzzing without
a semantic oracle will treat silent field drops as success. Do not add a
fuzzer until characterization oracles exist for the flattening algebra.

---

## 5. Material behaviors and risk-weighted attention

Exhaustive comprehension is infeasible (paper §11). Attention follows
change risk, criticality, and blast radius (paper §12), not file length.

**Highest expected return** (product + recent churn on `v2.1.0`):

1. `flatten_json` / `_process_array_items` / `array_mode` — the algebra.
2. Writer encode/decode and streaming part/coerce/consolidate.
3. `flatten` vs `flatten_stream` observational relationship.
4. Config interactions that change **O**.

**Already closer to adequate sensors** (do not drown these in clones):

- `stringify_values` exact encodings
- ID strategies (random/hash/natural/composite) at the
  `generate_transmog_id` grain
- Iterator routing and JSON5/HJSON dialect differences
- NaN/Inf/`""`/`None` as scalar null-like
- coerce_schema / consolidate file layout

**Lower priority / likely remain on the unknown ledger:**

- Concurrent writers
- Multi-GB memory bounds
- Provider-specific Avro codecs not in the default install
- Spreadsheet consumers, database `COPY` quirks beyond CSV-as-text

Rarely changed but high-consequence code still deserves sensors (paper
§12). `is_null_like` is small and already well pinned; keep it that way.

---

## 6. Phased experimental loop

The paper’s durable engineering contribution, via Feathers, is the short
loop (paper §14.5): identify a boundary, establish observability, make a
small change, rerun evidence, refactor inside the observed region. Large
simultaneous changes create attribution ambiguity.

Phases are ordered by **causal distance** and **current oracle poverty**,
not by ceremonial completeness. Later phases may start probes early if they
are cheap. They must not be declared done without the artifacts in §4.1.

### Phase 0 — Scaffolding (no new “coverage” theater)

**P:** The investigation can store claims, observations, verdicts, and
unknowns without mixing them.

Work:

- Put the apparatus in `tests/`: corpus, sensors, verdicts, unknown ledger.
- Seed unknowns from §3.3 into `tests/README.md` "Not claimed" section.
- Write three claims in tuple form for the flattening algebra (SMART
  simple arrays, SMART object arrays, SEPARATE primitives) **on the
  sensors**, without filling **O** until Phase 1 produces it.
- `tests/README.md`: observations are not verdicts; golden updates need
  a verdict note.

**Stop when:** a new pytest cannot be added without naming the **P** it
is supposed to support. That rule is process, not a linter.

**Out of scope:** rewriting all existing tests. Weak tests stay until
replaced by stronger sensors, so attribution of failures remains possible.

### Phase 1 — Flattening algebra characterization

This is the paper’s “establish observability” step for the actual product.

**P (examples to pin with exact tables):**

1. SMART + primitive array → parent field is the list; no child table.
2. SMART + object array → child `{entity}_{field}`; parent has no array
   column; `_parent_id` matches parent id.
3. SMART + mixed `[dict, str, int, None, list]` → **exact** layout
   (whatever the code does), recorded as observation first.
4. SEPARATE + primitive array → child rows `{value: item}`.
5. INLINE → `json.dumps` of the original array, `ensure_ascii=False`.
6. SKIP → array keys absent.
7. Empty dict/list omitted; `include_nulls` True/False on scalars.
8. Nested path join with `_`; collision `user_name` vs `user.name`.
9. `max_depth` omits the subtree and emits `UserWarning`.
10. Natural IDs: parent missing `id_field` raises; child missing
    `id_field` gets generated `_id` and does not invent `id_field`.

**D:** A checked-in corpus of JSON fixtures, one fixture per claim, not
ad-hoc dicts copied among tests.

**O:** Exact `main` and `tables` with metadata fields stripped **or**
explicitly compared. No `len > 0`. No `any("employees" in name)`.

**R:** In-memory `flatten` first (no writer). Writers are Phase 2 so a
failure can be attributed.

**S:** After oracles exist, run mutation on `flattening.py`. If mixed-array
mutants survive, the oracle is still weak.

**Stop when:** mixed arrays, all four `ArrayMode`s, null-like-in-array, and
`max_depth` have exact observations; mutation on `_process_array_items`
kills “drop nested arrays” and “ignore array_mode” classes, or survivors
are classified.

### Phase 2 — Relational oracles (paths that should agree)

**P:** For a fixed `(data, config)`, `flatten`+`save` and `flatten_stream`
decode to the same logical tables modulo documented format semantics and
nondeterministic `random` IDs / timestamps.

**O:** Differential. Hash IDs make rows comparable. Random IDs: compare
after dropping id/timestamp columns and matching children by payload.

**Also P:** Four writers produce the same *logical* values subject to an
**exception observation file** (CSV stringification, null representation,
Avro unions). Disagreement outside exceptions is an anomaly for judgment,
not a silent pass.

Replace the existing “all formats” test that only checks path counts. Do
not delete it until the differential test exists (attribution).

**Stop when:** one corpus file runs through flatten/stream × four formats
and the exception list is explicit. Unknown ledger item 3–4 can be closed
or narrowed.

### Phase 3 — Combinatorial config

Generate a `t=2` covering array over the parameter model in §4.3 plus
data-shape class (scalar nest, simple array, object array, mixed array,
sparse records). Each cell: run Phase 1 oracles that apply; skip cells
that the claim marks N/A (e.g. natural ID without `id_field` on parent is
an expected `ValidationError` — that is still an observation).

The paper’s qualification stands: this does not prove higher-order
interactions are irrelevant. It makes the modeled space inspectable.

**Stop when:** every pair of modeled parameters appears in at least one
executed cell, and failures name the cell.

### Phase 4 — Persistence and generated structure

- Characterization corpus is the organizational memory (paper §10).
- Snapshot updates require a reviewed change to the pinned assertion.
- Optional: generated config-use map (which functions read which
  `TransmogConfig` fields) as a build artifact, not a wiki.
- Do not store traces that are cheaper to regenerate than to review
  (paper §15.7).

### Phase 5 — Sensitivity as a recurring audit

Mutation or targeted fault injection on:

- `flatten_json` array branches
- `is_null_like`
- streaming `consolidate` / `coerce_schema`
- writer schema inference (Avro float vs int, NaN → null)

Fault injection already exists for disk-full. Keep that; it is the right
**R** for “OSError becomes OutputError.”

**Stop when:** a written mutation report exists for `flattening.py` and
survivors are classified. Do not gate merges on a score.

### Phase 6 — Agent protocol (this repository is agent-maintained)

Paper §16 is not optional here. Rules for future sessions:

1. Do not infer behavior from source or docs alone and then write tests
   that assert the inference. Run the probe; store **O**; then write the
   sensor.
2. Do not mock writers to “prove” formats.
3. If source, docs, and runtime disagree, emit **unknown** + a probe.
   Timestamp format was this failure mode.
4. Enumeration (covering arrays, corpus replay, mutation) is machine
   work. Verdicts (defect vs intended vs compatibility) are labeled as
   verdicts.
5. Do not add `len > 0` tests on flattening paths. If a test cannot name
   **P** and **O**, it does not land.
6. Prefer extending `tests/algebra/` over cloning
   `test_extract_arrays_*`.

---

## 7. Evidence dimensions (indexes into uncertainty)

The paper rehabilitates coverage as **indexes**, not certificates
(§11.1). Unclaimed propositions are named in `tests/README.md`.
Mechanically checkable gaps are verified by `scripts/audit_corpus.py`.
None is a quality score.

| Dimension | Question it answers |
| --- | --- |
| Structural | Which functions in `flattening.py` / `writers/` have no execution evidence? |
| Config | Which pairwise parameter cells have not been run? |
| Feature | Which capabilities lack traces (array modes, formats, ID modes)? |
| Shape | Which data-shape classes lack exact tables? |
| Relational | Which comparable paths lack a differential run? |
| Mutation | Which seeded faults in the algebra survived? |
| Environment | CPython versions declared in `pyproject.toml`; not every OS. |

When reporting status, list dimensions and their holes. Do not average
them.

---

## 8. Testability note

The paper treats unexpected testing difficulty as architectural evidence,
not as a moral failing (paper §13). Transmog is **easy** to observe:
pure-ish transforms, files, no network, no clock in the algebra except
`time_field`. Weak oracles are therefore not forced by design. They are
a measurement-apparatus problem. The remedy is better **O**, not
dependency injection theater.

Seams that already exist and should be used as seams: `TransmogConfig`,
writer factory, `get_data_iterator(streaming=...)`. Do not add interfaces
solely to mock them (paper §13).

---

## 9. What this plan refuses while executing

From the paper’s critical evaluation (§15), applied:

- **Do not** silently treat characterization goldens as specification.
  Update = verdict.
- **Do not** claim production or data-lake behavior from tmp-directory
  tests. Those tests are strong for “this library wrote this file.”
- **Do not** refactor production to make mixed arrays “easier to test”
  before the current actuality is recorded. Observe, then judge, then
  change (paper §4, §14.5).
- **Do not** build a knowledge graph of prose.
- **Do not** persist every generated pairwise output if it regenerates
  in seconds.
- **Do not** turn this plan into an infinite backlog. Phase 1–2 are the
  material gap. Phases 3–5 deepen. Phase 6 constrains agents. Stopping
  is allowed when the north-star statement in §1 can be uttered with
  honest holes named.

---

## 10. Immediate work that is *not* this architecture

Code defects and API footguns are separate from comprehension
infrastructure. They should be fixed with short loops and then
**characterized**. This plan must not wait on a perfect suite to allow
those fixes, and those fixes must not be mistaken for completing Phase 1.

When such a fix lands, add or update a Phase 1 sensor for the new
actuality in the same change if the behavior is user-visible. That keeps
causal distance short (paper §14.5).

---

## 11. References (paper and the works it rests on for this plan)

Primary: *Interrogating the Codebase: A Critical Synthesis of Evidence for
Behavioral Comprehension of Existing Software Systems* (September 2026),
especially Abstract; §3–11; §14–16.

Works the paper uses that this plan actually relies on (not decorative):

- Barr et al. (2015), test oracle problem
- Feathers (2002, 2021), characterization and change detectors
- Chen et al. (2018), metamorphic testing
- McKeeman (1998); Yang et al. (2011), differential testing
- Kuhn, Kacker, Lei (2010), NIST SP 800-142 combinatorial testing
- Just et al. (2014), mutation vs real faults
- Inozemtseva & Holmes (2014), coverage vs effectiveness
- Spadini et al. (2017), mocking practices
- Sillito, Murphy, De Volder (2006), questions programmers ask
- Claessen & Hughes (2000), property-based testing

---

## 12. Status

Bounded statement and protocol: `tests/README.md`. Unknown ledger:
Claims live on the sensors.

| Phase | Status |
| --- | --- |
| 0 Scaffolding | Done |
| 1 Flattening algebra | Done — `tests/algebra/` |
| 2 Relational oracles | Done — `tests/relations/` |
| 3 Combinatorial config | Done — t=2 and t=3 covering |
| 4 Persistence | Done — cheap O regenerated |
| 5 Mutation audit | Done — flattening 8/8, writers 4/8 |
| 5b Property-based | Done — `tests/properties/` |
| 6 Agent protocol | `tests/README.md` |

Unclaimed propositions are named in `tests/README.md`.
Mechanically checkable corpus gaps are verified by `scripts/audit_corpus.py`.
