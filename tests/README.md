# Evidence architecture

## Four objects

| Object | Meaning |
| --- | --- |
| **Mechanism** | Implementation structures capable of producing effects. |
| **Actuality** | What occurred under recorded inputs, configuration, and environment. |
| **Intent** | Requirements, compatibility promises, and documentation; these can be stale. |
| **Correctness** | Whether behavior satisfies a justified oracle, not merely a previous output. |

Source explains mechanisms; execution establishes sampled actuality.
Neither alone establishes correctness. Differential disagreement is an
anomaly, not automatic proof of which path is correct.

## Claim-evidence tuple

A sensor should be expressible as `C = (P, D, R, O, S, V)`:
**P**roposition, exercised **D**omain, included **R**eality,
**O**racle, **S**ensitivity evidence, and pro**V**enance.

`@pytest.mark.claim` identifies a proposition; it does not certify a test.
Collection requires a nonempty claim ID and proposition docstring in evidence
channels and for all marked tests. This checks identification, not oracle strength.
The input/generator defines D, the actual dependencies define R, and the
assertions define O. Sensitivity is bounded by the faults actually injected.
Mutation observations include revision, source/test/tool content digest,
interpreter, platform, dependency versions, and failed sensor names.

## Protocol

1. Name the proposition and justify its oracle: a requirement, codec
   constraint, explicit characterization, or independently defined relation.
   Observe existing behavior before intentionally changing it. Do not infer
   correctness from source and then assert that same inference.
2. Include real dependencies when their semantics are material to the claim.
   Fault injection is appropriate for error wrapping, not format fidelity.
3. Preserve legitimate counterexamples. Fix defects at their source; do not
   narrow a fixture/generator or accept new expected values just to pass.
   Changing a characterization pin requires a reviewed compatibility rationale.
4. Compare complete values, table sets, row occurrences, and exact ownership
   when those are the claim. Checking only existence, types, counts, or
   membership in any parent's IDs is insufficient for value/join fidelity.
5. Test writer behavior directly when preprocessing would hide the fault.
   Compare all decoded tables and parts; force multiple parts for schema drift.
6. Keep observations separate from judgments. Unknown and unsupported remain
   explicit. A passing smoke test does not establish a broader guarantee.
7. Regenerate machine facts. Do not maintain status plans, survivor stories,
   duplicated behavioral descriptions, or checked-in mutation reports.

## Running the apparatus

```bash
uv sync --locked --extra dev
uv run pytest tests/ --audit-corpus
uv run python scripts/audit_corpus.py
uv run python scripts/audit_mutations.py --output /tmp/transmog-mutations.json
```

The dev extra includes Hypothesis and dependencies for **every declared Avro
codec**. Missing dependencies and codec failures must fail, not silently remove
cases. Codec sensors assert metadata as well as decoded values.

`--audit-corpus` is for complete-suite runs: it requires every corpus fixture
to be loaded by an executed claim-marked sensor and checks required categories.
Setup-only loads and skipped sensor bodies do not count. Executed loads do not
certify that assertions use every value. The standalone inventory reports type
presence, **not behavioral coverage**.
Subset runs can omit the option.

Mutation runs use disposable snapshots, never modify the checkout, and classify
invalid/unapplied mutations, collection errors, and runner errors separately
from killed/surviving mutants. Survivors require investigation; no score defines
quality or gates merges. Reports are generated outside the checkout.

## Channels

| Location | Evidence |
| --- | --- |
| `corpus/`, `algebra/` | Checked-in inputs and exact characterization tables |
| `relations/` | Differential paths, parser/writer fidelity, metamorphic relations |
| `combinations/`, `covering.py` | Regenerated t=2/t=3 config × shape × batch interactions |
| `properties/` | Generated scalar values, array operations, IDs, and exact hierarchy ownership |
| `unit/` | Local contracts, failure boundaries, and audit-tool integrity |
| `integration/` | Real codecs, metadata, and complete workflows |

The covering model is explicit, not exhaustive. Heterogeneous columns remain
in the model: Arrow codecs infer marked JSON storage automatically when native
representation is impossible. Sensors check both the wire encoding and logical
values; unmarked strings are not normalized. See `docs/outputs.md` for details.

## Not claimed

- Concurrent writers to one directory
- Multi-GB inputs or guaranteed peak memory bounds
- Glue, Hive, Excel, or DuckDB consumer compatibility
- Cross-platform/version guarantees from a single local run; CI's declared
  matrix is not evidence until its executions are observed
- General fidelity for arbitrary Python objects or numbers outside the codecs'
  representable ranges

These are limits on conclusions, not placeholder skipped tests. Known defects
inside an exercised domain require fixes and regression sensors, not relocation
to this section.
