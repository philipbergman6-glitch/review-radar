---
status: accepted
date: 2026-09-10
---

# Every phase gate prints a number, and only reproducibility blocks the phase

Each phase and each ongoing track terminates in exactly one command that prints a final
`<NAME>_GATE=PASS|FAIL` line. That verdict contains **only reproducibility claims** — counts
that must reconcile, ID sets that must be identical, reruns that must agree, artefacts that
must exist and validate. **Every quality target prints its own `verdict=PASS|FAIL` on its own
line and never blocks the phase**; a missed quality target sets the capability's status to
`built, evaluated, below target` in the evaluation table and is reported as a FAIL there.

The rule exists because the two kinds of failure demand opposite responses. A reconciliation
failure means the numbers on the slide are wrong — the phase is not done, and reopening it is
the only honest move. A quality failure means the numbers are right and the result is
disappointing — reopening the phase would mean tuning against the audit set, which ADR-0001,
ADR-0003 and RR-21 all forbid. Under one mixed verdict the second case silently invites the
first response. Split, `THEMES_GATE=PASS` beside `THEMES_QUALITY macro_f1=0.61 bar=0.70
verdict=FAIL` says exactly what happened and leaves nothing to tune.

The audit's finding F3 is the standing counter-example: the exactly-once gate printed `PASS`
for runs that tested nothing, because it never asserted `0 < partial < records`. **A gate that
cannot fail is not a gate**, so every constituent below carries a case that would trip it.

## The canonical table

Phase order is ADR-fixed by RR-01. `<name>` verdicts use the RR-17 vocabulary:
**PASS · FAIL · REPORTED · NOT_RUN**.

| phase / track | command | blocking line | non-blocking lines | tripping case |
|---|---|---|---|---|
| **P2 Silver** | `make silver`, `make gate-silver`, `make reproduce-silver` | `SILVER_GATE … gate_scope= SILVER_GATE=PASS\|FAIL`; `SILVER_REPRO_GATE=PASS\|FAIL` | `SILVER_REJECT_REASON`×4, `SILVER_COLLISIONS` | an unresolvable group, a dropped catalogue row, a nondeterministic tie-break (RR-02) |
| **P3 Gold** | `make gold`, `make reproduce-gold`, freeze-time calibration | `GOLD_GATE=PASS\|FAIL`; `GOLD_REPRO_GATE=PASS\|FAIL`; `GOLD_CALIBRATION … verdict=PASS\|FAIL` | `GOLD_SPINE`, `GOLD_POINTS`, `GOLD_EPISODES`, `GOLD_ANALYTICAL … verdict=REPORTED` | a spine month missing; pandas and Spark disagreeing beyond the frozen tolerance; a rule whose placebo rate exceeds the ceiling |
| **P4 Search** | `make gate-search` | `SEARCH_GATE … SEARCH_GATE=PASS\|FAIL` | analyzer P@5/MRR@10 by stratum (`REPORTED`) | a `dynamic: strict` mapping violation; an unjudged pooled document |
| **P5 Embeddings** | `make gate-embeddings` | `EMBED_GATE … EMBED_GATE=PASS\|FAIL` | P@5/MRR@10, H-E1..3 verdicts, ANN recall@10, peak memory, latency (`REPORTED`) | a silver cohort id with no vector; a vector from a superseded spec hash |
| **P6 Themes** | `make gate-themes` | `THEMES_GATE … THEMES_GATE=PASS\|FAIL` | `THEMES_QUALITY … verdict=PASS\|FAIL`, `THEMES_BASELINES`, `THEMES_COVERAGE`, `THEMES_BUDGET`, `THEMES_AGREEMENT … verdict=REPORTED`, `THEMES_SENTIMENT`, `THEMES_DISAGREEMENT` | a prompt hash that differs from the frozen one; an audit frame short of its quota; a primary aggregation reading a non-`llm` `label_source` |
| **P7 RAG** | `scripts/gate_rag.py` | `RAG_GATE=PASS\|FAIL` — the 30/30 citation and scope contract only | `RAG_QUALITY grounded= adequate= abstention= false_refusal=` each `verdict=PASS\|FAIL` | one answer citing a review outside its window; a refusal that still carries a claim |
| **P8 Stream** | `scripts/gate_stream.py --run <run_id>` (control **and** demo) | `STREAM_GATE=PASS\|FAIL run_kind=control\|demo` | `STREAM_LATE`, `STREAM_RECON`, `STREAM_ALERTS` | watermark delay 0 drops the near slice; the unsorted file makes `natural_dropped > 0` |
| **Lineage track** | `scripts/gate_lineage.py --mode publication` | `LINEAGE_GATE=PASS\|FAIL … publication_ready= chain_links_checked=N` | development-mode provenance/freshness blocks | a snapshot with no attributing ledger row; `N == 0` |
| **Deliverables track** | `scripts/gate_demo.py` | `DEMO_GATE=PASS\|FAIL rehearsals=N max_elapsed_s= all_cells_ok= stream_running= backup_playable= docs_present=` | per-move fallback usage | one committed export with a failed cell; three of the four documents present |

Two constituents are **universal**: every phase gate prints
`run_contract_registered=true|false` for the jobs that phase adds (ADR-0008), blocking; and
every capability gate writes `eval/<capability>/gate.json`, which must exist and validate.

## Scope

A phase is complete only at `scope=full`. Sample runs print `gate_scope=sample` and render as
`NOT_RUN` in the evaluation table — never PASS. If full scope proves infeasible before the
submission date, the row ships as `REPORTED scope=sample` with the reason stated; that is a
declared deviation, not a pass.

## Never reached, versus cut on the merits

One verdict covers both: `NOT_RUN`. `conf/lineage_chain.toml` carries a per-capability
`status = "cut"` and a **mandatory** `cut_reason`; `"not reached by submission date"` is a
legitimate reason and is written as such. `make eval-table` hard-fails on an artefact that is
missing *and* not declared cut, so a capability is either a number or a written reason —
silence is impossible. Tripping case: delete `eval/rag/gate.json` without touching the config
and `make eval-table` exits non-zero.

## Corrections this ADR makes

- **ADR-0006 path.** RR-17 froze `data/eval/<capability>.json`; the built layout is
  `eval/<capability>/`. The artefact is **`eval/<capability>/gate.json`**, validated against
  `conf/eval-artifact.schema.json` (still to be written). ADR-0006's contract is unchanged.
- **ADR-0003 budget ledger.** The hosted ceiling (12,800 calls / $20) lapsed with RR-19's move
  to local `qwen3:8b`. The constituent is withdrawn and replaced by
  `THEMES_BUDGET labelled_reviews= elapsed_s= model=qwen3:8b api_mode=local`, reported.
- **ADR-0003 repeat kappa.** `NOT_RUN` per RR-21. The published agreement number is
  `THEMES_AGREEMENT` — the stratified Philip-50 against `agent_reference`, per-theme and
  overall Cohen's kappa with Wilson intervals, `verdict=REPORTED`. It prices the
  correlated-error risk; it is not a bar, so it never gates.
- **`label_source` enum.** ADR-0002 wrote `"llm"`, ADR-0003 wrote `"hosted_llm"`, RR-21 added
  `agent_reference`. Canonical set, one enum in `src/ai/labels.py`: **`llm`** (primary labels,
  whatever the host) · **`agent_reference`** (blind ground truth) · **`classifier`** (MLlib
  baseline). The primary theme-shift job asserts `label_source = "llm"`.
- **Repo baseline is not a gate.** RR-13's draft row is dropped. `make check` green on the
  commit is a standing invariant enforced by CI on every push (RR-15), not a phase
  completion check. `run_contract_registered` is the per-phase invariant that replaces it.
- **P6's quality bar leaves PASS/FAIL.** `THEMES_GATE` becomes reproducibility-only; the
  0.70 macro-F1 and 0.50 minimum-recall bars move to `THEMES_QUALITY`. The bars **do not
  move** (RR-21); they simply stop blocking P7.

## Considered options

- **One mixed verdict per phase** — the shape P6 was built with. Rejected: it makes a missed
  quality bar look like a broken phase, and the only available "fix" is tuning against a
  held-out set that three ADRs forbid touching.
- **Case-by-case failure policy per gate script** — rejected because it is not a decision,
  it is the absence of one; the audit's F3 is what that produces.
- **Restating every already-frozen gate line here** — RR-02, RR-06, RR-10, RR-16 and RR-01
  fixed their printed names verbatim. This ADR cites them and freezes only what nothing had
  decided: the failure classes, the scope rule, the RAG and Deliverables lines, and the P6
  corrections above.
- **Making the repo baseline a constituent of every phase gate** — CI already blocks the push;
  a second copy in nine gate lines is noise nothing consumes.
