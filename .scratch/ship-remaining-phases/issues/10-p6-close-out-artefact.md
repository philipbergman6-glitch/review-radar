# 10 — P6's close-out artefact

**What to build:** P6 rendered into the evaluation table as a finished, honest phase —
`THEMES_GATE` carrying reproducibility only, `THEMES_QUALITY` carrying the macro-F1 verdict
against its 0.70 bar, and the findings that make a missed bar a result rather than a gap.

Four things belong beside the number:

- **The star-only overlap.** If the LLM's interval still overlaps the star-only baseline's,
  that is a result about weak local models on a J-shaped corpus. It goes in the table and
  in design doc §7 as a stated outcome, not a footnote on a failed threshold.
- **The parse-failure census**, so the plumbing cost (~0.14 for v4) is separable from
  genuine disagreement.
- **The published agreement number** from the stratified 50 Philip labels by hand — drawn
  by seeded key beforehand, never re-tuned — reported per-theme and overall with Wilson
  intervals. Ground truth here is machine-made and says so: provenance is
  `agent_reference`, never `human`.
- **Repeat-kappa as `NOT_RUN` with its stated reason.** A deterministic labeller must not
  be given a stability number that means nothing.

**Blocked by:** 01 (the artefact contract) and 09 (the audit pass).

**Status:** done — 2026-09-11. `THEMES_GATE=PASS` (6/6, reproducibility only), `THEMES_QUALITY=FAIL` (0.4583 against 0.70, blocks nothing), `THEMES_AGREEMENT=NOT_RUN` pending Philip's 50, which are drawn and exported.

- [x] `THEMES_GATE` carries reproducibility claims only and its trip cases still trip
- [x] `THEMES_QUALITY` prints its own verdict against the unmoved 0.70 bar and does not block
- [x] P6's evaluation artefact validates against the contract at `scope=full`
- [x] The star-only comparison appears as a named finding with both intervals
- [~] The agreement number is published per-theme and overall with Wilson intervals
- [x] Repeat-kappa renders `NOT_RUN` with its written reason
- [x] `make eval-table` shows P6 complete, whatever the verdict

---

## Resolution (2026-09-11)

### The gate split

`THEMES_GATE` was seven constituents, six reproducibility and one the macro-F1 against
ADR-0003's 0.70 bar. That made a disappointing quality number look like a broken phase, and a
broken phase is something you reopen — which is exactly the act RR-21, ADR-0001 and the audit
seal exist to refuse. The bar moved out.

    THEMES_GATE scope=full constituents_ok=6/6 failed=none kind=reproducibility THEMES_GATE=PASS
    THEMES_QUALITY macro_f1=0.4583 bar=0.7 bootstrap95=[0.3647,0.5223] min_supported_recall=0.4667
                   recall_bar=0.5 supported=9/10 reviews=200 failure_rate=0.135 scope=full
                   blocks=false verdict=FAIL
    THEMES_AGREEMENT n=0 expected=50 scope=full blocks=false verdict=NOT_RUN

`scripts/gate_themes.py` exits **0**. P6 is `built, evaluated, below target` and P7 is not held
up. The decision lives in `src/gates/themes.py`, pure over already-loaded facts; the script
collects facts and does the I/O, the same seam every other gate has had since ticket 02.
`tests/test_themes_close_out.py` flips each of the six constituents alone and asserts the gate
trips, and asserts no `THEMES_SCORE` line survives inside it.

### The finding, and it is not the one the ticket was drafted to expect

The ticket said: *"If the LLM's interval still overlaps the star-only baseline's, that is a
result about weak local models on a J-shaped corpus."* On the holdout it does not.

    THEMES_COMPARISON set=audit llm=0.4583 [0.3647,0.5223] classifier=0.3899 [0.3074,0.4639]
                      star_only=0.2968 [0.2612,0.3493] llm_vs_star=disjoint
                      classifier_vs_star=overlapping development_llm_vs_star=overlapping

Three claims, each printed rather than assembled by hand for a slide:

- **The labeller separates from the star floor.** `[0.3647, 0.5223]` against `[0.2612, 0.3493]`
  — disjoint. `qwen3:8b` misses its bar by a wide margin *and* is distinguishably better than
  predicting themes from a star rating. Both are true and the table says both.
- **Development said otherwise, and development was the smaller sample.** The artefact's note
  states the reversal in those terms: "the holdout reverses that earlier, smaller-sample
  reading rather than confirming it". The development overlap is the weaker prior evidence,
  not a contradiction to explain away.
- **The MLlib arm does not separate.** `[0.3074, 0.4639]` still overlaps the floor, and that is
  the honest reading of ADR-0002's baseline — carried in the same note rather than left out
  because it is the less flattering half.

Non-overlapping 95% intervals are a conservative way to claim separation — stricter than a test
of the difference, not weaker — which is the right direction for a number in a submission.
`gate.disjoint` returns `None`, never `False`, when an interval is missing, and the line then
reads `none`: the absence of evidence never renders as evidence of overlap.

### The representative stratum publishes NOT_RUN, and the artefacts are not touched

    THEMES_SUBSET representative reviews=80 macro_f1=NOT_RUN bootstrap95=withheld supported=0
                  failure_rate=0.0375 reason="no theme reaches min_support in the 80
                  prevalence-representative rows, so macro-F1 is undefined there and the
                  interval beside it in the sealed artefact is an artefact of the resampler,
                  not a result"

The sealed artefacts still carry `"macro_f1": null` beside `[0.444, 0.870]` for the labeller
and `[0.483, 0.800]` for the star floor — the second sitting *above* that system's own overall
score. Nothing was regenerated: their sha256s are in `eval/themes/audit-seal.json`, `git diff`
on `eval/themes/score-audit-*.json` is empty, and `THEMES_SEAL` still prints
`artefacts_changed=0`. Rewriting a scored artefact after the set is opened is the one act the
seal exists to refuse, so the fix is at the reporting layer, which is the only kind available.

For the *next* pass, `src/ai/theme_scoring.system_report` now skips the bootstrap entirely when
the point estimate is `None`, with a test. Ticket 09 called that a change to a future pass; it
is made here as one, and it cannot reach the sealed files.

### The agreement number — the long pole, now unblocked

`make adjudicate-export` is the ADR-0003 *disagreement* worklist and is a different mechanism;
nothing read `audit.adjudication_rows = 50` at all. That is built now, as
`scripts/agreement_subset.py` and four targets.

    AGREEMENT_DRAW sample=audit scope=full rows=50/50 seed=20260907 salt=audit-adjudication
                   enriched_arrived_damaged=3/3 … enriched_wrong_size_or_fit=3/3
                   representative=20/20 shortfall=none
    AGREEMENT_EXPORT sample=audit rows=50 out=eval/themes/blind-agreement-audit.jsonl

Decisions worth naming:

- **Stratified 3 per theme + 20 representative**, not a flat random 50, which would have been
  ~60% enriched by construction and would price the correlated-error risk only where themes are
  dense. Every theme gets a floor of hand-labelled reviews.
- **Its own salt** (`audit-adjudication`), so the 50 are not a deterministic prefix of the
  frame's own ordering.
- **Frozen on first write.** A second `--draw` that produces a different 50 crashes. A subset
  redrawn after anyone has seen a label is a subset chosen for its answer.
- **The unit is one (review, theme) decision** — 50 × 10 = 500 — and **kappa is published
  beside the raw rate**, because theme presence is rare and raw agreement is inflated by easy
  shared negatives. Kappa is `none`, never `0.0`, where it is undefined: perfect agreement
  about nothing is not agreement no better than chance.
- **The denominator is the frozen draw.** `--score` refuses if any of the 50 carries no human
  label, rather than scoring whatever happened to be covered.
- `label_source="human"`, its own job `theme_labels_human` with its own run contract and
  migration `0004`, so the two annotators are separable in the ledger afterwards. Sharing a job
  with `theme_labels_reference` would leave an agreement number nobody can attribute.

`docs/theme-taxonomy/ADJUDICATION-50.md` is Philip's worksheet: the ten policy rules verbatim
from the reference sessions (a different policy would measure the distance between two
policies), the files he must not open before finishing, the output object, and the four
commands. `make agreement-check` validates a partial file so the all-or-nothing import is not
the feedback loop.

**Until he labels them, `THEMES_AGREEMENT` publishes `NOT_RUN` with that reason.** The
capability is owed and named rather than silent, and it flips to `REPORTED` on
`make agreement-score` → `make gate-themes` with no further code.

### `make eval-table`

All four P6 rows render. `themes` PASS, `themes_quality` FAIL with `ci=[0.3647, 0.5223]`,
`themes_agreement` NOT_RUN with its reason, `themes_repeat_kappa` NOT_RUN with the
determinism reason already in `conf/lineage_chain.toml`. The remaining `EVAL_TABLE_ERROR`s are
`gold_calibration`, `rag`, `rag_quality`, `stream_control`, `stream_demo` and `demo` — tickets
11–19, untouched here.

### One correction made in passing

`src/ai/wilson.py` returned `[0.722, 0.9999999999999999]` at `p == 1`: an interval that
excludes the rate it is an interval for. Clamped to contain its own point estimate. It shows up
the moment a theme has perfect agreement, which on 50 rows is likely.

### What this surfaced and did not fix — ticket 10a

Publishing P6's artefacts made the lineage gate walk P6 for the first time: 34 links and
`LINEAGE_GATE=PASS` before, 54 links and `FAIL` after. Nothing broke — the gate was passing by
omission while `capability=themes` printed `LINEAGE_PENDING`. The theme-label snapshots carry
no `run_id` in their snapshot summary (`merge_chunk` writes by `MERGE`, which has no `writeTo`
option; the rows carry `run_id` as a column, so provenance is recoverable), and three edges
pin `theme_samples` by `latest_success` when the job legitimately runs once per frame. Both are
written up in **ticket 10a**, with the orphaned `running` gold run from 2026-09-07.

### Verified

    make gate-themes    -> THEMES_GATE=PASS (6/6), THEMES_QUALITY=FAIL, THEMES_AGREEMENT=NOT_RUN, exit 0
    make eval-table     -> all four P6 rows render; errors are P7/P8/deliverables only
    make agreement-draw -> 50/50, no shortfall; re-running is refused unless identical
    make agreement-score-> refuses: "the 50 have not been imported"
    git diff eval/themes/score-audit-*.json eval/themes/audit-seal.json -> empty
    make check          -> 461 passed, ruff clean
