---
title: Ship the remaining phases — close P6 honestly, build P7/P8, and make the evaluation table real
labels: [ready-for-agent]
created: 2026-09-10
status: open
source: wayfinder map `.scratch/wayfinder/map.md` (closed 2026-09-10, RR-01…RR-23)
---

# Ship the remaining phases

## Problem Statement

Philip has eleven days until the 2026-09-21 submission and a project whose decisions are
all made but whose story cannot yet be told end to end.

Four phases are built with passing gates and P6 Themes is mid-flight. But the pieces that
turn built phases into a defensible submission do not exist: **P6 is measurably below its
bar and has one unused prompt attempt left**, P7 RAG and P8 Stream have frozen protocols
and no code, and — most consequentially — **there is no mechanism that assembles
per-capability results into the single table the design doc, the slides and the README are
all specified to read from.**

Three concrete failure modes follow from that gap:

1. **The submission looks like it hides things.** ADR-0011 says every phase appears with a
   verdict, and unreached phases print `NOT_RUN` plus a written `cut_reason`. Nothing
   currently renders that. If P8 is not reached and nothing says so in the project's own
   vocabulary, the omission reads as ignorance rather than as a schedule decision.
2. **P6's honest result has nowhere to land.** Development macro-F1 for `label-v4` is
   0.4298 against a bar of 0.70 that was frozen before the number existed and does not
   move. That is a reportable FAIL, not a blocker — but only if something reports it. Worse,
   the score sits inside the interval of a star-only baseline (0.4298 [0.354, 0.484] vs
   0.3656 [0.339, 0.407]), so the more interesting finding is currently invisible.
3. **The talk has a fork it cannot read.** Slide 2 ships two variants and the fork is called
   on `text_characterisable < 3` from the gold analytical line. That number is printed by
   gold, but nothing collects it where the rehearsal can see it.

Underneath all three: the demo is nine or ten live moves in one notebook kernel, and the
notebook does not exist. Every phase built so far is invisible to a grader without it.

## Solution

Finish the pipeline in phase order, and build the **evaluation spine** that makes the result
legible whatever it turns out to be.

**Close P6 honestly.** Spend the last budgeted prompt version on the development set, score
it beside `label-v4` and the star-only baseline, freeze whichever prompt wins, then open the
audit set exactly once for all three systems. Expect the bar to be missed — the 6g diagnosis
puts the ceiling over answered rows at 0.5663 — and report that as a FAIL with the
star-only overlap stated as a finding in its own right, rather than as a footnote on a
failed threshold.

**Build the evaluation spine before the remaining capabilities.** One artefact contract that
every capability gate writes through, and one renderer that turns those artefacts into the
table the doc, the slides and the README share. Built first, it makes P7 and P8 gate-ready
by construction and gives a partial submission a truthful shape: a capability is either a
number or a written reason, never silence.

**Then P7 RAG and P8 Stream**, each behind the gate its ADR already froze, each conditional
in the sense the map fixed — built if reached, `NOT_RUN` with a `cut_reason` if not.

**Then the deliverables**, which are a track rather than a final phase: the demo notebook
that makes every built phase visible, two rehearsals that count, the two-page design doc and
the nine slides.

The organising principle throughout: **the project should be able to describe its own state
accurately at any moment.** That is what makes an eleven-day budget survivable — work stops
when the date arrives, and whatever was reached is already described.

## User Stories

**Closing P6**

1. As Philip the engineer, I want the last budgeted prompt version run on the development
   set, so that ADR-0003's "at most five prompt versions, commit every version and its
   development per-theme table" is true of what shipped rather than of what was planned.
2. As Philip the engineer, I want `label-v5` scored against the same 200 development rows
   as `label-v4`, so that the comparison that selects the frozen prompt is like-for-like.
3. As Philip the engineer, I want the 24-row `label-v5` artefact recognised as a scorer
   fixture rather than a result, so that nobody later reads 0.1208 as an evaluation.
4. As Philip the engineer, I want the winning prompt frozen with its version and freeze
   commit recorded, so that the audit set is opened against something immutable.
5. As Philip the engineer, I want the audit set opened exactly once, for all three systems
   together, so that no system gets a second look at held-out data.
6. As Philip the presenter, I want a macro-F1 below 0.70 to be reported as FAIL without
   reopening P6, so that I never tune a prompt against the audit set.
7. As Philip the presenter, I want the star-only overlap reported as a finding, so that
   "a local 8B model did not separate from predicting themes off the star rating" is a
   result I state rather than a weakness a grader discovers.
8. As Philip the presenter, I want the parse-failure census reported beside the score, so
   that I can distinguish the plumbing cost (~0.14) from genuine disagreement.
9. As Philip the engineer, I want a parse failure to score as an empty prediction on the
   evaluation sets but to drop the row from classifier training, so that a row with no
   label is never confused with a row with no themes.
10. As Philip the engineer, I want the MLlib classifier trained on the pool labelled by the
    frozen prompt, so that ADR-0002's pass rule compares it against a stable teacher.
11. As Philip the engineer, I want the classifier's per-theme thresholds fitted on
    development and applied unchanged to audit, so that it is scored the same way the
    star-only baseline is.
12. As Philip the presenter, I want the published agreement number from the stratified 50,
    so that the correlated-error risk of machine-made ground truth is priced rather than
    caveated.
13. As Philip the engineer, I want repeat-kappa to render `NOT_RUN` with its stated reason,
    so that a deterministic labeller is not given a stability number that means nothing.

**The evaluation spine**

14. As Philip the engineer, I want one artefact contract every capability gate writes
    through, so that the renderer knows one shape rather than five.
15. As Philip the engineer, I want each artefact to carry protocol hash, model and prompt
    identity, population identity and run id, so that a number can always be traced to the
    run that produced it.
16. As Philip the presenter, I want one command that renders every capability into one
    table, so that the design doc, the slides and the README cannot drift from each other.
17. As Philip the presenter, I want the renderer to hard-fail when an artefact is missing
    *and* not declared cut, so that silence is impossible.
18. As Philip the presenter, I want a phase never reached to render `NOT_RUN` with a written
    reason, so that an unfinished project reads as one that knows its own state.
19. As Philip the engineer, I want "not reached by submission date" to be a legitimate,
    already-modelled cut reason, so that running out of time is expressible without
    embarrassment.
20. As Philip the engineer, I want every gate to print whether the jobs it adds registered a
    run contract, so that lineage is checked continuously rather than at the end.
21. As the grader, I want each capability's threshold visible beside its value, so that I
    can see the bar was set before the measurement rather than after it.
22. As the grader, I want reproducibility verdicts and quality verdicts visually separated,
    so that I can tell a broken pipeline from an honest miss.

**P7 RAG**

23. As a category manager, I want answers about a product's decline to cite the reviews they
    rest on, so that I can read the evidence rather than trust the summary.
24. As a category manager, I want the system to refuse when the corpus cannot answer, so
    that I am not given a confident answer built from nothing.
25. As Philip the engineer, I want answerability decided by evidence scan and manual
    validation, never by what the retriever returned, so that the evaluation does not grade
    the retriever twice.
26. As Philip the engineer, I want the 30 questions frozen before any answer is generated,
    so that the question set cannot drift toward what the system happens to do well.
27. As Philip the engineer, I want question slots drawn from the actual decline ranking, so
    that RAG answers concern the real result rather than a convenient demo topic.
28. As Philip the presenter, I want the citation and scope contract to block at 30/30, so
    that a system that cites nothing cannot ship as working.
29. As Philip the presenter, I want the four quality targets to print their own verdicts
    without blocking, so that a below-target RAG still ships as evaluated.
30. As Philip the engineer, I want answer keys to forbid prevalence and direction claims, so
    that a retriever's ranking is never mistaken for a representative sample.
31. As Philip the presenter, I want temporal answers to contrast cited examples only, so that
    the quantitative theme-shift claim stays where it is computed — the gold table.
32. As Philip the engineer, I want a refusal that carries a claim or a citation to count as a
    contract violation, so that hedged non-answers cannot pass as abstentions.
33. As Philip the engineer, I want RAG's own call ledger, so that its cost is attributed to
    it rather than pooled with labelling.

**P8 Stream**

34. As Philip the presenter, I want the replay paced so the pipeline is watchable, so that
    the demo shows a stream rather than a five-second batch.
35. As Philip the engineer, I want the file sorted once into its own topic, so that event
    order is a property of the data rather than of the replay.
36. As Philip the engineer, I want lateness injected in known quantities, so that the
    watermark is measured rather than asserted.
37. As Philip the engineer, I want a control run that must print zero natural drops, so that
    a passing demo run cannot be an artefact of an unsorted file.
38. As Philip the engineer, I want the streaming projection reconciled against batch, so
    that "beside batch" is a checked claim.
39. As Philip the engineer, I want the streaming job to share silver's validation function,
    so that a divergence between the two paths shows up as a reconciliation failure.
40. As Philip the presenter, I want the event-time clock printed as it advances, so that the
    audience can see time moving independently of wall clock.
41. As Philip the presenter, I want the stream started early in the demo and its gate printed
    late, so that it runs in the background rather than costing a pause.

**Lineage**

42. As Philip the presenter, I want a demo move that queries the ledger and time-travels the
    lake, so that provenance is shown rather than described.
43. As Philip the engineer, I want every artefact to carry the run id that produced it, so
    that provenance is a join rather than an assertion.
44. As Philip the engineer, I want the lineage gate to walk a declared chain and print how
    many links it checked, so that a passing verdict over an empty chain is impossible.
45. As Philip the engineer, I want failed runs to keep their partial outputs, so that a
    failure is diagnosable rather than erased.

**Deliverables**

46. As Philip the presenter, I want every built phase visible in one notebook kernel, so
    that the demo exercises the real code path rather than a parallel app.
47. As Philip the presenter, I want a rehearsal to be counted by a gate, so that "it ran
    clean twice" is a number rather than a memory.
48. As Philip the presenter, I want a rehearsal to hold with the stream running in the
    background, so that the rehearsal reflects demo conditions.
49. As Philip the presenter, I want a recorded backup exported from an executed notebook, so
    that a failing stack costs me nothing on the day.
50. As Philip the presenter, I want the two slide-2 variants written in advance and the fork
    called on a printed number at the second rehearsal, so that the decision is not made
    under pressure.
51. As Philip the presenter, I want the design doc's phase table and the evaluation table to
    share one spine, so that the doc cannot contradict the repo.
52. As Philip the presenter, I want the four declines stated in their binding wording, so
    that a schedule cut is never dressed up as a principled one.
53. As Philip the presenter, I want five prepared Q&A answers that each contain a number, so
    that I answer with evidence rather than position.
54. As the grader, I want the README to describe the system that exists, so that what I read
    matches what I can run.

## Implementation Decisions

### The evaluation artefact is the spine, and it is built first

- **One contract, `conf/eval-artifact.schema.json`**, frozen by ADR-0011 and named as P7's
  first deliverable. Every capability gate writes `eval/<capability>/gate.json` against it.
  The path shape is a decision, not incidental: ADR-0006 originally froze
  `data/eval/<capability>.json` and ADR-0011 corrected it to the built layout.
- **Required fields**: capability, phase, protocol hash, model and prompt identity,
  population identity, `pipeline_run_id`, scope (`full` | `sample`), status, and — when the
  capability did not run — a `cut_reason`. Values carry their threshold beside them so the
  renderer never has to know a bar.
- **The verdict vocabulary is closed**: `PASS` / `FAIL` / `REPORTED` / `NOT_RUN`. A gate's
  `PASS|FAIL` carries reproducibility claims only. Quality targets emit their own verdict
  line and never block. This split is the reason the two must not share a field.
- **`make eval-table` renders** every artefact into one table and **hard-fails** when an
  artefact is missing and the capability is not declared cut in `conf/lineage_chain.toml`.
  The same file supplies `cut_reason`, and is shared with the lineage gate.
- **A phase is complete only at `scope=full`.** Sample runs render `NOT_RUN`, never `PASS`.

### Gate scripts follow the established shape

- Each gate prints its named constituent lines, then one terminal
  `<NAME>_GATE=PASS|FAIL` line — the pattern already established by silver, gold, search,
  embeddings and themes.
- **Every constituent carries a case that would trip it.** This is the audit's F3 rule: the
  exactly-once gate once printed PASS for runs that tested nothing. A gate that cannot fail
  is not a gate.
- **Every phase gate prints `run_contract_registered`** for the jobs it adds, and it blocks.
- New gates to build: RAG, stream, demo, lineage, plus a gold reproduction gate mirroring
  silver's.

### P6 close-out

- **Run `label-v5` on the 200-row development set** — the fifth and final version in
  ADR-0003's budget, already written against the measured failure census (repeated theme
  ids, over-length quotes, padding to the three-theme limit).
- **Select and freeze** whichever of v4/v5 scores higher on development, recording version
  and freeze commit. Then **open the audit set once**, for the LLM labeller, the MLlib
  classifier and the star-only baseline together.
- **Expect a miss.** The diagnosis puts the ceiling over answered rows at 0.5663. The bar
  does not move; the result is `THEMES_QUALITY … verdict=FAIL` and P6's status becomes
  `built, evaluated, below target`. **This does not block P7.**
- **Report the baseline overlap as a finding.** If the LLM's interval continues to overlap
  the star-only baseline's, that is a result about weak local models on a J-shaped corpus,
  and it belongs in the evaluation table and design doc §7 as a stated outcome.
- **`label_source` enum is closed**: `llm` · `agent_reference` · `classifier`. The primary
  theme-shift job asserts `llm`. Classifier rows can never be aggregated into primary labels.
- **Parse failures are asymmetric by design**: an empty prediction on the evaluation sets,
  a dropped row in classifier *training*, with the dropped count reported.
- **Ground truth is machine-made and says so.** Provenance is `agent_reference`, never
  `human`; the stratified 50 Philip labels are drawn by seeded key beforehand, never
  re-tuned, and the agreement is published per-theme and overall with Wilson intervals.

### P7 RAG

- Retrieval reuses the P4/P5 hybrid retriever unchanged — the phase adds one prompt and one
  evaluation, nothing more.
- **30 frozen questions**: 20 answerable, 10 unanswerable across three strata. Answerability
  is decided by evidence scan and manual validation, **never** by retriever output.
- Question slots are drawn from the decline ranking, so answers concern the actual result.
- **Structured answer keys forbid prevalence and direction claims.** Temporal questions
  contrast cited examples only; the quantitative theme-shift claim lives in the gold table.
- **`RAG_GATE` is the citation and scope contract only, at 30/30**: every non-refused answer
  cites at least one review from its retrieved set in correct scope, and refusals carry
  neither claims nor citations. **`RAG_QUALITY`** prints grounded ≥ 16/20, adequate ≥ 14/20,
  abstention ≥ 8/10, false refusal ≤ 2/20 — fixed integer denominators, non-blocking.
- Philip is sole judge, with a seven-step disagreement precedence already fixed. RAG carries
  its own call ledger.

### P8 Stream

- A **reconciled projection beside batch**, never a replacement: its own topic, one
  Structured Streaming job sharing silver's validation function, a 30-day watermark,
  `dropDuplicatesWithinWatermark` — exact here because the review identity carries the
  timestamp — append-mode product-month aggregates, and the frozen decline rule applied per
  batch.
- **Constant record rate, not event-time-linear compression**, with the event-time clock
  printed as it advances. Pacing and slice sizes are frozen in configuration before any run.
- **Two runs required.** Control passes on zero natural drops and zero differing
  product-months. Demo passes only if the injected near slice was accepted and the far slice
  dropped, in exactly the frozen counts, with every differing product-month explained by
  dropped rows.
- The sort job records its input digest in the ledger. Sample and full never share a topic.

### Lineage

- `pipeline_runs` is the **run ledger**: one row per execution *attempt* of every job, typed
  core plus JSONB inputs/outputs/counts/params, with a UUID run id generated before
  execution and stamped into every Iceberg snapshot, Elasticsearch document and evaluation
  artefact. Insert `running`, finalize `success|failed`; partial outputs are kept and never
  rolled back.
- The lineage gate walks a **declared chain** from configuration and prints how many links
  it checked, so a vacuous pass is impossible. It runs in development or publication mode.
- The demo move is a ledger query plus a Spark SQL `VERSION AS OF` read of silver's pinned
  bronze snapshot.

### Deliverables

- **One notebook, one kernel**, holding one Spark session, one Elasticsearch client and one
  Postgres connection; cells call serving helpers only, so the demo exercises the pipeline's
  own code path. Kibana carries exactly one move. The health check runs in a real terminal
  first.
- **Ten moves, 4:40 of a 5:00 budget**, with the paced replay started at move 2 and the
  stream gate printed at move 10.
- **A rehearsal is an executed export** with every cell succeeded and cell timestamps
  spanning ≤ 300 s, with the stream running. `DEMO_GATE` counts them; threshold is two.
  `docs_present` is a blocking four-file check.
- **Design doc: eight sections, two pages hard, links not appendices.** Its phase table is
  the evaluation table's spine. **Talk: eight slides plus a title, 9:30 of a 10:00 ceiling**
  (5:00 slides + 4:40 demo + 20 s slack), shaped setup → one continuous demo → payoff.
- **Slide 2 ships two variants**; the fork is called at the second rehearsal on the gold
  analytical line's `text_characterisable < 3`, not on whether P6 finished.
- **The four declines carry binding wording**: Kafka Connect ES sink says *dropped for time*;
  HDFS concedes MinIO is not equivalent; Oozie is owned as an opinion; Sqoop and Pig keep the
  course's own citation.

### Ordering

P6 close-out → evaluation spine → P7 → P8 → deliverables, with the notebook and doc grown
alongside rather than stacked at the end. There is **no cut order and no drop-dead date**:
build in this order every day and ship what is reached, rendering the rest `NOT_RUN` with a
reason.

## Testing Decisions

**What makes a good test here.** Test external behaviour: the verdict a gate reaches from a
given set of artefacts, the reconciliation arithmetic, the contract a document must satisfy.
Do not test how a gate loaded its inputs. The existing suite already works this way and
should be matched, not extended in a new direction.

**The single new seam: the evaluation artefact.** Every capability gate is split so that a
pure verdict function takes already-loaded artefacts and returns its constituents plus the
terminal `PASS|FAIL`, with I/O outside it. This is the one seam P6, P7, P8, lineage and the
renderer all cross, and it means gates are testable with no Spark, Elasticsearch, Kafka or
Ollama running. Schema validity is tested once, at the contract.

**Prior art to follow, named because the shapes already exist:**

- `tests/test_eos_verdict.py` — the closest model: a pure `failures(...)` over counts,
  asserting both that a real failure trips it and that a vacuous run cannot pass.
- `tests/test_theme_scoring.py`, `tests/test_decline_rule.py`, `tests/test_search_fusion.py`
  — pure scoring and rule logic tested directly on constructed inputs.
- `tests/test_canonical.py` — golden vectors pinning an encoding two engines must agree on;
  the model for anything where a Python and a Spark implementation must match.
- `tests/test_runs_contracts.py` — run-contract registration, which every new job extends.
- `tests/test_search_contract.py` — a mapping contract validated as a contract.
- `tests/integration/` (`test_ledger_pg.py`, `test_search_es.py`) — the only place
  service-backed tests belong; `tests/conftest.py` already skips Spark tests without a JDK.

**Modules to be tested:**

- The evaluation artefact schema, and the renderer's hard-fail on missing-and-not-cut.
- Each new gate's verdict function, **each with at least one trip case**: RAG (a refusal
  carrying a citation must violate the contract), stream (watermark zero must drop the near
  slice; an unsorted file must make natural drops non-zero; a diverged validation function
  must break reconciliation), lineage (an empty chain must not pass), demo (one rehearsal
  must not reach the threshold), gold reproduction (a perturbed slice must not reconcile).
- The theme close-out arithmetic: parse-failure asymmetry between evaluation and training,
  classifier threshold fitting on development only, agreement statistics with Wilson
  intervals.
- The streaming path's shared validation function, asserted identical to silver's.

**Explicitly not tested:** prompt output quality — that is what the frozen evaluation sets
measure, and a unit test asserting a model's answer would freeze a number the protocol says
must be earned.

## Out of Scope

- **Anything the map ruled out**: Kafka Connect ES sink, single-node HDFS, GraphFrames/GraphX,
  Oozie/Sqoop/Pig, the 23M-review category, streaming AI as its own build, automated narrative
  generation, NL→ES DSL, and any "which reviews should you not trust" framing. Each has a
  written reason and appears in the design doc as a decline, not a silence.
- **Structured logging and integration-test strategy** — ruled out of scope 2026-09-10. `print`
  remains; the metrics half is answered by the run ledger. Implementation sessions inherit the
  standing bar (a test per layer, a printed gate, a ledger row, secrets from environment only)
  without this spec adding a logging workstream.
- **Re-deciding anything the map settled.** Twenty-three tickets and eleven ADRs are closed
  records. If implementation reveals a decision was wrong, that is a new ADR amending the old
  one, not a silent change.
- **Moving any threshold.** Not the 0.70 macro-F1 bar, not RAG's four integer targets, not the
  rehearsal count.
- **Tuning against held-out data.** The audit set opens once. The protocol freeze happens
  before candidates are selected.
- **Re-extracting the course PDFs.** The coverage doc exists so they need not be reopened.
- **Re-scoring the grade estimate.** The build-plan artifact's grade table is the auditor's
  2026-09-01 figure and is labelled historical.
- **A contingency schedule.** No cut order, no drop-dead dates, no hour budgets.

## Further Notes

**Three rules bind every session from here.** They are the reason the project reads as
rigorous, and each is easier to violate than to notice:

1. **The design doc is written against `conf/lineage_chain.toml`, not against prose.** Its
   phase table *is* the evaluation table's spine.
2. **Thresholds never move** — not for a weaker labeller, not for a missed bar.
3. **A quality miss never reopens a phase.** Only reproducibility failures do. Reopening on
   a quality result is what tuning against a holdout looks like.

**P6 is expected to fail its bar, and that is a planned outcome rather than a problem.** The
project's defensibility rests on the bar having been set before the number existed and not
having moved afterwards. A reported FAIL with a published agreement number and a stated
baseline overlap is a stronger submission than a passing number nobody can audit.

**Two decisions are deliberately still open**, and both belong to the protocol freeze rather
than to this spec: the decline-rule thresholds, and which gold population `product_month`
projects. Setting either before the freeze is the exact failure ADR-0001 exists to prevent.
The freeze must print its calibration verdict, and **a rule that fails calibration must not
be frozen.**

**Conditional means built if reached.** P7 and P8 are not promises. If the date arrives
first, they render `NOT_RUN` with `"not reached by submission date"` — a legitimate,
already-modelled reason — and the submission is honest rather than incomplete.

**Provenance of this spec**: the wayfinder map at `.scratch/wayfinder/map.md`, closed
2026-09-10 with all 23 tickets resolved. Individual decisions live in `.scratch/wayfinder/tickets/`
and `docs/adr/`; this spec collapses them into build order and adds nothing new.
