# 02 — The four built phases write evaluation artefacts

**What to build:** silver, gold, search and embeddings each emit an evaluation artefact
when their gate runs, so the table from ticket 01 shows real numbers for the four phases
already built instead of placeholders. Running the four gates and then `make eval-table`
produces the submission's table with no hand-assembly anywhere.

Each gate is split at the same seam: a pure verdict function takes already-loaded
artefacts and returns its named constituents plus the terminal `<NAME>_GATE=PASS|FAIL`,
with I/O outside it. This is the one seam P6, P7, P8, lineage and the renderer all cross,
and it is what makes gates testable with no Spark, Elasticsearch, Kafka or Ollama running.

Printed output does not change shape — the named constituent lines then one terminal
verdict line stay exactly as they are. This is a prefactor: same behaviour, new seam.

**Blocked by:** 01 — the artefact contract and table.

**Status:** done — landed 2026-09-10

- [x] Each of the four gates exposes a pure verdict function over loaded artefacts
- [x] Each gate writes `eval/<capability>/gate.json` validating against the contract
- [x] Terminal gate lines are byte-identical to what they printed before the split
- [x] Each verdict function has a unit test running with no services up
- [x] `make eval-table` after running all four shows four real rows and no placeholders
- [x] Every artefact carries the `pipeline_run_id` of the run that produced it

## What landed

`src/gates/` is the new pure leaf: `silver.py`, `gold.py`, `search.py`, `embeddings.py`,
importing no pyspark, no elasticsearch, no psycopg. Each gate script now reads facts and
hands them over; the lines and the verdict are formatted there. `src/common/evaluation.py`
gained the writer side — `Verdict`, `repro_verdict`, `build_artifact`, `write_artifact` —
and refuses to put an artefact on disk that does not match the contract.

**Six artefacts, not four.** The scope was widened by one decision (2026-09-10, Philip): the
four terminal gates plus `silver_repro` and `gold_analytical`, both of which already printed
a gate line and neither of which any other ticket in this set owns. Without them
`make eval-table` would have stayed non-zero at submission with nothing scheduled to fix it.

Measured on the full run the same day, every one PASS or REPORTED:

| capability | gate | value / bar | run id |
|---|---|---|---|
| silver | `SILVER_GATE` | 7/7 | `4447727f` |
| silver_repro | `SILVER_REPRO_GATE` | 13/13 | `4447727f` |
| gold | `GOLD_GATE` | 3/3 | `6e1c0d3a` |
| gold_analytical | `GOLD_ANALYTICAL` | 964 development alerts, no bar | `6e1c0d3a` |
| search | `SEARCH_GATE` | 6/6 | `d2e56c46` |
| embeddings | `EMBED_GATE` | 6/6 | `273e0939` |

Byte identity was checked against `HEAD` for the search gate by diffing the two scripts'
live output — identical. The others are pinned by `tests/test_gate_verdicts.py`, which also
trips every constituent of every gate one at a time.

## Three deliberate deviations

1. **`SILVER_RERUN` moved above the terminal line** and became a constituent of
   `SILVER_GATE`. It previously printed *after* the verdict and exited 1 on a mismatch while
   the line above still read `PASS`. The exit code is unchanged; the printed verdict is now
   honest. An absent previous run still prints `n/a` and is counted nowhere — a constituent
   that cannot fail would inflate the count without testing anything (audit F3).
2. **`EMBED_TABLE` and `EMBED_INDEX` now always print.** With no embeddings run they used to
   print nothing at all, which is exactly the silence ticket 01 exists to make impossible.
   They print `embeddings_run=none ok=false` / `ledger_row=none ok=false`.
3. **`GOLD_ANALYTICAL` over an empty population is `NOT_RUN`, not `REPORTED` `0.0000`.**
   "The rule fires on nothing" and "nothing was eligible" are different claims, so the
   artefact carries a written reason instead of a number. `Verdict` enforces this: `NOT_RUN`
   without a reason raises.

## Left open

`gold_calibration` (`GOLD_CALIBRATION`, P3, quality) is declared in
`conf/lineage_chain.toml` and **has no producer anywhere in the repo** — grep finds the name
only in that file. It is not this ticket's to invent and no other ticket owns it. Either a
gate must be written for it or it must be declared cut with a reason; until then
`make eval-table` will keep exiting non-zero on it.
