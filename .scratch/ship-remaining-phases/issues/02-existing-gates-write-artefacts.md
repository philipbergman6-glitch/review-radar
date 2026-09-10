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

**Status:** ready-for-agent

- [ ] Each of the four gates exposes a pure verdict function over loaded artefacts
- [ ] Each gate writes `eval/<capability>/gate.json` validating against the contract
- [ ] Terminal gate lines are byte-identical to what they printed before the split
- [ ] Each verdict function has a unit test running with no services up
- [ ] `make eval-table` after running all four shows four real rows and no placeholders
- [ ] Every artefact carries the `pipeline_run_id` of the run that produced it
