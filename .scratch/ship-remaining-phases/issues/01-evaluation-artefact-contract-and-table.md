# 01 — The evaluation artefact contract and `make eval-table`

**What to build:** one command that prints the project's whole evaluation table — every
capability with its value, its threshold, its verdict and its run id — assembled from
per-capability artefacts rather than by hand. A capability that has not run appears with
`NOT_RUN` and a written reason. A capability that is missing *and* not declared cut makes
the command fail loudly.

This is the spine. Built first, it makes every later phase gate-ready by construction and
gives a partial submission a truthful shape: a capability is either a number or a written
reason, never silence.

The artefact contract is frozen by ADR-0011 and lives at `conf/eval-artifact.schema.json`.
Required fields: capability, phase, protocol hash, model and prompt identity, population
identity, `pipeline_run_id`, scope (`full` | `sample`), status, the value with its
threshold beside it, and — when the capability did not run — a `cut_reason`. Verdict
vocabulary is closed: `PASS` / `FAIL` / `REPORTED` / `NOT_RUN`.

The declared chain lives in `conf/lineage_chain.toml` and is shared with the lineage gate
(ticket 04). It names every capability the submission claims, and supplies `cut_reason`
for the ones deliberately not run. "Not reached by submission date" is a legitimate,
already-modelled reason.

**Blocked by:** None — can start immediately.

**Status:** done — landed 2026-09-10

- [x] `conf/eval-artifact.schema.json` exists and every required field above is declared
- [x] `conf/lineage_chain.toml` declares every capability P2–P8 plus the deliverables track
- [x] `make eval-table` renders one table: capability, value, threshold, verdict, scope, run id
- [x] Reproducibility verdicts and quality verdicts are visually separated in the output
- [x] A capability missing from `eval/` and not carrying a `cut_reason` makes the command exit non-zero
- [x] A capability declared cut renders `NOT_RUN` with its reason and does **not** fail the command
- [x] An artefact at `scope=sample` renders `NOT_RUN`, never `PASS` — a phase is complete only at `scope=full`
- [x] Schema validity is tested once, at the contract; the renderer's hard-fail has its own test
