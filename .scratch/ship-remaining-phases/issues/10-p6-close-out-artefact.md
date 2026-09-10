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

**Status:** ready-for-agent

- [ ] `THEMES_GATE` carries reproducibility claims only and its trip cases still trip
- [ ] `THEMES_QUALITY` prints its own verdict against the unmoved 0.70 bar and does not block
- [ ] P6's evaluation artefact validates against the contract at `scope=full`
- [ ] The star-only comparison appears as a named finding with both intervals
- [ ] The agreement number is published per-theme and overall with Wilson intervals
- [ ] Repeat-kappa renders `NOT_RUN` with its written reason
- [ ] `make eval-table` shows P6 complete, whatever the verdict
