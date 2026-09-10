# 15 — The streaming projection and the control run

**What to build:** a Structured Streaming job producing product-month aggregates **beside**
batch, never replacing it, and a control run proving the two agree. "Beside batch" stops
being a claim and becomes a checked reconciliation.

The shape:

- one streaming job **sharing silver's validation function**, so a divergence between the
  two paths shows up as a reconciliation failure rather than as silent drift;
- a 30-day watermark with `dropDuplicatesWithinWatermark` — exact here because the review
  identity carries the timestamp;
- append-mode product-month aggregates with the frozen decline rule applied per batch.

**The control run must print zero natural drops and zero differing product-months.** That
is what stops a passing demo run from being an artefact of an unsorted file.

**Blocked by:** 02 (gates write artefacts) and 14 (the sorted topic and paced replay).

**Status:** ready-for-agent

- [ ] The streaming job calls silver's validation function, not a copy of it
- [ ] A test asserts the streaming path's validation is identical to silver's
- [ ] 30-day watermark and `dropDuplicatesWithinWatermark` in place
- [ ] Streaming product-month aggregates reconciled against the batch gold table
- [ ] The control run prints zero natural drops and zero differing product-months
- [ ] Trip case: an unsorted input must make natural drops non-zero
- [ ] Trip case: a diverged validation function must break reconciliation
- [ ] The run registered a run contract
