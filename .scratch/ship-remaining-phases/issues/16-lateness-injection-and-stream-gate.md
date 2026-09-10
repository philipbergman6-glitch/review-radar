# 16 — Lateness injection, the demo run, and `STREAM_GATE`

**What to build:** the run that measures the watermark rather than asserting it. Lateness
is injected in **known quantities** — a near slice that must be accepted and a far slice
that must be dropped — and the gate passes only if exactly the frozen counts land on each
side, with every differing product-month explained by dropped rows.

Two runs are required for P8 and both must be present: the control run from ticket 15, and
this demo run. A gate that only ever saw the demo run could not tell a working watermark
from an unsorted file.

In the demo, the stream starts early (move 2) and its gate is printed late (move 10), so it
runs in the background rather than costing a pause.

**Blocked by:** 15 — the projection and the passing control run.

**Status:** ready-for-agent

- [ ] Near and far lateness slices injected in counts frozen before the run
- [ ] The near slice is accepted and the far slice dropped, in exactly those counts
- [ ] Every product-month differing from batch is explained by dropped rows
- [ ] `STREAM_GATE` prints its named constituents then a terminal verdict line
- [ ] The gate requires both the control run and the demo run to be present
- [ ] Trip case: a zero watermark must drop the near slice and fail the gate
- [ ] The evaluation artefact validates against the contract at `scope=full`
