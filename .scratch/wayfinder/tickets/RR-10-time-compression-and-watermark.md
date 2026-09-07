---
id: RR-10
title: Replay pacing, time compression and the watermark
type: grilling
status: open
assignee: unassigned
blocked-by: []
blocks: [RR-13, RR-14]
---

## Question

The streaming story is the audit's F4 and the answer to "isn't this just batch with extra
steps?". Today: five Iceberg commits between 13:46:27 and 13:46:31 — the whole dataset drains
in ~5 seconds — and the file is not chronological (6,388 timestamp inversions in the first
10,000 lines; only 20 consecutive same-`asin` pairs). The producer docstring's ordering claim
has already been corrected in the repo.

Decide:

1. **Compression ratio.** The data spans 2000-11-01 → 2023-09-09, with a 2020 peak of 126,753
   reviews. Mapping ~23 years onto a demo window is the choice: how many minutes, and is the
   mapping linear over event time or over record count? Linear over event time makes the
   early years empty and 2020 a flood — which may be exactly the point, or may be a dead
   two minutes on stage. State which, and why.
2. **Producer rate.** What `--rate` follows from the ratio, and does the demo replay the
   whole 701,528 or a window of it? The runbook's live sequence currently replays a 10,000-row
   sample so bronze goes 701,528 → 711,528 — decide whether that step survives.
3. **Watermark delay.** How late is too late, given the replay is sorted by timestamp and
   lateness is therefore *injected* rather than natural. Say where the late events come from:
   a deliberately held-back slice is honest and demonstrable; pretending the sort produced
   them is not.
4. **Streaming silver: replace or sit beside the batch silver?** Two code paths cost
   maintenance and confuse the "which one is real?" question; one path means the earlier
   phases' gate has to be re-provable in streaming form.
5. **Sort mechanics.** A one-off sort of the replay file by timestamp, or sorting at the
   producer? And what this does to the `parent_asin` keyed partitioning that the producer
   currently demonstrates — chronological order across six partitions is not global order on
   the consumer side, and that is a Q&A trap worth pre-answering.

**Gate.** "Late data visibly handled" is not a number. The resolution must name the printed
one: e.g. count of events accepted after their window closed vs count dropped beyond the
watermark, with the expected values for the demo run.

## Input from RR-09 (closed 2026-09-04)

- The replay is a **historical backtest of a frozen monitoring rule**, not a prospective
  experiment; say so on stage.
- The demo payoff is the alert firing at `alert_triggered_at` (when persistence P was
  satisfied), never at the retrospective `condition_started_at`. The streaming gold must
  therefore emit the per-evaluation-point relation incrementally.
- The holdout starts 2020-01-01; the pre-2020 stretch is where the rule was developed, so a
  compression that spends most of the demo window before 2020 wastes the payoff. 2020–2021
  carry 251,650 reviews `[observed, phase0-profile]`; 2023 is censored at September.
- Watermark interacts with "unevaluable": a late review that lands after its product-month
  was evaluated must be handled as a re-evaluation or a documented loss; the printed gate
  should include how many evaluation points changed flag because of late data.

## Input from RR-01 (closed 2026-09-06)

Stream is **P8, conditional**: the cut rule is `RR-12`'s, but Philip's standing input is
"do not rebuild the whole pipeline merely to claim streaming", which bears directly on item 4
(replace vs sit beside the batch silver). The phase's deliverable is a paced replay,
event-time watermark demonstration and late-event *metrics* — the gate must print those.

## Input from RR-02 (closed 2026-09-07)

- Item 4 is settled: **batch silver is authoritative** (bounded run pinned to a bronze
  snapshot, atomic replace). A streaming silver, if P8 survives, is a *separate* demo
  table/path that must reconcile with batch after the bounded replay — same `review_id`,
  same collision classes, same printed identity — and never replaces it.
- Dedupe is history-wide (content-only survivor over the whole snapshot); a streaming path
  cannot reproduce it without holding all state, so its gate is reconciliation, not dedupe.
