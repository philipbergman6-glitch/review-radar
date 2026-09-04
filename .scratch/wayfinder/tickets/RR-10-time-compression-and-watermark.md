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
