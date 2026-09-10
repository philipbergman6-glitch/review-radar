# 14 — The sorted topic and the paced replay

**What to build:** a replay of the review file into its own Kafka topic, sorted once by
event time and paced so the pipeline is watchable — the demo shows a stream, not a
five-second batch. The event-time clock is printed as it advances, so an audience can see
time moving independently of the wall clock.

Two decisions are load-bearing:

- **Sorted once, into its own topic.** Event order becomes a property of the data rather
  than of the replay. The sort job records its input digest in the run ledger.
- **Constant record rate**, not event-time-linear compression. Pacing and slice sizes are
  frozen in configuration **before** any run.

Sample and full never share a topic.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] A sort job produces an event-time-ordered file and records its input digest in the ledger
- [ ] The replay writes to its own topic, separate from the batch topic and from sample
- [ ] Pacing is a constant record rate, read from frozen configuration
- [ ] The event-time clock is printed as it advances during replay
- [ ] Pacing and slice sizes are committed before the first run
- [ ] The replay registered a run contract
