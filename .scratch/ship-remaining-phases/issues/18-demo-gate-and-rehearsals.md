# 18 — `DEMO_GATE`: rehearsals counted, documents checked

**What to build:** "it ran clean twice" turned from a memory into a number. A rehearsal is
an **executed export** — every cell succeeded, cell timestamps spanning ≤ 300 s, with the
stream running in the background so the rehearsal reflects demo conditions. `DEMO_GATE`
counts them and the threshold is two.

Alongside it, `docs_present` is a blocking four-file check.

The second rehearsal also produces the recorded backup: an export from an executed
notebook, so a failing stack on the day costs nothing.

**Blocked by:** 17 — the notebook.

**Status:** ready-for-agent

- [ ] A rehearsal is defined as an executed export and validated as one, not self-reported
- [ ] Cell timestamps must span ≤ 300 s for a rehearsal to count
- [ ] A rehearsal only counts if the stream was running during it
- [ ] `DEMO_GATE` counts rehearsals against a threshold of two, and blocks
- [ ] Trip case: one rehearsal must not reach the threshold
- [ ] Trip case: an export with a failed cell must not count
- [ ] `docs_present` checks all four files and blocks
- [ ] A recorded backup is exported from an executed notebook and committed or linked
