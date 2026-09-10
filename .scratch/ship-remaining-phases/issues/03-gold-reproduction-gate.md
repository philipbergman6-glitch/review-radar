# 03 — Gold reproduction gate

**What to build:** an independent re-derivation of the gold tables that can be run against
the pinned snapshot and either reconciles or does not — the same claim `reproduce-silver`
already makes one layer down. Gold currently asserts its own correctness using the code
that produced it; this makes reproducibility a checked property.

The reproduction is deliberately *not* the Spark path. It re-derives the gold aggregates
by an independent route from the pinned silver snapshot and reconciles counts and key sets
against the published tables.

**Blocked by:** 02 — the gates must already write artefacts.

**Status:** ready-for-agent

- [ ] `make reproduce-gold` re-derives gold from the pinned silver snapshot independently
- [ ] Counts and key sets are reconciled against the published gold tables
- [ ] The gate prints its named constituents then a terminal verdict line
- [ ] It writes an evaluation artefact carrying the run id and the snapshot it reproduced
- [ ] A trip case exists: a perturbed slice must make reconciliation fail
- [ ] A vacuous run — zero rows compared — cannot print PASS
