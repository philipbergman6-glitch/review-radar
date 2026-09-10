# 04 — Lineage gate over the declared chain

**What to build:** a gate that walks the chain declared in `conf/lineage_chain.toml` and
proves provenance is a join rather than an assertion — every artefact, Iceberg snapshot
and Elasticsearch generation traceable to the `pipeline_runs` row that produced it. It
prints how many links it checked, so a passing verdict over an empty chain is impossible.

The run ledger is one row per execution *attempt* of every job: typed core plus JSONB
inputs, outputs, counts and params, with a UUID run id generated **before** execution and
stamped into everything the run writes. Insert `running`, finalize `success|failed`.
Partial outputs are kept and never rolled back, so a failure stays diagnosable.

The gate runs in development or publication mode.

**Blocked by:** 02 — the gates must already write artefacts.

**Status:** ready-for-agent

- [ ] `conf/lineage_chain.toml` declares the chain; the gate reads it rather than hardcoding
- [ ] The gate prints the number of links checked alongside its verdict
- [ ] Every phase gate prints `run_contract_registered` for the jobs it adds, and it blocks
- [ ] Every artefact reachable from the chain carries a run id resolving to a ledger row
- [ ] A failed run's partial outputs are present and its ledger row reads `failed`
- [ ] Trip case: an empty chain must not pass
- [ ] Trip case: an artefact whose run id has no ledger row must fail
