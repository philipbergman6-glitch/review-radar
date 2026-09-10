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

**Status:** done — landed 2026-09-10

- [x] `conf/lineage_chain.toml` declares the chain; the gate reads it rather than hardcoding
- [x] The gate prints the number of links checked alongside its verdict
- [x] Every phase gate prints `run_contract_registered` for the jobs it adds, and it blocks
- [x] Every artefact reachable from the chain carries a run id resolving to a ledger row
- [x] A failed run's partial outputs are present and its ledger row reads `failed`
- [x] Trip case: an empty chain must not pass
- [x] Trip case: an artefact whose run id has no ledger row must fail

## How it landed

**A link is one checkable join**, and there are six kinds: `artefact` (an `eval/<id>/gate.json`
names a run that must resolve, have succeeded, and run a job the capability declares),
`pin` (every job a claimed capability declares has a successful run behind it), `output` (the
recorded Iceberg snapshot is in the table's history *and* stamped with the run id; the ES
generation exists, is the alias target, and every document carries `source_run_id`; the
catalogue rows carry the loader's id), `input` (an identity no edge joins is still checked for
existence, so a cut edge never becomes an unchecked one), `edge` (the join itself, in both
directions: an undeclared edge found in the ledger fails as loudly as a declared edge the
ledger does not show), and `retention` (a `failed` run's partial outputs are still there).

**The chain is rooted in the artefacts, not in the newest ledger row.** A capability with no
artefact yet contributes no links, prints `LINEAGE_PENDING`, and withholds `publication_ready`
while leaving `chain_clean` true -- so a development PASS coexists with an unfinished
submission, which is the shape RR-16 asked for. Walking from artefacts is also what catches a
stale branch: P6's `theme_samples` runs are pinned to an older gold run than the one P3's
artefact publishes, and those runs are correctly *not* in the chain today.

**First run against the real stack:** `LINEAGE_GATE gate_mode=development chain_clean=true
publication_ready=false chain_links_checked=34 LINEAGE_GATE=PASS`, over 7 claimed
capabilities and 6 pinned runs. Two findings the walk surfaced, both real:

* the chain declared P2 as `jobs = ["silver"]` alone, leaving silver's catalogue edge with no
  pinned upstream. `catalogue_load` is P2's other job -- its run id *is* the
  `catalogue_load_id` -- and the declaration now says so. That also put `products` under the
  gate: 112,590 rows, checked in Postgres.
* one `gold` row from 2026-09-07 10:39 is still `running`: a killed driver, exactly the case
  ADR-0008 §3 says the freshness gate catches. It prints as `LINEAGE_ORPHAN` and withholds
  `publication_ready`. Nothing here rewrites it -- the ledger is not edited by a gate.

**Honest limits.** The retention check contributes zero links today because no run has ever
finalized to `failed`; the rule is unit-tested and `tests/integration/test_ledger_pg.py`
covers the ledger half. `run_contract_registered` is wired into the six gates that exist
(silver, silver_repro, gold, gold_repro, search, embeddings); P6/P7/P8 gates add it as they
land, which is exactly when their capability first declares jobs. The bronze edge is declared
`cut` with its reason: the drain writes no ledger row yet, so silver's bronze snapshot is
checked for existence but joined to nothing.
