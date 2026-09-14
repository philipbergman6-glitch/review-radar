---
status: accepted
date: 2026-09-14
---

# `publication_ready` is unreachable by construction, and publication mode is declined out loud

[ADR-0008](0008-run-ledger-as-lineage.md) gave the lineage gate two flags and
[ADR-0011](0011-phase-gates-print-a-number-and-only-reproducibility-blocks.md) named
`scripts/gate_lineage.py --mode publication` as the Lineage track's blocking command.
`chain_clean` is the verdict; `publication_ready` was meant to be the stricter question — every
declared capability pinned, every pinned output still the current one, no run left `running`.
Since the holdout re-run of gold it has printed `false`, and the standing explanation ("stale
gold outputs, nobody's ticket yet") assumed the chain was at fault. It is not. The check is.

## What the walk actually counts

`scripts/gate_lineage.py` computes `stale_outputs` as one for every walked run whose output
still exists but is no longer its table's head. The walk deliberately includes runs that are
not the published one, because three parts of the design say it must:

1. **Recorded pins walk superseded runs on purpose.** `conf/lineage_chain.toml` declares
   `upstream_pin = "recorded"` on four edges (ticket 10a): a frozen sample names the run it
   read, not the latest, and the gate pins and walks that run *beside* the published one. Gold
   run `4cd1fde7`, which P6's frames were drawn from on 2026-09-07, is therefore always walked,
   and it is always behind the head — three stale outputs. `theme_samples` run `3265c771`, the
   frame the blind reference labels were written against, has had its `matched_controls`
   overwritten by a later draw — a fourth.
2. **Calibration pins a pre-freeze gold run by protocol.** `gold_calibration` records
   `6e1c0d3a`, the run the rule was calibrated on before the freeze commit
   ([ADR-0012](0012-calibration-precedes-the-freeze-and-the-ceiling-never-moves.md)). The
   holdout gold run `0cbdcc0d` came after, so calibration can never name the current head:
   three more stale outputs, and the count can only grow with every legitimate re-run.
3. **Control and demo share a table.** P8's control projection (`74dde108`) and demo
   projection (`c95be84a`) both write `stream.product_month`, and both are pinned from their
   own artefacts. Whichever ran second owns the head; the other is stale — the last two.

That is the nine in `eval/lineage/gate.json` (`stale_outputs=9`), and every one of them is a
run the design requires the gate to walk. `publication_ready=true` cannot be reached by running
anything in any order. It is not a pending phase; it is a flag whose definition contradicts the
pinning rules written after it.

## Decision

**Publication mode is declined, and the decline is written here rather than coded around.**

- `--mode development` is the Lineage track's command. Its verdict, `chain_clean`, is the
  claim the submission makes: every artefact joins back to a ledger row, every recorded input
  joins to the run that produced it, `chain_links_checked > 0`. That is the reproducibility
  claim ADR-0011 asks of the track, and it holds (`chain_clean=true`, 101 links, 20 runs,
  0 pending, 0 orphaned).
- `--mode publication` and `make gate-lineage-publication` stay in the tree and keep printing
  the number. A target that cannot pass is not deleted, because deleting it would hide the
  count the walk produces; its help line now says it cannot pass and why.
- `publication_ready` keeps its definition and keeps printing `false`. A `true` earned by
  narrowing what "stale" means would be a pass by exception, not by fact.
- The stale outputs remain individually legible: each `LINEAGE_OUTPUT` line prints
  `current=false` with the run and snapshot, so a reader can check that all nine are runs the
  chain pins on purpose. A stale output on a run pinned from a *published* artefact of a
  single-writer table — the case the flag was built for — would still show here; there is none.

The ADR-0011 canonical table's Lineage row is amended by this ADR: blocking line
`LINEAGE_GATE gate_mode=development chain_clean= … chain_links_checked=N`;
`publication_ready` is a reported line, declined as a bar.

## Considered and rejected

- **Count stale only on the run each capability's own artefact pins.** Removes the four from
  the recorded pins and nothing else. `gold_calibration` (route 2) and the control projection
  (route 3) are pinned from their own artefacts, so the count stays at five and the flag stays
  false. Reaching `true` needs three carve-outs — recorded pins, calibration, shared streaming
  tables — each one an exemption the walk cannot check, which is the opposite of what ticket
  10a bought.
- **Re-run everything downstream of the head so the recorded pins catch up.** Redraws P6's
  frozen frames and re-labels against them, and re-calibrates a rule the freeze forbids
  touching (ADR-0001, ADR-0012). Inference may repeat, measurement may not (ticket 09).
- **Delete the flag and the mode.** Loses the `current=false` evidence per output and the
  per-run stale count, both of which a reader needs to check this ADR's claim.
