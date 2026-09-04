---
id: RR-13
title: A printed number for every phase gate
type: grilling
status: open
assignee: unassigned
blocked-by: [RR-01, RR-02, RR-06, RR-08, RR-09, RR-10, RR-12, RR-16, RR-17, RR-11]
blocks: [RR-14, RR-18]
---

## Question

The map's standing rule is that every phase gate prints a number. Several currently do not:

| phase | gate as written | is it a number? |
|---|---|---|
| Silver | `count(bronze) == count(silver) + count(rejects) + duplicates_removed` | yes — keep |
| Gold | "three insights written down with numbers" | **no** — a count of prose |
| ES + embeddings | "index doc count equals silver cohort count" plus "the tables exist and were read aloud once" | half |
| Aspects | "the evaluation table exists with real numbers" | **no** — existence, not a threshold |
| RAG | "20/20 answers cite retrieved reviews" | yes — keep |
| Stream | "a late event visibly lands in the correct window" | **no** — visibly is not a number |
| Deliverables | "a dry run to someone who has not seen the project" | **no** |
| Lineage (`RR-16`) | none yet | must be a row count or a snapshot-id match |
| Repo baseline (`RR-15`) | none yet | CI green on the commit; `pytest` count |

For each phase in the numbering fixed by `RR-01`, produce:

1. the **command** that prints the gate (a script path and its flags);
2. the **printed value**, named exactly;
3. the **pass threshold** — the value at which the phase is done, decided now rather than
   after seeing the output, which is the whole point of a gate;
4. what happens on failure — does the phase reopen, or does a documented deviation ship?

Learn from the audit's F3: the existing exactly-once gate prints `PASS` for runs that tested
nothing, because it never asserts `0 < partial < records`. **A gate that cannot fail is not a
gate** — every threshold here needs a case that would trip it.

The AI rows take their thresholds from `RR-17`; the silver row may be read from
`pipeline_runs` per `RR-16`. Blocked on every phase-content decision, because a threshold cannot be set before the phase
knows what it produces.
