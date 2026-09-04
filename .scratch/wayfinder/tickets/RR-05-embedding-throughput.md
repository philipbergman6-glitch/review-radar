---
id: RR-05
title: MiniLM embedding throughput on this host
type: task
status: closed
assignee: philipbergman (claimed 2026-09-04)
blocked-by: []
blocks: [RR-06]
---

## Question

Nothing to decide — produce the number the embedding-scope decision waits on. The artifact's
own instruction for the embedding step is "measure throughput first, size the cohort to under
an hour of CPU", and no measurement exists.

Measure, on this host, under demo conditions:

1. Sentence-embedding throughput in reviews/second for the intended MiniLM model on CPU,
   over a real sample of review text — not synthetic strings. Review length matters: mean 33
   words, median 102 chars, max 14,989 chars (`docs/phase0-profile.txt`).
2. The effect of batch size and of truncation length on that rate.
3. Peak RSS while embedding, against the constraint that `.venv` is already 1.7 GB (torch
   489 MB) and the Spark driver takes 4 GB on the host
   (`docs/AUDIT_REPORT_2026-09-01.md` F8).
4. Measure with the Supabase stack stopped, per `docs/DEMO_RUNBOOK.md` §2 — a contended VM
   measured the producer at 231,849 rec/s against an earlier 483,503, so contention is worth
   roughly 2× and the number must say which condition it was taken under.

Then project the wall-clock cost of the two candidate cohorts:

- reviews of ≥20 words belonging to the 1,902 products with ≥50 reviews (the artifact's
  "cohort"), and
- all 349,059 reviews of ≥20 words (49.8% of the corpus).

**Deliverable.** The command, the conditions, and the numbers — the audit's F6 lesson is
that a throughput figure without its command and conditions should be dropped rather than
quoted.

## Resolution

Closed 2026-09-04. Findings, command, and conditions:
[`docs/research/RR-05-minilm-embedding-throughput.md`](../../../docs/research/RR-05-minilm-embedding-throughput.md).
Script: `scripts/bench_embed.py`.

**Setup** `[observed]`: Apple M5 (10 cores, 16 GiB), `.venv` Python 3.11.15, torch 2.13.0,
sentence-transformers 6.0.1 (already installed), model `sentence-transformers/all-MiniLM-L6-v2`
(`src/common/config.py:51`), 87 MB download, CPU only, 4,000 real reviews from `data/sample/`
(mean 75.7 words — longer than the cohorts, so rates are conservative). Measured under a host
lock with Supabase stopped and no Spark running; one extra run with Supabase up.

**Throughput** `[observed]`, `model.encode` only, after warm-up:

| config | reviews/s |
|---|---|
| batch 64, 256 tok, 4 threads (torch default), Supabase stopped | **373.7** |
| same, Supabase running | 368.2 (contention 1.5%, not the producer's 2×) |
| batch 16 / 256 at 256 tok | 371.0 / 316.7 |
| batch 64, 128 tok / 64 tok | 507.9 / 812.0 |
| batch 64, 256 tok, 10 threads | 422.0 |
| batch 64, 64 tok, 10 threads (best) | **905.5** |

**Peak RSS** `[observed]`: 740 MB at batch 64, 1,795 MB at batch 256 (2,008 MB with 10
threads); 424 MB after model load. Batch 64 fits beside the 4 GB Spark driver; 256 does not
inside Spark workers.

**Cohorts** counted from `data/raw` `[observed]`: (a) ≥20 words in the 1,902 products with
≥50 reviews = **123,510**; (b) all ≥20 words = 349,073 (profile: 349,059).

**Wall-clock** `[inferred]`: (a) **5.5 min** at the default stopped-Supabase rate, 5.6 min
contended, 2.3 min at the best rate; (b) **15.6 min** default, 15.8 min contended, 6.4 min
best. Both cohorts are far under the "one hour of CPU" bound; the scope decision in `RR-06`
turns on memory and index size, not on embedding time. Truncation to 64 tokens is the only
big lever (2.2×) and clips about half the cohort's reviews — a quality call for `RR-06`.
