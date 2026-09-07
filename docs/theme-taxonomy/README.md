# Discovery run and the proposed complaint taxonomy

Everything here is **input to a decision Philip makes by hand** (ADR-0003, "Taxonomy and human
labels"). No file in this directory freezes the taxonomy; the frozen artifact will be
`conf/theme-taxonomy.json`, written only after the merge below is approved.

## The discovery run

`theme_labels_llm` spec v1, run `a4a62ffd-61d0-434d-bdcf-fcc2b2b400fd`, 2026-09-07.

| | |
|---|---|
| Model | `qwen3:8b` via Ollama, `api_mode=local`, temperature 0, seed 20260907, `think:false` |
| Prompt | `discovery-v1` (`conf/prompts/discovery-v1.txt`), config hash `af22cf44df76` |
| Sample | `discovery`, 600 pre-2020 reviews drawn by `conf/theme_sampling.toml` (70% rated ≤3) |
| Result | `ok=484` `parse_failed=116` `api_failed=0`, 897 phrases, 499 distinct aspects, 147 reviews with no complaint |
| Cost | 18,538 s wall clock, 30.9 s/review averaged (see the throughput note below) |

**Throughput note, reported because it changes the P6 time budget.** The first 500 reviews ran
at 2.7–4.6 s/review; the last 100 ran at roughly 30 s/review. The RR-19 budget was measured at
6.7 s/review on 8 reviews and is not representative of a long serial run on this host. Later
budget lines should be planned against the observed tail, not the smoke-test rate.

## Failure characterisation

`scripts/discovery_failures.py` → `discovery-failures.csv`. 116 of the 600 discovery rows
(19.3%) ended `parse_failed`; every one was a **semantic validation** failure, none a transport
or JSON-parse failure (`api_failed=0`).

| Cause | Reviews | Share of failures |
|---|---:|---:|
| `quote_too_long` (>12 words) | 82 | 70.7% |
| `quote_not_in_review` (invented or paraphrased quote) | 26 | 22.4% |
| `aspect_too_long` (>3 words) | 20 | 17.2% |

(Shares exceed 100% because one response can fail several ways.)

**The one-retry rule is a measured no-op.** 114 of the 116 failures produced a byte-identical
validation error on the retry. ADR-0003 mandates "one retry with the identical prompt and
inference settings"; at temperature 0 with a fixed seed that retry is deterministic and cannot
succeed after a *validation* failure. It is kept because it is the frozen protocol and because
it still has real value for `api_failed`, where the fault is transport, not sampling. The
doubled inference cost of the retry is the price of not editing a protocol mid-run.

The failures are not silently dropped: all 116 rows are stored with both raw responses and
both validation errors, and 19.3% is the discovery-stage failure coverage.

## The merge, and the frozen taxonomy

`scripts/propose_taxonomy.py` counted the 499 raw aspects into `aspect-counts.csv`. The merge
in `merge-proposal.csv` (aspect → theme) groups 75 aspects into named themes; the remaining
424 aspects are a long tail of singletons and product-specific phrasings and fall to `other`.
`scripts/score_taxonomy.py` applies the ADR-0003 support rule to the *merged* theme over
**distinct** reviews and products (summing per-aspect counts would double-count a review that
produced two aspects in the same theme) and writes `merge-table.csv` (every merge, with phrase
counts preserved) and `theme-support.csv`.

The first pass produced **11** candidate themes and every one of them cleared the rule, while
ADR-0003 caps the taxonomy at ten. The rule cannot break that tie, so the script reported
`over_ceiling=yes` and refused to trim. Philip resolved it in `RR-20` on 2026-09-07 by
**merging `breaks_or_wears_out` into `poor_build_quality`** — ten themes, nothing dropped.
Dropping `arrived_damaged` (the lowest-support theme) was rejected because it is the only
fulfilment theme and therefore the one most likely to shift post-2020; blurring texture into
scent was rejected as compressing two physically distinct failures.

The frozen artifact is **`conf/theme-taxonomy.json` v1**, with definitions, inclusions,
exclusions and boundary notes per theme. Measured over the 313 low-rated discovery reviews:

| Theme | Aspects | Phrases | Low-rated reviews | Share | Products |
|---|---:|---:|---:|---:|---:|
| `does_not_work` | 11 | 90 | 71 | 22.7% | 59 |
| `poor_build_quality` | 13 | 79 | 67 | 21.4% | 64 |
| `overpriced` | 4 | 43 | 33 | 10.5% | 32 |
| `wrong_size_or_fit` | 8 | 41 | 32 | 10.2% | 31 |
| `unpleasant_texture` | 8 | 34 | 27 | 8.6% | 28 |
| `unpleasant_scent` | 3 | 29 | 23 | 7.4% | 24 |
| `hard_to_use` | 9 | 28 | 23 | 7.4% | 24 |
| `not_as_described` | 8 | 29 | 22 | 7.0% | 24 |
| `irritation_or_harm` | 6 | 27 | 21 | 6.7% | 22 |
| `arrived_damaged` | 5 | 23 | 18 | 5.8% | 19 |

At least one of these themes covers **219 of 313 low-rated reviews (70.0%)**; the other 30%
are covered only by `other`.

**Stated cost of the merge.** `poor_build_quality` absorbed the wear-over-time signal, so a
product that was cheaply made and one that failed after six months are no longer separable in
the theme-shift table. Recorded here so no later reader infers durability was tracked alone.

## Ground truth

`RR-21` (2026-09-07) supersedes `RR-19`: the **agent** labels both 200-row sets, blind — only
`title`, `text` and the frozen taxonomy are visible, never `qwen3:8b`'s output, the star
rating, the product or the window. Those rows carry `label_source="agent_reference"`, a value
distinct from both `local_llm` and `human`.

Because ground truth written by one LLM to evaluate another has correlated errors, the risk is
**measured rather than caveated**: Philip hand-labels a stratified 50 of the audit set, drawn
by the seeded `draw_key` before he sees any agent label, and the Claude-vs-Philip agreement is
published as a number (per-theme and overall, Cohen's kappa, Wilson 95% interval) in the
evaluation table's verdict column. His labels never overwrite the agent's and never re-tune
anything. The 40 five-day-apart repeats are reported `NOT_RUN` — intra-annotator stability is
not defined for a deterministic labeller, and is not replaced with a number that resembles it.

The 0.70 macro-F1 bar and the 0.50 recall floor, frozen before any measurement, do not move.
