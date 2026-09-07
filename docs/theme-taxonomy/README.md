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

## The proposed merge

`scripts/propose_taxonomy.py` counted the 499 raw aspects into `aspect-counts.csv`. The merge
in `merge-proposal.csv` (aspect → theme) groups 75 aspects into **11 candidate themes**;
the remaining 424 aspects are a long tail of singletons and product-specific phrasings and fall
to `other`. `scripts/score_taxonomy.py` applies the ADR support rule to the *merged* theme over
**distinct** reviews and products (summing per-aspect counts would double-count a review that
produced two aspects in the same theme) and writes `merge-table.csv` (every merge, with phrase
counts preserved) and `theme-support.csv`.

Measured over the 313 low-rated discovery reviews:

| Theme | Aspects | Phrases | Low-rated reviews | Share | Products | Meets rule |
|---|---:|---:|---:|---:|---:|---|
| `does_not_work` | 11 | 90 | 71 | 22.7% | 59 | yes |
| `poor_build_quality` | 6 | 50 | 45 | 14.4% | 43 | yes |
| `overpriced` | 4 | 43 | 33 | 10.5% | 32 | yes |
| `wrong_size_or_fit` | 8 | 41 | 32 | 10.2% | 31 | yes |
| `unpleasant_texture` | 8 | 34 | 27 | 8.6% | 28 | yes |
| `breaks_or_wears_out` | 7 | 29 | 25 | 8.0% | 24 | yes |
| `unpleasant_scent` | 3 | 29 | 23 | 7.4% | 24 | yes |
| `hard_to_use` | 9 | 28 | 23 | 7.4% | 24 | yes |
| `not_as_described` | 8 | 29 | 22 | 7.0% | 24 | yes |
| `irritation_or_harm` | 6 | 27 | 21 | 6.7% | 22 | yes |
| `arrived_damaged` | 5 | 23 | 18 | 5.8% | 19 | yes |

At least one of these themes covers **219 of 313 low-rated reviews (70.0%)**; the other 30% are
covered only by `other`.

## Open decision for Philip, blocking the freeze

**All 11 candidate themes clear the 3%-and-two-products support rule, but ADR-0003 sets a hard
ceiling of ten and targets eight.** The rule cannot break the tie, so the cut is a hand
decision and this script will not make it. Three ways to reach ten or fewer:

1. Drop `arrived_damaged` (lowest support, 5.8%). Cleanest single cut, but it is the only theme
   about fulfilment rather than the product, so it is the one most likely to shift post-2020.
2. Merge `breaks_or_wears_out` into `poor_build_quality` (cheap build and early failure are
   adjacent) → 10 themes with a 19–20% combined theme. Loses the wear-over-time signal.
3. Merge `unpleasant_texture` and `unpleasant_scent` into one `unpleasant_sensory` → 10 themes.
   Blurs two physically distinct complaints.

Nothing downstream — the hand-labelling tool, prompt development, the audit set — can start
until this is settled, because the frozen taxonomy is on screen while labelling.
