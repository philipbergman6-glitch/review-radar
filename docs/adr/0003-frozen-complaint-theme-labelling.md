---
status: accepted
date: 2026-09-06
---

# Hosted Haiku labels a frozen, complaint-only taxonomy

The primary analysis needs consistent complaint labels, not a broad aspect-by-sentiment
ontology. We will discover at most ten named themes from pre-2020 episode and matched-control
reviews, freeze their definitions and prompt, and use Haiku 4.5 as the primary labeller. Only
its frozen labels feed the theme-shift table. `llama3.2:3b` is evaluated on the development
and audit sets and remains available for one single-review demo; the demo otherwise reads
cached Iceberg labels and never calls a hosted model.

## Taxonomy and human labels

- Draw a seeded 600-review pre-2020 discovery sample from episode and control windows,
  oversampling reviews rated at most 3 stars to about 70%. Haiku extracts free complaint
  phrases. Merge them by hand into named themes and definitions, preserving phrase counts and
  every merge in `docs/theme-taxonomy/merge-table.csv`. Keep a theme only when it occurs in at
  least 3% of the low-rated discovery reviews and at least two products. Target eight themes;
  ten is a hard ceiling. MiniLM clusters are a cross-check only if cheap.
- Hand-label 200 enriched pre-2020 development reviews and 200 post-2020 audit reviews. The
  audit contains 120 rows enriched from the frozen theme terms and 80 prevalence-representative
  rows from candidate and control windows; report the representative subset separately. Relabel
  40 audit rows at least five days later and report per-theme intra-annotator kappa.
- Try at most five prompt versions. Commit every version and its development per-theme table.
  Freeze the last selected prompt before opening the audit set.
- Pass when audit macro-F1 over supported named themes is at least 0.70 and no supported theme
  has recall below 0.50. A supported theme has at least ten audit positives. Report precision,
  recall, F1 and support per theme; development macro-F1; `other`; abstention; parse/API failure
  coverage; and a seeded product-clustered bootstrap interval as context.
- After scoring, adjudicate each disagreement once without changing the labels or spec. Allowed
  causes are `model_missed`, `model_invented`, `definition_boundary`, `human_error`, and
  `star_misleading`. Commit one row per disagreement, show cause counts and five worked examples,
  including at least one misleading-star case where the model was right.

## Output and failure semantics

The versioned contract is `conf/complaint-theme-label.schema.json`. A successful response has
one item per negatively mentioned named theme, each with an exact contiguous quote of at most
15 whitespace-delimited words. Theme ids must belong to the frozen taxonomy and may occur once.
Positive praise is not labelled. `other.phrase` is required iff `other.present` and is at most
five words; otherwise it is null. Unmentioned themes are omitted.

`abstain=true` means the model deliberately declines to label; it is not a transport or parse
failure. An abstention must have no themes, `other={"present":false,"phrase":null}`, and
`label_confidence="low"`. The overall sentiment may still describe explicit review text.

Validate strict JSON, the schema, frozen theme membership, uniqueness, word limits, conditional
`other`/abstention rules, and evidence presence in the normalized title or text. Never strip code
fences, repair JSON, coerce a value, or manufacture a quote. An invalid response gets one retry
with the identical prompt and inference settings. If that also fails, store both raw responses
and both validation errors with `label_status="parse_failed"`; emit no label and report the row
in failure coverage. API failures use the same one-retry ceiling and end as `api_failed`.

## Data minimisation, execution and budget

The only review fields sent are `title` and `text`. No id, product id, timestamp, rating, product
name, metadata, or human label crosses the wire. The exact request template is in
`docs/LLM_LABEL_RUNBOOK.md`.

Public is not the same as not sensitive; we sent the minimum field set and no identifier, and
cached every response so the demo makes no live call.

The hosted-call ceiling is 12,800 logical calls:

| Budget line | Reviews/calls | Execution |
|---|---:|---|
| pre-2020 discovery | 600 | Batch |
| pre-2020 prompt development | 200 reviews × at most 5 versions = 1,000 | standard |
| pre-2020 classifier training pool | 3,000 | Batch |
| post-2020 candidate/control inference | at most 8,000 | Batch |
| untouched post-2020 audit | 200 | Batch, once after freeze |

Retries caused by invalid output or API failure are recorded separately and do not enlarge a
cohort; they do count toward the provider spend cap. The application stops submitting new work at
12,800 logical calls or an estimated $20 cumulative spend, whichever comes first. Set the Claude
Console workspace limit to $20 before provisioning the key. The estimate from RR-03 is about $16
if all work used standard pricing and about $8 for Batch-eligible work; the cap leaves retry and
token-length headroom. A cap hit is a reported incomplete budget line, never silently resampled.

## Iceberg cache and idempotency

`gold.review_theme_labels` has one row per logical inference and the following schema:

```text
idempotency_key string NOT NULL
source_review_id string NOT NULL
budget_line string NOT NULL
label_source string NOT NULL              -- hosted_llm | local_llm | human
model_id string NOT NULL
label_spec_version string NOT NULL
prompt_version string NOT NULL
inference_config_hash string NOT NULL
api_mode string NOT NULL                  -- batch | standard | local | manual
label_status string NOT NULL              -- succeeded | model_abstained | parse_failed | api_failed
themes array<struct<theme_id:string,evidence_quote:string>>
other_present boolean
other_phrase string
abstain boolean
overall_sentiment string
label_confidence string
attempt_count int NOT NULL
attempts array<struct<attempt_no:int,provider_request_id:string,raw_response:string,
                      validation_error:string,input_tokens:int,output_tokens:int,
                      estimated_cost_usd:decimal(12,6),completed_at:timestamp>>
created_at timestamp NOT NULL
```

The idempotency key is lowercase hex SHA-256 over the UTF-8, length-prefixed tuple
`(source_review_id, label_source, model_id, label_spec_version, prompt_version,
inference_config_hash)`. It is computed locally and is never sent. Write with `MERGE` on the key:
an existing terminal row is a cache hit; retries update its `attempts`; no second logical row may
be inserted. The config hash covers temperature, maximum output tokens, system prompt, taxonomy
file hash and output-schema hash. Raw responses are cached so every label is auditable.
For the Batch API's required `custom_id`, generate a fresh random UUID per provider attempt and
retain the local mapping; never derive that wire value from `source_review_id` or the idempotency
key.

## Sentiment weak-label check

This is a sanity check of `overall_sentiment`, not theme validation and not the star-only theme
baseline. Exclude 3-star reviews. For 1–2 stars, agreement means `negative`; for 4–5 stars it
means `positive`; `mixed` and `none` are disagreements. Print one row per star bucket plus an
overall row with `n`, counts for all four predicted sentiments, agreement numerator/rate and a
Wilson 95% interval. Also print the fixed adjudication-cause counts; do not revise text-derived
labels to agree with stars.

## Consequences

- The primary label quality is directly measured and can fail visibly.
- The taxonomy cannot expand after holdout review text or audit outcomes are seen; uncovered
  complaints remain an exploratory appendix.
- Rating remains available locally for the weak-label check and star-only baseline but cannot
  contaminate the hosted text labeller.
