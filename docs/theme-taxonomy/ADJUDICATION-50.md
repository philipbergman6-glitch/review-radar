# Philip's blind 50 — the labelling worksheet

**Why you are doing this.** Every macro-F1 in P6's evaluation table is scored against ground
truth that a language model wrote. The reference annotator (`claude-opus-5`) and the system
under test (`qwen3:8b`) are both language models, so their errors may be correlated, and a
correlated ground truth would make the headline number look better than it is. RR-21's answer
is not a caveat but a published number: you hand-label a stratified 50 of the audit set, and
the agreement between your labels and the machine's goes in the table as
`THEMES_AGREEMENT`, per-theme and overall, with Wilson intervals.

It is **REPORTED, never barred**. No agreement threshold was frozen before this was measured
and none is being invented now. Whatever it says is the result.

## What is already fixed, and cannot move

| | |
|---|---|
| The 50 | drawn 2026-09-11 by seeded key, `eval/themes/agreement-subset-audit.json`, frozen on first write |
| The draw | seed `20260907`, salt `audit-adjudication`, 3 per theme stratum (30) + 20 prevalence-representative |
| Your file | `eval/themes/blind-agreement-audit.jsonl` — 50 rows, `blind_id` + `title` + `text` |
| Taxonomy | `conf/theme-taxonomy.json` v1, ten themes |

A second `make agreement-draw` that produced a different 50 is refused, not accepted: a subset
redrawn after anyone has seen a label is a subset chosen for its answer.

## The one rule that makes this worth doing

**Label from `blind-agreement-audit.jsonl` and the taxonomy alone.** Do not open, before you
finish:

- `eval/themes/reference-audit.jsonl` (the agent's labels — the thing you are being compared to)
- `eval/themes/blind-audit.map.json` (which review is which, and therefore its star rating)
- any `score-audit-*.json`, the seal, or `conf/prompts/label-v5.txt`

Seeing the agent's answer before writing yours does not produce a disagreement number, it
produces an agreement number, and there is no way to tell the two apart afterwards. If you do
look at any of them, say so and the run is void — the 50 stay frozen and the number is
published as `NOT_RUN` with that reason, which is a better outcome than a number nobody can
trust.

Star ratings are deliberately absent for the same reason: `star_only` is one of the three
systems being scored, and a human who saw the stars would partly be scoring it.

## The policy, verbatim from the reference sessions

These are the rules the agent labelled all 400 rows under. Applying a *different* policy would
measure the distance between two policies rather than between two annotators.

1. **A theme is labelled when the review complains about what the theme's definition covers.**
   Read the definition in `conf/theme-taxonomy.json`, including its `excludes`.
2. **An explicitly accepted shortcoming is not a complaint.** "It's cheap plastic, but for the
   price I'm fine with it" carries no `poor_build_quality`.
3. **Praise is never a label.** A five-star review with no complaint carries no themes at all —
   and a ground truth full of true negatives is what makes precision measurable rather than
   assumed. Roughly half the audit rows have no theme.
4. **There is no cap on themes per review.** Label every theme the review complains about.
5. **`overpriced` needs an explicit money, price or value word.** "Not worth it" alone does not
   qualify.
6. **Worked-then-failed goes to `poor_build_quality`**, per the taxonomy's `excludes`, not to
   `does_not_work`.
7. **Pain-causing stiffness goes to `irritation_or_harm` only.**
8. **A listing mismatch is labelled even inside a positive review** — but not when the reviewer
   waves it off ("smaller than I expected, no big deal").
9. **`abstain` means unreadable, empty of content, or not about a product.** It does *not* mean
   "wrong language": a Spanish review gets labelled normally. The scorer reads an abstained row
   as ground truth of *no themes*, so abstaining on a readable review charges a false positive
   to any system that read it correctly.
10. **`other.present`** when the review complains about something real that none of the ten
    themes covers. The phrase is at most five words.

## The output object

One JSON object per line in `eval/themes/human-agreement-audit.jsonl`, carrying the row's
`blind_id` plus exactly the label schema (`conf/complaint-theme-label.schema.json`):

```json
{"blind_id": "aud-0005",
 "themes": [{"theme_id": "poor_build_quality", "evidence_quote": "the handle snapped after two uses"}],
 "other": {"present": false, "phrase": null},
 "abstain": false,
 "overall_sentiment": "negative",
 "label_confidence": "high"}
```

- `evidence_quote` — **verbatim from the review**, at most 15 words. It is checked against the
  text character for character, exactly as the model's quotes are. A quote typed from memory
  fails the same way an invented one does.
- `other.phrase` — at most 5 words, and `null` whenever `present` is `false`.
- `abstain: true` forces `themes: []`, `other.present: false` and `label_confidence: "low"`.
- `overall_sentiment` — one of `positive` · `negative` · `mixed` · `none`.

**The import is all-or-nothing.** One rejected label fails all 50, because a partially
imported subset silently shrinks the denominator of the agreement number. Check as you go
rather than at the end:

```bash
make agreement-check   # the same validator over whatever is written so far, no import
```

## Running it

```bash
make agreement-export     # already run — regenerates the 50-row worklist if you lose it
# ... label into eval/themes/human-agreement-audit.jsonl, checking with `make agreement-check` ...
make agreement-import     # writes label_source="human" rows, job theme_labels_human, all 50 or none
make agreement-score      # -> eval/themes/agreement-audit.json, per-theme + overall, Wilson + kappa
make gate-themes          # publishes THEMES_AGREEMENT into eval/themes_agreement/gate.json
make eval-table           # the row appears as REPORTED
```

## How the number will read

The unit is **one (review, theme) decision** — 50 reviews × 10 themes = 500 decisions — because
that is how the labels are used downstream and what the per-theme F1s are built from.

Both **raw agreement and Cohen's kappa** are published. Theme presence is rare, so raw
agreement is inflated by the many easy shared negatives and on its own would read as a high
number that means little; kappa corrects for chance and is the honest half. Raw agreement stays
beside it because hiding it invites the suspicion it was dropped after being seen.

Kappa is reported as `none`, never `0.0`, where it is undefined — when neither of you ever
labels a theme, expected agreement is 1 and the correction divides by zero. Perfect agreement
about nothing is not the same as agreement no better than chance.

Nothing here changes a label, re-scores a system or moves a threshold. `RR-21` is explicit: the
50 "never overwrite or re-tune", and the 0.70 bar does not move whatever this says.
