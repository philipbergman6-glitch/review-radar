# The two FAILs — viva sheet

`GOLD_CALIBRATION` (power **0.1857** vs bar **0.80**) and `THEMES_QUALITY` (macro-F1 **0.4583**
vs bar **0.70**) are the two numbers an examiner reaches for first, because they are the only
two places in the evaluation table where the project states in public that it missed its own
bar. Both have good answers. Both are easy to fumble live, in opposite directions: the
calibration answer fumbles by *apologising* for a result that is actually the strongest
methodological evidence in the project, and the themes answer fumbles by *overclaiming* a
labeller that is genuinely weak.

Understanding is 5% of the grade. This sheet exists because a confident wrong sentence costs
more than the 5%: it retroactively makes every other number in the deck look unexamined.

Every figure below is quoted from an artefact, named beside it. Nothing here is rounded past
what the artefact prints.

---

## The two sentences

Say the first. Never say the second — not as a throwaway, not in an answer about something
else.

> **Survives:** *an alert is unlikely to be noise.* The rule was selected under a noise ceiling
> committed before the measurement, and runs at **0.80 placebo alerts/month against a ceiling
> of 1.0** (`GOLD_PLACEBO … alerts_per_month=0.8021 ceiling=1 verdict=PASS`).

> **Does not survive:** *the rule finds declining products.* It detects **0.1857
> [0.1678, 0.2051]** of injected 0.3-star declines within 6 evaluable points — it misses about
> **81%** of them. The 139 alerted products are a **floor, not a census**.

`docs/DESIGN.md` §6 already binds this: *"The rule finds declining products does not follow
from this and is not claimed."* The deck's slide 2 variant B is the same sentence in
presentation form.

---

## FAIL 1 — `GOLD_CALIBRATION`, power 0.1857 vs 0.80

**Q. Your detection power is 0.186 against a bar of 0.80. That's a four-fold miss. Why is the
project still standing?**

**A.** Because the bar was committed to `conf/decline_rule.toml` **before** the producer ran,
and it was not moved afterwards. The search covered **144** grid configurations; **116** hold
the placebo ceiling; the winner `B6 R12 δ0.3 P3 min_reviews20` is the **highest-power
configuration under the ceiling**, at power **0.1857 [0.1678, 0.2051]** and **0.8021 placebo
alerts/month**. The selection reason is written into the artefact rather than into prose:
*"highest power (0.186) among 116 configurations under the ceiling; the 0.80 power target is
not met and is reported as missed rather than bought by a weaker ceiling."* A FAIL that is
printed, interval-bounded and attributable to a pre-registered bar is a result. A PASS obtained
by moving the bar afterwards would not have been.

**Q. Couldn't you just have tuned harder? Maybe the grid was too small.**

**A.** The frontier answers that, and it is why the failure is a finding rather than an
excuse. Ignoring the ceiling entirely, the **loosest configuration in the whole grid** —
`B6 R12 δ0.3 P2 n10` — reaches power **0.4604** at **3.9890 alerts/month**
(`GOLD_CALIBRATION_FRONTIER … under_ceiling=false`). That is **four times over the noise
ceiling and still barely half the bar**. So 0.80 was not missed by a corner of the grid that
went unsearched; it is **unreachable at any honest noise budget on this corpus**. The
shortfall is about window shapes and per-product review volume in `All_Beauty`, not about
search effort.

**Q. So why not relax the noise ceiling until power clears 0.80?**

**A.** That move was **pre-rejected**, in writing, before the number existed — RR-09 round 5
fixed the ceiling at 1 alert/month on a workload argument (four investigations of capacity, at
most a quarter of them noise) and pre-declared the contingency: *do not weaken the ceiling;
report the trade-off.* ADR-0012 lists it as the first rejected option. A noise bar chosen after
seeing that the rule cannot clear it is not a bar. The same ADR also rejects restating the bar
at 0.5 stars where power is higher (**0.2768**) — and notes that a superseded config block in
commit `6178411` had already made exactly that move, injecting 0.5 stars and citing an ADR that
did not exist. It was caught and reverted. That is the audit trail, not a confession.

**Q. What does a FAIL actually stop?**

**A.** By ADR-0011, quality gates print a number and block nothing; only reproducibility gates
block. `GOLD_CALIBRATION` is the single exception on the map: it blocks the **protocol freeze**
rather than the phase. Within that, the split is deliberate — a **ceiling breach refuses the
freeze outright**; a **power shortfall is the one waivable refusal**, and the waiver is an
explicit flag that writes its own reason into the config. So the override is recorded, not
silent.

**Q. What is the rule good for, at 19% detection?**

**A.** Triage, not census. RR-09 round 5 pre-declared **investigate and watchlist** as the
response to exactly this outcome, which is why the alert list is presented that way rather than
as a finding about the category. The honest framing: **139 of 502 eligible products** alerted
in the 2020–2023 holdout under a rule frozen beforehand — a low-yield, low-noise monitor.

**Q. Anything the measurement told you that you didn't expect?**

**A.** Yes, and it would have made a plausible sentence false. A trailing-baseline rule absorbs
a sharp step within `B` months, so a **gradual** decline holds the alert condition for more
consecutive points than a step of the same terminal size. At 0.5 stars, the six-month ramp is
caught **more** often than the step: **0.3032 vs 0.2768**. *"The rule detects sharp declines
best"* is intuitive, and wrong here. The bar is stated at the step; the ramp is reported beside
it.

---

## FAIL 2 — `THEMES_QUALITY`, macro-F1 0.4583 vs 0.70

**Q. Macro-F1 0.4583 against a bar of 0.70. Is the labelling layer just not working?**

**A.** It works, and it is not good enough for the word *explains* — those are different
claims. On the held-out audit set of 200 reviews: labeller **0.4583 [0.3647, 0.5223]**, MLlib
classifier **0.3899 [0.3074, 0.4639]**, star-only floor **0.2968 [0.2612, 0.3493]**. The
labeller's interval is **disjoint** from the floor's, so it is distinguishably better than
guessing themes off the star rating; the **classifier's overlaps** the floor and is not. And
the direction matters: on development the labeller and the floor **overlapped**, so the holdout
**reverses** that earlier, smaller-sample reading rather than confirming it. That is a holdout
doing its job.

**Q. You swapped to a local 8B model. Doesn't that invalidate the 0.70 bar?**

**A.** `ANTHROPIC_API_KEY` is empty, so the labeller is local. The bar **did not move** —
0.70 stayed 0.70 for the weaker labeller, which is the whole point of setting it first.
`llama3.2:3b` measured degenerate on a v0 prompt (all 8 candidate themes on 5 of 8 reviews, at
3.78 s/review); `qwen3:8b` discriminates at 6.7 s/review. The result is reported as a FAIL that
blocks nothing.

**Q. Where does the gap actually come from?**

**A.** Two named places, and the first precedes any judgement about theme quality:

1. **Parse failures: 27 of 200, 13.5%.** Each scores as an **empty prediction**, so it is a
   pure recall loss (`parse_failed=27 api_failed=0 absent=0 abstained=0`). Counted, not
   excluded.
2. **Over-firing.** The per-theme rows show high recall, low precision on the broad themes:
   `overpriced` p=0.219 r=0.824, `not_as_described` p=0.212 r=0.778, `unpleasant_scent`
   p=0.176 r=0.600. The narrow, physical themes are the strong ones: `arrived_damaged`
   f1=**0.750**, `irritation_or_harm` p=**0.700**.

**Q. Nine themes, not ten — did you drop the one that was failing?**

**A.** No, and the counts are printed so that can be checked. `wrong_size_or_fit` has support
**8** in the reference labels, below the pre-set `min_support` of 10, so it is excluded from the
headline average by the support rule — not by its score. Its row is printed anyway:
p=0.086 r=0.375 f1=0.140, `supported=false`. Including it would **lower** the headline, so the
exclusion is not flattering the number. Same discipline on the strata: the 80
prevalence-representative rows are reported **NOT_RUN**, because no theme reaches `min_support`
there, so macro-F1 is undefined — the interval beside it in the sealed artefact is an artefact
of the resampler, not a result.

**Q. What can you still say about themes, then?**

**A.** That they **illustrate** a decline, not that they explain one. The demo hero
`B00RPJZMUM` is presented over **18 baseline and 3 recent** labelled mentions — an
illustration, explicitly not an estimate. And the fork that decides slide 2 was resolved by
this number, not by mood: candidate supply was fine (`matched_candidates=708` against a
threshold of 3), so it was the **measured quality guard** that narrowed the claim to variant B.
Recording which of the two guards fired is the point of having written the rule down.

---

## Traps — confident wrong sentences

| Don't say | Say instead |
|---|---|
| "The rule finds declining products." | "The rule raises few, low-noise alerts; it misses ~81% of declines its own size." |
| "0.186 is low but the dataset is hard." | "0.80 is unreachable under the ceiling — the frontier tops out at 0.4604 at 3.99 alerts/month." |
| "We'd have passed with a looser ceiling." | "Loosening the ceiling was pre-rejected by RR-09 round 5 and never done. Even unceilinged, the grid reaches 0.4604." |
| "The rule detects sharp declines best." | At 0.5★ the ramp beats the step, 0.3032 vs 0.2768. |
| "About a quarter of alerts are noise." | **Not established.** 0.80 placebo alerts/month is measured on the pre-2020 calibration frame under permutation; the holdout's 148 alerts sit on a different population and eligibility. No like-for-like noise fraction has been computed — don't quote one. |
| "The labeller explains the declines." | It is distinguishably above the star-only floor and below its own bar; themes illustrate. |
| "The LLM beats the classifier." | Their intervals **overlap** ([0.3647, 0.5223] vs [0.3074, 0.4639]). Only the comparison against the *floor* separates them: labeller disjoint, classifier overlapping. |
| "We dropped a theme that wasn't working." | Excluded by the pre-set `min_support` 10 rule at support 8; counts printed; including it would lower the headline. |
| "A FAIL means the phase failed." | ADR-0011: quality gates print a number and block nothing. `GOLD_CALIBRATION` blocks only the freeze. |

## If pressed to concede something

Concede this, and only this: **the project measures a monitor it cannot yet make sensitive, and
labels themes it cannot yet call explanations.** Both shortfalls were measured against bars set
in advance, both are reported with intervals, and neither bar was moved to accommodate the
result. The declines are stated as declines; nothing is upgraded by wording.

---

*Sources: `eval/gold_calibration/gate.json`, `eval/gold_calibration/selected.json`,
`eval/themes_quality/gate.json`, `docs/adr/0012-calibration-precedes-the-freeze-and-the-ceiling-never-moves.md`,
`docs/adr/0011-phase-gates-print-a-number-and-only-reproducibility-blocks.md`, `docs/DESIGN.md` §6–§7,
`docs/SLIDES.md`. `tests/test_written_deliverables.py` fails if the headline figures here drift
from the gate artefacts.*
