---
id: RR-18
title: Design doc sections and slide narrative order
type: grilling
status: open
assignee: unassigned
blocked-by: [RR-09, RR-11, RR-13]
blocks: [RR-14]
---

## Question

Graduated from the fog on 2026-09-04: the two deliverables worth 10% directly and framing
the other 90%. The brief (§7, §8) fixes the outer shape — a 1–2 page design document with an
architecture diagram, data flow, technologies and the AI capability; a 5–10 minute talk
covering problem and dataset, architecture, the AI capability and why, results and insights,
challenges and trade-offs; up to 3 minutes of questions.

Decide:

1. **Design doc sections, in order, with the number each one carries.** The coverage doc
   (`docs/course-coverage.md` §3) says the doc must name which of the V's the project
   exercises, each backed by a measured figure. Which sections, which figures — from the
   profile, the exactly-once gate, the phase gates (`RR-13`), and the evaluation tables
   (`RR-17`).
2. **The declined-out-loud paragraph.** Kafka Connect ES sink, HDFS, GraphFrames, Oozie —
   each with its one-line reason, per the map's Out of scope. Where it sits in the doc.
3. **Slide order.** Which of the demo moves opens (`RR-09` decides the question; `RR-11`
   decides the surface), where the exactly-once proof sits, where the evaluation table
   sits, where lineage (`RR-16`) sits if kept. Roughly 8 slides for 8 minutes.
4. **Trade-offs slide content.** Iceberg not a course technology (coverage doc Finding 1),
   Spark on the host not in a container, local LLM vs hosted, public-but-sensitive review
   text. Which three go on the slide.
5. **Q&A prep.** The audit's §6 red-teamed a prepared Q&A; which five questions get a
   written answer with a number in it.

**Resolution:** a section outline for the doc and a slide-by-slide outline, each entry
naming its figure. Not the doc, not the slides — those are execution.

## Input from RR-09 (closed 2026-09-04)

First three beats are fixed: (1) the opening question on screen, category-manager persona;
(2) the one line — *"N of M eligible products triggered at least one sustained-decline alert
during 2020–2023, using a rule developed on pre-2020 data and then applied unchanged"*;
(3) the hero product: two windows, adjusted theme shift, representative reviews. Then zoom
out: eligibility and yearly coverage, robustness grades, sensitivity variants, placebo
calibration and power. Three distinctions must appear in both doc and talk: alerts are not
causal proof; post-scan bootstrap intervals are descriptive unless the scan is calibrated;
the taxonomy is held out, or the aspect explanation is exploratory. The provenance result
(key collisions) is a Q&A answer, not a slide of its own. If the aspect half is unbuilt the
claim narrows to *"The completed result identifies sustained rating declines; aspect-level
explanation is preliminary."*
