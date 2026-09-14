# Review Radar — the talk

Eight slides plus a title, one continuous demo between them. **9:30 of the brief's 10:00
ceiling**, then up to 3 minutes of questions. The deck is written here so the timing claim is
checkable: `tests/test_written_deliverables.py` fails if the running order stops adding up.

Structure is **setup (5) → demo → payoff (3)**, so every demo move lands as proof of a claim
made thirty seconds earlier rather than as a tour. Demo-last was considered and rejected: the
recorded backup under [`docs/demo/`](demo) and two passing rehearsals already buy that
insurance, and buying it twice costs the narrative.

## The running order

| # | slide | s | the figure it carries |
|---|---|---|---|
| 0 | Title | 10 | Review Radar · Amazon Reviews 2023, `All_Beauty` · 701,528 reviews |
| 1 | The question, and who asks it | 30 | the question verbatim; a category manager; 701,528 reviews / 112,590 products / 2000-11-01 → 2023-09-09 |
| 2 | The answer, in one line | 40 | **two variants, see below** — the N-of-M line and the hero product's two windows |
| 3 | The data, and which V's | 30 | 311 MB → 96.5 MB (3.2×) · 232k–484k rec/s · free-form `details` + Postgres catalogue · 6,139 collision groups, 0 rating conflicts |
| 4 | Architecture and data flow | 40 | the diagram; the exactly-once proof as a figure (120,000 expected, 89,964 before the kill, 0 duplicate offsets); the run ledger labelled |
| 5 | The AI capability, and why this one | 35 | deck 5's own `BM25 does not consider the semantic meaning`; embeddings + client-side RRF; ANN recall@10 0.96; H-E1 not held |
| — | **LIVE DEMO** — ten moves, one kernel | 280 | `demo.run_sheet()`; 260 s of moves + 20 s for the two surface switches |
| 6 | How we avoided fooling ourselves | 40 | ADR-0001's holdout + protocol freeze; ADR-0011's split; the audit set opened once |
| 7 | Results, zoomed out | 35 | 139 of 502 eligible products alerted in the 2020–2023 holdout; 425 episodes overall, closed 207 / 165 / 53; detection power 0.186 vs bar 0.80; macro-F1 0.4583 vs the star-only floor 0.2968 |
| 8 | Trade-offs, and what we declined | 30 | three trade-offs, four declines |

**290 s of slides + 280 s of demo = 570 s, 9:30 of the 10:00 ceiling.**
The slides take 290 s, not the 300 s the outline first budgeted: the per-slide seconds it
proposed summed to 320 and the ceiling wins over the sketch. The 20 s of slack survives.

## Slide 2, and the fork

Slide 2 is the only slide whose content is not fixed in advance, so **both variants are
written now** and the choice is made by reading a printed number, not by judging the mood in
the room.

### Slide 2, variant A — the wide claim

> **139 of 502 eligible products** raised at least one sustained rating decline during
> **2020–2023**, under a rule developed on pre-2020 data and applied unchanged — 148 alerts,
> 148 episodes — and the complaint themes behind them are labelled from the review text.
>
> The rule was sealed before a single holdout point was evaluated. It raises **0.80 false
> alerts a month** against a budget of one, and catches **18.6%** of declines its own size —
> so this is a floor, not a census.
>
> Hero: `B00RPJZMUM`, decline rank 3 — baseline 2016-10…2017-03 against recent
> 2017-04…2017-09; `hard_to_use` and `not_as_described` give way to `does_not_work`
> (*"motor stopped working"*) and `poor_build_quality` (*"is no longer usable"*).
>
> *"— and here is why that number is defensible."*

### Slide 2, variant B — the narrow claim

> **The completed result identifies sustained rating declines; aspect-level explanation is
> preliminary.** 139 of 502 eligible products raised an alert during **2020–2023** under a rule
> frozen beforehand, at 0.80 false alerts a month and 18.6% detection — the decline claim
> stands, the theme claim is illustrative.
>
> Hero: `B00RPJZMUM` — the same two windows and the same theme shift, shown as an
> illustration over 18 baseline and 3 recent labelled mentions, not as an estimate.
>
> *"— and here is why that narrowness is the result."*

### The fork — which variant ships, and what decides it

Called at the **second rehearsal**, which `DEMO_GATE` timestamps. Two printed numbers, in this
order:

1. **The count of robust, text-characterisable decline candidates**, from the `theme_samples`
   ledger row for the audit frame: `candidate_episodes=964 matched_candidates=708
   dropped_not_text_characterisable=219 dropped_no_matching_control=37`. The rule is
   **`matched_candidates < 3` → variant B**. It reads **708**, so the fork itself does not
   narrow the claim — the sample is there.
2. **The published P6 number**, from `make eval-table`: `THEMES_QUALITY macro_f1=0.4583
   bar=0.70 → FAIL`, with `THEMES_AGREEMENT=NOT_RUN`. This is the second, independent guard:
   *the taxonomy is held out, or the aspect explanation is exploratory.* A labelling layer
   measured below its own pre-set bar cannot carry the word "explains".

**As called at the rehearsal of 2026-09-14: variant B ships.** The fork on candidate supply
did not fire; the guard on measured quality did. Both are numbers printed before the talk, and
recording which one narrowed the claim is the point of writing the rule down.

## Prepared Q&A

Five answers, each of them a number rather than a position. HDFS and Kafka Connect are
deliberately given no slot: slide 8 declines both out loud, so a prepared answer would spend
the argument twice.

**Q. Why Iceberg, when the course didn't teach it?**
**A.** Conceded up front — **one mention in ~860 slides**. It earns its place on mechanism, not
on syllabus: **3.2×** compaction (311 MB → 96.5 MB), a `run_id` in every snapshot summary, and
**5 snapshots** on the bronze table with `VERSION AS OF` returning **701,528** rows live at demo
move 4. It scores under pipeline design (25%), not course technologies (20%).

**Q. Show me a query where BM25 loses.**
**A.** Demo move 6, live: for *"nail polish chips after one day"* the fused winner is ranked
**6 by BM25 and 3 by kNN** — neither list puts it first, and the sum of 1/(60+rank) over both
legs does; **9 of the fused top 10** are in both legs. Across the frozen set, **ANN recall@10
0.96** against exact cosine, and **H-E1 is not held** — kNN alone does not beat BM25; the
hybrid is where the gain is.

**Q. One broker, one Elasticsearch node — what does CAP even mean here?**
**A.** Operationally, nothing: **replication factor 1**, one ES node, so there is no partition
to survive and I will not claim one. The honest answer names the settings that *would* decide
it — `acks=all` with `min.insync.replicas` ≥ 2 across ≥ 3 brokers. What the single broker does
prove is exactly-once *processing*: **120,000 expected, 89,964 before a `SIGKILL`, 120,000
after, 0 duplicate `(partition, offset)` pairs**.

**Q. Why a local 8B model, and doesn't that invalidate your 0.70 bar?**
**A.** `ANTHROPIC_API_KEY` is empty, so the labeller is local. `llama3.2:3b` measured
degenerate on a v0 prompt — all **8** candidate themes on **5 of 8** reviews, at **3.78
s/review**; `qwen3:8b` discriminates at **6.7 s/review**, and 3 client threads do not help
(6.08 s/review). The bar did **not** move: the result is **macro-F1 0.4583
[0.3647, 0.5223] against 0.70**, reported as a `FAIL` that blocks nothing. It is still
distinguishably above the star-only floor **0.2968 [0.2612, 0.3493]** — disjoint intervals —
which the MLlib classifier at **0.3899 [0.3074, 0.4639]** is not.

**Q. Your 6,139 collisions — duplicates, or genuine repeat reviews?**
**A.** **6,139 groups over 13,415 rows: 6,138 byte-exact, 1 conflicting on `helpful_vote`, and
0 conflicting on rating**, so **7,276** rows were removed keeping one survivor each. Zero
rating disagreements is the evidence they are one review recorded twice, not two opinions. The
P8 sort re-derives all of it from the raw file sharing no code with silver.

**Reserves, not written up:** *"why stream a static file"* (nothing downstream of Kafka
changes — demo move 2 answers it visually) and *"schema evolution — show me an example."*

## Slide 8 — the wording that binds

Three trade-offs: **Iceberg is not a course technology** (conceded, one mention in ~860
slides); **a local `qwen3:8b` instead of hosted Haiku** with the 0.70 bar deliberately unmoved;
**public but sensitive review text** — only title and text cross the wire, never `user_id`s.

Four declines, said out loud, in these words:

- **Kafka Connect Elasticsearch sink — dropped for time.** Not argued from the deck's
  `Except for a trivial "file" connector` line: that covers a Connect *source*, and the deck
  separately names `Kafka Connect connectors for JDBC, HDFS, S3, and Elasticsearch`. A sink is
  a real, course-named use of Connect. This was the schedule, not the merits.
- **Single-node HDFS — declined on resources, and MinIO is not equivalent.** Block replication
  and rack awareness against a flat key space with no atomic rename — which is precisely why
  PostgreSQL holds the Iceberg catalogue.
- **Sqoop and Pig — on the course's own slide:**
  `Apache Sqoop moved into the Attic in June 2021`; Spark SQL does Pig's transform.
- **Oozie — this one is an opinion, and I own it as one.** Spark subsumes the multistage
  orchestration and the run ledger fills the role, but Oozie is not retired and the decks teach
  it across 25 mentions. GraphFrames/GraphX are declined the same way, as a detour.
