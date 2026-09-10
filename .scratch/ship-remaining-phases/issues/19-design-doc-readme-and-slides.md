# 19 — The design doc, the README and the slides

**What to build:** the three written deliverables, all reading from the same spine so they
cannot contradict each other or the repo.

**Design doc: eight sections, two pages hard, links not appendices.** Its phase table is
written against `conf/lineage_chain.toml` — not against prose — and *is* the evaluation
table's spine. §7 carries P6's stated outcome including the star-only comparison.

**Talk: eight slides plus a title, 9:30 of a 10:00 ceiling** (5:00 slides + 4:40 demo +
20 s slack), shaped setup → one continuous demo → payoff.

- **Slide 2 ships two variants.** The fork is called at the **second rehearsal**, on the
  gold analytical line's printed `text_characterisable < 3` — not on whether P6 finished.
  Both variants are written in advance so the decision is not made under pressure.
- **Five prepared Q&A answers, each containing a number.**

**The four declines carry binding wording**, so a schedule cut is never dressed up as a
principled one: Kafka Connect ES sink says *dropped for time*; HDFS concedes MinIO is not
equivalent; Oozie is owned as an opinion; Sqoop and Pig keep the course's own citation.

**README describes the system that exists**, so what a grader reads matches what they can
run.

**Blocked by:** 01 (the table's spine) and 18 (the rehearsal that calls the slide-2 fork).

**Status:** ready-for-agent

- [ ] Design doc has all eight sections and fits two pages
- [ ] Its phase table is generated from or checked against `conf/lineage_chain.toml`
- [ ] §7 states P6's outcome including the star-only comparison
- [ ] Nine slides total, timed to 9:30 against the 10:00 ceiling
- [ ] Both slide-2 variants written; the fork rule names the printed number that decides it
- [ ] Five Q&A answers prepared, each containing a number
- [ ] The four declines appear in their binding wording
- [ ] README matches what `make` targets actually do
- [ ] No document states a number that `make eval-table` does not
