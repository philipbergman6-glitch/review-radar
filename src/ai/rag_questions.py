"""The frozen P7 question set: slot selection, evidence scan, answer keys (ADR-0006, RR-17).

P7 is evaluated once, on thirty questions that must exist *before* any answer does. Everything
in this module is pure -- plain rows in, plain rows out, no Spark, no Elasticsearch, no Ollama --
so the draw that picks the questions is a function of the data and the frozen configuration,
and a test can exercise it without a lake.

Three rules shape the whole file, and each one is easier to violate than to notice:

* **Answerability is never decided by the retriever.** It is decided by an *evidence scan*:
  lexical candidate discovery with a frozen per-question term list over every silver review in
  scope, followed by manual semantic validation of what the scan matched. Deciding it from
  retriever output would grade the retriever twice -- once as the thing under test and once as
  the thing that defined the test.
* **Slots come from the decline ranking, not from taste.** The top five eligible candidate
  episodes by bootstrap lower bound of (baseline mean - recent mean), each with its nearest
  matched control. A question about a product picked because it demos well is a question about
  the demo.
* **Answer keys forbid prevalence and direction claims.** A retrieved set of ten reviews is not
  a sample of anything. "Three of the cited reviews mention leaking" is supportable; "leaking
  complaints increased" is not, and the quantitative theme-shift claim stays where it is
  computed -- the gold table.

The public surface, in the order the pipeline uses it:

  `load_spec`            read and hash `conf/rag-question-spec.json`
  `bootstrap_lower_bound` the RR-09 ranking statistic for one episode
  `rank_candidates`      ranked eligible episodes, deterministic to the tie-break
  `select_slots`         top five candidates + nearest distinct control each
  `scan_terms` / `scan_review`  the lexical half of the evidence scan
  `build_questions`      slots + validated evidence -> the thirty questions
  `check_manifest`       the invariants a frozen manifest must satisfy
"""
from __future__ import annotations

import hashlib
import json
import random
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.common.config import PROJECT_ROOT

SPEC_PATH = PROJECT_ROOT / "conf" / "rag-question-spec.json"
MANIFEST_PATH = PROJECT_ROOT / "conf" / "rag-questions.json"

#: Question families. The two answerable families ask about the same ten products, so a
#: product that is easy to talk about cannot inflate one family without inflating the other.
FAMILIES = ("product_scoped", "temporal")

#: The three unanswerable strata and their fixed sizes (RR-17). Reported beside the abstention
#: rate, never pooled: refusing an out-of-domain question is a different skill from noticing
#: that a real product has nothing to say in a real window.
UNANSWERABLE_STRATA = {"absent_attribute": 4, "zero_review_scope": 3, "out_of_domain": 3}

SLOT_ROLES = ("candidate", "control", "filler")

#: ADR-0006's four forbidden claim classes, written once so every key carries the same words.
FORBIDDEN_CLAIMS = (
    "prevalence: how many or what share of this product's reviews complain about something",
    "direction: that a complaint increased, decreased, became more common, or started",
    "representativeness: that the cited reviews are typical of the product or the corpus",
    "causation: that one thing caused another, including a decline in rating",
)

ANSWERABLE_MIN_SUPPORT = 3          # validated complaint-bearing reviews, per scope or window
UNANSWERABLE_MAX_SUPPORT = 0        # the 1-2 band is excluded from the set entirely


# ------------------------------------------------------------------------ spec ----
@dataclass(frozen=True)
class QuestionSpec:
    """`conf/rag-question-spec.json`, plus the hash stamped on everything drawn from it."""
    version: str
    status: str
    seed: int
    bootstrap_draws: int
    lower_bound_quantile: float
    templates: dict[str, str]
    scan_terms: dict[str, list[str]]
    absent_attributes: list[dict[str, Any]]
    out_of_domain: list[dict[str, Any]]
    refusal_reasons: dict[str, str]
    spec_hash: str
    raw: dict[str, Any] = field(repr=False, default_factory=dict)

    @property
    def frozen(self) -> bool:
        return self.status == "frozen"


def load_spec(path: Path = SPEC_PATH) -> QuestionSpec:
    doc = json.loads(path.read_text())
    if doc.get("status") not in ("provisional", "frozen"):
        raise ValueError(f"status must be provisional|frozen, got {doc.get('status')!r}")
    if int(doc["bootstrap_draws"]) < 100:
        raise ValueError("bootstrap_draws below 100 cannot support a 95% lower bound")
    if not 0.0 < float(doc["lower_bound_quantile"]) < 0.5:
        raise ValueError("lower_bound_quantile must be strictly between 0 and 0.5")
    for family in FAMILIES:
        if family not in doc["templates"]:
            raise ValueError(f"templates is missing the {family} family")
    if not doc["scan_terms"]:
        raise ValueError("scan_terms is empty: an evidence scan with no terms proves nothing")
    for theme, terms in doc["scan_terms"].items():
        if not terms:
            raise ValueError(f"scan_terms[{theme}] is empty")
    absent = doc["absent_attribute"]["probes"]
    ood = doc["out_of_domain"]["probes"]
    if len(absent) < UNANSWERABLE_STRATA["absent_attribute"]:
        raise ValueError("not enough absent_attribute probes to fill the stratum")
    if len(ood) < UNANSWERABLE_STRATA["out_of_domain"]:
        raise ValueError("not enough out_of_domain probes to fill the stratum")
    for probe in [*absent, *ood]:
        if not probe.get("terms"):
            raise ValueError(f"probe {probe.get('id')!r} carries no scan terms")
    for stratum in UNANSWERABLE_STRATA:
        if stratum not in doc["refusal_reasons"]:
            raise ValueError(f"refusal_reasons is missing the {stratum} stratum")
        if stratum not in doc["templates"]:
            raise ValueError(f"templates is missing the {stratum} stratum")
    # Hashed over everything that can change a question, and nothing else. `note` and
    # `decided_in` are prose; a comment rewrite must not look like a protocol change.
    payload = {"version": doc["version"], "seed": doc["seed"],
               "bootstrap_draws": doc["bootstrap_draws"],
               "lower_bound_quantile": doc["lower_bound_quantile"],
               "templates": doc["templates"], "scan_terms": doc["scan_terms"],
               "absent_attribute": absent, "out_of_domain": ood,
               "refusal_reasons": doc["refusal_reasons"]}
    spec_hash = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return QuestionSpec(version=str(doc["version"]), status=doc["status"], seed=int(doc["seed"]),
                        bootstrap_draws=int(doc["bootstrap_draws"]),
                        lower_bound_quantile=float(doc["lower_bound_quantile"]),
                        templates=doc["templates"], scan_terms=doc["scan_terms"],
                        absent_attributes=absent, out_of_domain=ood,
                        refusal_reasons=doc["refusal_reasons"], spec_hash=spec_hash, raw=doc)


# --------------------------------------------------------------------- ranking ----
def bootstrap_lower_bound(baseline: list[float], recent: list[float], *, seed: int,
                          draws: int, quantile: float) -> float | None:
    """Lower bound of (mean(baseline) - mean(recent)), by percentile bootstrap.

    RR-09 ranks decline candidates by this rather than by the raw difference, because a
    two-star drop over four reviews and a two-star drop over four hundred are not the same
    finding, and the raw difference cannot tell them apart. Returns `None` when either window
    is empty -- an episode with no ratings in a window is not rankable, and inventing a zero
    would rank it *above* a real but uncertain decline.

    Deterministic in `seed` alone: the caller derives the seed from the episode id, so the
    ranking never depends on the order episodes happened to arrive in.
    """
    if not baseline or not recent:
        return None
    rng = random.Random(seed)
    nb, nr = len(baseline), len(recent)
    diffs = []
    for _ in range(draws):
        b = sum(baseline[rng.randrange(nb)] for _ in range(nb)) / nb
        r = sum(recent[rng.randrange(nr)] for _ in range(nr)) / nr
        diffs.append(b - r)
    diffs.sort()
    # Nearest-rank percentile: no interpolation, so the value is always an observed resample.
    idx = min(len(diffs) - 1, max(0, int(quantile * len(diffs))))
    return diffs[idx]


def episode_seed(spec_seed: int, episode_id: str) -> int:
    """A per-episode bootstrap seed, so each episode's resamples are its own."""
    h = hashlib.sha256(f"{spec_seed}:{episode_id}".encode()).hexdigest()
    return int(h[:16], 16)


def rank_candidates(episodes: list[dict[str, Any]], ratings: dict[str, dict[str, list[float]]],
                    spec: QuestionSpec) -> list[dict[str, Any]]:
    """Eligible candidate episodes, ranked by the RR-09 statistic, descending.

    `episodes` are rows of `gold.matched_controls` at `control_rank = 1` -- already
    text-characterisable and already matched, which is what "eligible" means before
    answerability is scanned. `ratings[episode_id]` carries `{"baseline": [...], "recent": [...]}`.

    Ties break on `candidate_asin` then `episode_id`, both ascending, so the order is fixed by
    the data rather than by Spark partitioning or dictionary insertion.
    """
    scored = []
    for ep in episodes:
        r = ratings.get(ep["episode_id"], {})
        lb = bootstrap_lower_bound(r.get("baseline", []), r.get("recent", []),
                                   seed=episode_seed(spec.seed, ep["episode_id"]),
                                   draws=spec.bootstrap_draws,
                                   quantile=spec.lower_bound_quantile)
        if lb is None:
            continue
        scored.append({**ep, "decline_lower_bound": round(lb, 6),
                       "baseline_n": len(r.get("baseline", [])),
                       "recent_n": len(r.get("recent", []))})
    scored.sort(key=lambda e: (-e["decline_lower_bound"], e["candidate_asin"], e["episode_id"]))
    for i, e in enumerate(scored, start=1):
        e["decline_rank"] = i
    return scored


def select_slots(ranked: list[dict[str, Any]], controls: dict[str, list[dict[str, Any]]],
                 *, eligible: set[str] | None = None, n: int = 5) -> dict[str, Any]:
    """Walk the ranking and take the first `n` candidates that can carry a distinct control.

    `controls[episode_id]` is that episode's matched controls ordered by `control_rank`, the
    frozen matching distance. The *nearest* control is taken; if it is already in use by a
    higher-ranked candidate, the next-nearest is, because the ten products must be distinct --
    a product answering two questions in the same family would be counted twice.

    `eligible` is the set of products the evidence scan has cleared -- lexically at the
    pre-filter stage, and by manual semantic validation at the freeze. It is optional so the
    ranking can be inspected before any scan runs, but the frozen draw always passes it:
    ADR-0006 says *top five **eligible** candidates*, and a candidate the scan cannot confirm
    is not one. Descending past it is the protocol working, not a relaxation, which is why
    every skip is recorded with its reason and the ranking itself never changes.
    """
    used: set[str] = set()
    slots: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for ep in ranked:
        if len(slots) >= 2 * n:
            break
        cand = ep["candidate_asin"]
        if cand in used:
            skipped.append({"episode_id": ep["episode_id"], "reason": "candidate_already_used"})
            continue
        if eligible is not None and cand not in eligible:
            skipped.append({"episode_id": ep["episode_id"], "reason": "candidate_not_eligible"})
            continue
        pick = None
        for ctl in sorted(controls.get(ep["episode_id"], []), key=lambda c: c["control_rank"]):
            if ctl["control_asin"] in used or ctl["control_asin"] == cand:
                continue
            if eligible is not None and ctl["control_asin"] not in eligible:
                continue
            pick = ctl
            break
        if pick is None:
            skipped.append({"episode_id": ep["episode_id"], "reason": "no_distinct_eligible_control"})
            continue
        used.update({cand, pick["control_asin"]})
        common = {k: ep[k] for k in ("episode_id", "point_month", "baseline_start", "baseline_end",
                                     "recent_start", "recent_end", "decline_rank",
                                     "decline_lower_bound")}
        slots.append({**common, "parent_asin": cand, "slot_role": "candidate",
                      "match_distance": None, "control_rank": None})
        slots.append({**common, "parent_asin": pick["control_asin"], "slot_role": "control",
                      "match_distance": round(float(pick["baseline_mean_diff"]), 6),
                      "control_rank": int(pick["control_rank"])})
    return {"slots": slots, "skipped": skipped,
            "complete": len(slots) == 2 * n,
            "candidates_selected": sum(1 for s in slots if s["slot_role"] == "candidate")}


# ---------------------------------------------------------------- evidence scan ----
def _term_pattern(term: str) -> re.Pattern[str]:
    # Word-bounded so "work" does not match "network", but phrases keep their internal spaces
    # flexible, because review text is full of double spaces and line breaks.
    parts = [re.escape(w) for w in term.lower().split()]
    return re.compile(r"\b" + r"\s+".join(parts) + r"\b")


def compile_terms(term_lists: dict[str, list[str]]) -> dict[str, list[re.Pattern[str]]]:
    """Compile a `{group: [term, ...]}` map into the patterns `scan_review` takes."""
    return {group: [_term_pattern(t) for t in terms] for group, terms in term_lists.items()}


def scan_terms(spec: QuestionSpec, themes: list[str] | None = None) -> dict[str, list[re.Pattern[str]]]:
    """Compile the frozen theme term lists. `themes=None` means every theme."""
    chosen = spec.scan_terms if themes is None else {t: spec.scan_terms[t] for t in themes}
    return compile_terms(chosen)


def probe_regex(terms: list[str]) -> str:
    """The same word-bounded matching as `scan_review`, as one Java-compatible regex.

    Used where the scan has to run inside Spark rather than in the driver -- an out-of-domain
    probe is checked against all 694k silver reviews, and collecting those to Python to run the
    identical Python regex would move the corpus to prove it says nothing.
    """
    return "|".join(r"\b" + r"\s+".join(re.escape(w) for w in t.lower().split()) + r"\b"
                    for t in terms)


def scan_review(text: str, patterns: dict[str, list[re.Pattern[str]]]) -> dict[str, list[str]]:
    """Which themes' frozen terms this review's text matches, and which terms matched.

    Lexical only, and deliberately generous. A match makes a review a *candidate* for manual
    semantic validation; it never makes it evidence. Regex alone proves nothing -- "this does
    not leak, unlike the last one" matches the leak terms and is praise.
    """
    hay = " " + (text or "").lower() + " "
    out: dict[str, list[str]] = {}
    for theme, pats in patterns.items():
        hits = sorted({p.pattern for p in pats if p.search(hay)})
        if hits:
            out[theme] = hits
    return out


def answerability(validated: list[dict[str, Any]], *, windows: list[str]) -> dict[str, Any]:
    """The frozen answerability verdict over already-validated evidence.

    `validated` rows carry `window` and `complaint_bearing`. Product-scoped questions pass one
    window; temporal questions pass two and must clear the bar in *each*, because an answer
    that contrasts a well-evidenced window against an empty one is not a contrast.

    The 1-2 band is excluded on purpose: a question with two supporting reviews is neither
    safely answerable nor honestly unanswerable, and putting it in either bucket makes the
    denominator lie.
    """
    per_window = {w: sum(1 for v in validated
                         if v["window"] == w and v["complaint_bearing"]) for w in windows}
    lo = min(per_window.values()) if per_window else 0
    if lo >= ANSWERABLE_MIN_SUPPORT:
        verdict = "answerable"
    elif max(per_window.values(), default=0) <= UNANSWERABLE_MAX_SUPPORT:
        verdict = "unanswerable"
    else:
        verdict = "excluded_band"
    return {"verdict": verdict, "validated_per_window": per_window,
            "min_window_support": lo}


# -------------------------------------------------------------- question build ----
def question_id(family: str, index: int) -> str:
    return f"{family}-{index:02d}"


def _key(required: list[str], themes: list[str], supporting: list[dict[str, Any]],
         refusal: str | None) -> dict[str, Any]:
    return {"required_propositions": required,
            "acceptable_themes": sorted(set(themes)),
            "supporting_review_ids": supporting,
            "forbidden_claims": list(FORBIDDEN_CLAIMS),
            "expected_refusal_reason": refusal}


def check_manifest(manifest: dict[str, Any]) -> list[str]:
    """Every invariant a frozen question set must satisfy, as a list of violations.

    Returned rather than raised so the caller can print all of them at once; the freeze script
    refuses to write on a non-empty list. This is the function the tests exercise, and each
    rule here has a test that trips it -- a check that cannot fail is not a check.
    """
    v: list[str] = []
    qs = manifest.get("questions", [])
    if len(qs) != 30:
        v.append(f"expected 30 questions, found {len(qs)}")
    ids = [q["question_id"] for q in qs]
    if len(set(ids)) != len(ids):
        v.append("question ids are not unique")

    answerable = [q for q in qs if q["answerability"] == "answerable"]
    unanswerable = [q for q in qs if q["answerability"] == "unanswerable"]
    if len(answerable) != 20:
        v.append(f"expected 20 answerable questions, found {len(answerable)}")
    if len(unanswerable) != 10:
        v.append(f"expected 10 unanswerable questions, found {len(unanswerable)}")

    for family in FAMILIES:
        n = sum(1 for q in answerable if q["family"] == family)
        if n != 10:
            v.append(f"expected 10 answerable {family} questions, found {n}")
    for stratum, size in UNANSWERABLE_STRATA.items():
        n = sum(1 for q in unanswerable if q.get("stratum") == stratum)
        if n != size:
            v.append(f"expected {size} {stratum} questions, found {n}")

    # The same ten products in both answerable families: a product easy to talk about cannot
    # inflate one family alone, and the two families stay comparable question for question.
    by_family = {f: {q["parent_asin"] for q in answerable if q["family"] == f} for f in FAMILIES}
    if by_family[FAMILIES[0]] != by_family[FAMILIES[1]]:
        v.append("the two answerable families do not cover the same ten products")
    for family, products in by_family.items():
        if len(products) != 10:
            v.append(f"{family} covers {len(products)} distinct products, expected 10")

    for q in qs:
        key = q.get("answer_key", {})
        if list(key.get("forbidden_claims", [])) != list(FORBIDDEN_CLAIMS):
            v.append(f"{q['question_id']}: answer key does not carry the frozen forbidden claims")
        if not q.get("scope", {}).get("windows"):
            v.append(f"{q['question_id']}: no scope windows declared")
        if q["answerability"] == "answerable":
            support = {w: 0 for w in q["scope"]["windows"]}
            for s in key.get("supporting_review_ids", []):
                support[s["window"]] = support.get(s["window"], 0) + 1
            for w, n in support.items():
                if n < ANSWERABLE_MIN_SUPPORT:
                    v.append(f"{q['question_id']}: window {w} carries {n} supporting reviews, "
                             f"needs {ANSWERABLE_MIN_SUPPORT}")
            if not key.get("required_propositions"):
                v.append(f"{q['question_id']}: answerable question has no required propositions")
            if key.get("expected_refusal_reason") is not None:
                v.append(f"{q['question_id']}: answerable question carries a refusal reason")
        else:
            if key.get("supporting_review_ids"):
                v.append(f"{q['question_id']}: unanswerable question carries supporting reviews")
            if not key.get("expected_refusal_reason"):
                v.append(f"{q['question_id']}: unanswerable question has no expected refusal reason")
            if key.get("required_propositions"):
                v.append(f"{q['question_id']}: unanswerable question carries required propositions")
        if q.get("slot_role") not in SLOT_ROLES:
            v.append(f"{q['question_id']}: slot_role {q.get('slot_role')!r} is not a frozen role")
    return v
