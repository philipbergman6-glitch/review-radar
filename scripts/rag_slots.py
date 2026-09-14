"""Which products P7's questions are about, drawn from the decline ranking (ticket 11, ADR-0006).

The thirty questions must concern the project's actual result, not a product that happens to
demo well. So the ten products in the two answerable families are not chosen: they are the top
five eligible decline candidates by the RR-09 ranking -- the bootstrap lower bound of
(baseline mean - recent mean) -- each paired with its nearest matched control by the frozen
matching distance.

Pairing every candidate with a control is the part that is easy to skip and expensive to lose.
A control is a product observed over the *same* calendar windows, similar in baseline rating and
volume, that never alerts. Asking the same two questions of both means a RAG answer that sounds
alarming about a declining product can be held against the answer it gives about a stable one.

This script reads and never writes the lake. It produces `eval/rag/slots.json`, pinned to the
gold and silver runs it read, and prints one `RAG_SLOTS` line. It does **not** decide
answerability -- that is `scripts/rag_evidence_scan.py`, and the slots it draws here are
provisional until the scan has confirmed every product in them.

Run:  ./run.sh python scripts/rag_slots.py            (or `make rag-slots`)
      ./run.sh python scripts/rag_slots.py --ranking-only   # print the ranking, draw nothing
"""
from __future__ import annotations

import argparse
import json
import time
from typing import Any

from pyspark.sql import functions as F

from src.ai.rag_questions import load_spec, rank_candidates, select_slots
from src.common import config as C
from src.common import runs
from src.common.config import PROJECT_ROOT
from src.common.console import line_buffered_stdout
from src.common.spark import CATALOG, build

line_buffered_stdout()

OUT_DIR = PROJECT_ROOT / "eval" / "rag"
SLOTS_PATH = OUT_DIR / "slots.json"
WALKED_PAST_PATH = OUT_DIR / "walked-past.json"
RANKING_PATH = OUT_DIR / "decline-ranking.json"

#: How far down the ranking the draw is allowed to walk. Not a relaxation ladder: every episode
#: it walks past is recorded with a reason, and the ranking order never changes. The cap exists
#: so a bug that rejects everything fails loudly instead of scanning 700 episodes.
MAX_RANKED_CONSIDERED = 60


def _table(name: str, scope: str) -> str:
    return f"{CATALOG}.{name}" + ("" if scope == "full" else f"_{scope}")


def load_episodes(spark, scope: str) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    """Every matched episode, plus its controls ordered by matching distance."""
    rows = [r.asDict() for r in spark.table(_table("gold.matched_controls", scope)).collect()]
    if not rows:
        raise RuntimeError("gold.matched_controls is empty: run `make theme-samples` first")
    controls: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        controls.setdefault(r["episode_id"], []).append(r)
    # One episode row per episode, taken from its nearest control so the window bounds and the
    # candidate's own window statistics come from a single row rather than an arbitrary one.
    episodes = [min(v, key=lambda c: c["control_rank"]) for v in controls.values()]
    return episodes, controls


def window_ratings(spark, scope: str, episodes: list[dict[str, Any]]) -> dict[str, dict[str, list[float]]]:
    """Every candidate's per-window review ratings, as plain lists the ranking can bootstrap.

    Pulled in one pass over silver rather than one query per episode: 700-odd episodes over a
    694k-row table is a single join, and 700 filtered scans is a coffee break.
    """
    wanted = spark.createDataFrame(
        [(e["episode_id"], e["candidate_asin"], e["baseline_start"], e["baseline_end"],
          e["recent_start"], e["recent_end"]) for e in episodes],
        "episode_id string, parent_asin string, baseline_start string, baseline_end string, "
        "recent_start string, recent_end string")
    silver = (spark.table(_table("silver.reviews", scope))
              .select("parent_asin", "rating", F.date_format("review_month", "yyyy-MM").alias("month")))
    joined = (silver.join(F.broadcast(wanted), "parent_asin")
              .withColumn("window",
                          F.when((F.col("month") >= F.col("baseline_start"))
                                 & (F.col("month") <= F.col("baseline_end")), F.lit("baseline"))
                           .when((F.col("month") >= F.col("recent_start"))
                                 & (F.col("month") <= F.col("recent_end")), F.lit("recent")))
              .filter(F.col("window").isNotNull())
              .select("episode_id", "window", "rating"))
    out: dict[str, dict[str, list[float]]] = {}
    for r in joined.collect():
        out.setdefault(r["episode_id"], {}).setdefault(r["window"], []).append(float(r["rating"]))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scope", default="full", choices=("full", "sample"))
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--ranking-only", action="store_true",
                    help="print and store the ranking without drawing slots")
    ap.add_argument("--reject-from", default=None,
                    help="eval/rag/validation.json: products manual validation could not "
                         "confirm. Removed from the eligible set, so the draw walks past them.")
    ap.add_argument("--eligible-from", default=None,
                    help="eval/rag/prefilter.json: restrict the draw to products whose every "
                         "window cleared the lexical pre-filter. The ranking is unchanged; the "
                         "draw walks past what the scan cannot confirm, recording each skip.")
    args = ap.parse_args()

    spec = load_spec()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    gold_run = runs.latest_success("gold", category=args.category, data_scope=args.scope)
    silver_run = runs.latest_success("silver", category=args.category, data_scope=args.scope)
    samples_run = runs.latest_success("theme_samples", category=args.category,
                                      data_scope=args.scope)
    for name, run in (("gold", gold_run), ("silver", silver_run), ("theme_samples", samples_run)):
        if run is None:
            raise RuntimeError(f"no successful {name} run at scope={args.scope}")

    spark = build("rag-slots")
    try:
        episodes, controls = load_episodes(spark, args.scope)
        ratings = window_ratings(spark, args.scope, episodes)
        titles = {r["parent_asin"]: r["product_title"] for r in
                  spark.table(_table("silver.reviews", args.scope))
                  .select("parent_asin", "product_title").filter(F.col("product_title").isNotNull())
                  .dropDuplicates(["parent_asin"]).collect()}
    finally:
        spark.stop()

    ranked = rank_candidates(episodes, ratings, spec)
    head = ranked[:MAX_RANKED_CONSIDERED]
    RANKING_PATH.write_text(json.dumps(
        {"spec_version": spec.version, "spec_hash": spec.spec_hash,
         "spec_status": spec.status, "scope": args.scope,
         "bootstrap_draws": spec.bootstrap_draws,
         "lower_bound_quantile": spec.lower_bound_quantile,
         "statistic": "percentile bootstrap lower bound of (baseline mean - recent mean), RR-09",
         "episodes_ranked": len(ranked), "episodes_stored": len(head),
         "gold_run_id": gold_run["run_id"], "silver_run_id": silver_run["run_id"],
         "theme_samples_run_id": samples_run["run_id"],
         "ranking": [{k: e[k] for k in ("decline_rank", "episode_id", "candidate_asin",
                                        "point_month", "baseline_start", "baseline_end",
                                        "recent_start", "recent_end", "decline_lower_bound",
                                        "baseline_n", "recent_n")} for e in head]},
        indent=2, sort_keys=True) + "\n")
    print(f"RAG_RANKING episodes={len(ranked)} stored={len(head)} "
          f"top_lb={head[0]['decline_lower_bound'] if head else None} "
          f"spec_hash={spec.spec_hash[:12]} path={RANKING_PATH.relative_to(PROJECT_ROOT)}")

    if args.ranking_only:
        return

    eligible = None
    prefilter_note = None
    if args.eligible_from:
        pf = json.loads((PROJECT_ROOT / args.eligible_from).read_text())
        if pf["spec_hash"] != spec.spec_hash:
            raise RuntimeError("the pre-filter was computed under a different spec hash")
        eligible = set(pf["eligible_products"])
        prefilter_note = {"source": args.eligible_from,
                          "min_lexical_matches": pf["min_lexical_matches"],
                          "products_considered": pf["products_considered"],
                          "eligible_products": len(eligible)}
    rejected: list[str] = []
    if args.reject_from:
        val = json.loads((PROJECT_ROOT / args.reject_from).read_text())
        if val["spec_hash"] != spec.spec_hash:
            raise RuntimeError("the validation record was written under a different spec hash")
        rejected = sorted(val["rejected_products"])
        if eligible is None:
            raise RuntimeError("--reject-from narrows an eligible set; pass --eligible-from too")
        eligible -= set(rejected)

    draw = select_slots(head, controls, eligible=eligible)
    for s in draw["slots"]:
        s["product_title"] = titles.get(s["parent_asin"])
    doc = {"spec_version": spec.version, "spec_hash": spec.spec_hash, "spec_status": spec.status,
           "scope": args.scope, "category": args.category,
           "answerability_confirmed": False,
           "prefilter": prefilter_note,
           "rejected_by_validation": rejected,
           "note": "Provisional until scripts/rag_evidence_scan.py and its manual validation "
                   "confirm every product. The lexical pre-filter narrows; it never decides. "
                   "A product validation cannot confirm is dropped and the draw walks further "
                   "down the same ranking, every skip recorded.",
           "gold_run_id": gold_run["run_id"], "silver_run_id": silver_run["run_id"],
           "theme_samples_run_id": samples_run["run_id"],
           "slots": draw["slots"], "skipped": draw["skipped"],
           "complete": draw["complete"]}
    SLOTS_PATH.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")

    # Every product any draw has reached, with its windows. Kept because a redraw would otherwise
    # erase the reading that justified the descent: the evidence scan reads this file as well as
    # the current slots, so the validation census covers the whole walk and not just its end.
    walked = json.loads(WALKED_PAST_PATH.read_text())["products"] if WALKED_PAST_PATH.exists() else []
    known = {w["parent_asin"] for w in walked}
    for ep in head:
        by_asin = {ep["candidate_asin"]: None}
        by_asin.update({c["control_asin"]: c for c in controls.get(ep["episode_id"], [])})
        for asin in by_asin:
            reached = asin in rejected or any(s["parent_asin"] == asin for s in draw["slots"])
            if reached and asin not in known:
                walked.append({"parent_asin": asin, "episode_id": ep["episode_id"],
                               "decline_rank": ep["decline_rank"],
                               "outcome": "rejected_by_validation" if asin in rejected else "drawn",
                               **{k: ep[k] for k in ("baseline_start", "baseline_end",
                                                     "recent_start", "recent_end")}})
                known.add(asin)
    WALKED_PAST_PATH.write_text(json.dumps(
        {"spec_hash": spec.spec_hash,
         "note": "Every product a draw has reached, whether it was kept or walked past. The "
                 "evidence scan reads this as well as the current slots, so the validation "
                 "census records the whole walk rather than only where it stopped.",
         "products": sorted(walked, key=lambda w: (w["decline_rank"], w["parent_asin"]))},
        indent=2, sort_keys=True) + "\n")
    print(f"RAG_SLOTS candidates={draw['candidates_selected']} products={len(draw['slots'])} "
          f"complete={str(draw['complete']).lower()} skipped={len(draw['skipped'])} "
          f"elapsed_s={round(time.time() - t0, 1)} path={SLOTS_PATH.relative_to(PROJECT_ROOT)}")
    if not draw["complete"]:
        raise SystemExit("the draw did not reach five candidates with distinct controls; "
                         "the fallback rule in ADR-0006 applies and must be recorded")


if __name__ == "__main__":
    main()
