"""Independent pandas reproduction of silver (ADR-0007 consequences, CONTEXT.md "Independent
reproduction").

Re-derives silver from the raw review JSONL and the raw metadata JSONL using only the
specification -- the ADR text and the canonical-encoding description -- and compares the
result, row by row, with the three tables the latest successful `silver` run pinned in the
run ledger. Deliberately imports nothing from src/spark/silver.py or src/common/canonical.py;
Spark is used only to *read* the pinned Iceberg snapshots into pandas.

What must agree exactly:
  * raw line count == bronze rows the run read;
  * reject rows per reason;
  * the set of surviving review_ids, and per survivor: rating, helpful_vote, word count,
    review_month, product_title, main_category, store, price;
  * the set of collision rows (group id, row hash), each row's class and survivor flag.

Prints one SILVER_REPRO line per check and a final SILVER_REPRO_GATE; exit 0 on PASS. The
verdict itself is pure (`src/gates/silver.py:repro`), and is published to
`eval/silver_repro/gate.json` for `make eval-table`.

Run:  ./run.sh python scripts/reproduce_silver.py [--scope full|sample]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd

from src.common import config as C
from src.common import evaluation as E
from src.common import runs
from src.common.console import line_buffered_stdout
from src.common.spark import build
from src.gates import lineage as L
from src.gates import silver as gate

line_buffered_stdout()

TS_MIN_MS = int(datetime(1995, 1, 1, tzinfo=UTC).timestamp() * 1000)
INT_RE = re.compile(r"^-?[0-9]{1,19}$")
PRICE_RE = re.compile(r"^\$?([0-9]{1,3}(?:,[0-9]{3})*|[0-9]+)(?:\.([0-9]{1,2}))?$")
REASONS = ("unparsable_json", "missing_key_field", "invalid_rating", "timestamp_out_of_range")


# ---------------------------------------------------------------- encoding ----
# Written from the words in ADR-0007 §4: fixed field order, 4-byte big-endian UTF-8 byte
# length then the bytes, null = 0xFFFFFFFF, empty = length 0.
def _field(v: str | None) -> bytes:
    if v is None:
        return b"\xff\xff\xff\xff"
    b = v.encode("utf-8")
    return len(b).to_bytes(4, "big") + b


def sha(*fields: str | None) -> str:
    return hashlib.sha256(b"".join(_field(f) for f in fields)).hexdigest()


def images_text(images: Any) -> str | None:
    if images is None:
        return None
    cleaned = [{k: v for k, v in sorted(img.items()) if v is not None} for img in images]
    return json.dumps(cleaned, separators=(",", ":"), ensure_ascii=False)


def bool_text(v: Any) -> str | None:
    return None if v is None else ("true" if v else "false")


def int_text(v: Any) -> str | None:
    return None if v is None else str(int(v))


# -------------------------------------------------------------- validation ----
def _blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


def _int_like(v: Any) -> int | None:
    """The JSON value as the integer Spark would parse from its string form, else None."""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and INT_RE.match(v.strip()) and v == v.strip():
        return int(v)
    return None


def _rating_ok(v: Any) -> bool:
    if isinstance(v, bool) or v is None:
        return False
    try:
        f = float(v)
    except (TypeError, ValueError):
        return False
    return 1 <= f <= 5 and f == int(f)


def reject_reason(line: str, bound_ms: int) -> tuple[str | None, dict[str, Any] | None]:
    try:
        d = json.loads(line)
    except ValueError:
        return "unparsable_json", None
    if not isinstance(d, dict):
        return "unparsable_json", None
    ts_raw = d.get("timestamp")
    if _blank(d.get("user_id")) or _blank(d.get("parent_asin")) or _blank(ts_raw):
        return "missing_key_field", d
    if not _rating_ok(d.get("rating")):
        return "invalid_rating", d
    ts = _int_like(ts_raw)
    if ts is None or ts < TS_MIN_MS or ts > bound_ms:
        return "timestamp_out_of_range", d
    return None, d


def _opt_str(v: Any) -> str | None:
    return None if v is None else str(v)


def valid_row(d: dict[str, Any], lineno: int) -> dict[str, Any]:
    user_id, parent_asin = str(d["user_id"]).strip(), str(d["parent_asin"]).strip()
    asin = None if _blank(d.get("asin")) else str(d["asin"]).strip()
    ts = _int_like(d["timestamp"])
    rating = int(float(d["rating"]))
    hv = d.get("helpful_vote")
    hv = None if hv is None else int(float(hv))
    title, text = _opt_str(d.get("title")), _opt_str(d.get("text"))
    verified = d.get("verified_purchase")
    images = d.get("images")
    return {
        "lineno": lineno, "review_id": sha(user_id, parent_asin, str(ts)),
        "row_hash": sha(str(rating), title, text, bool_text(verified), int_text(hv), asin,
                        images_text(images)),
        "parent_asin": parent_asin, "rating": rating, "title": title, "text": text,
        "verified": verified, "helpful_vote": hv, "asin": asin, "images_text": images_text(images),
        "text_len": 0 if text is None else len(text),
        "word_count": 0 if text is None else len(text.split()),
        "review_month": datetime.fromtimestamp(ts / 1000, tz=UTC).strftime("%Y-%m-01"),
    }


# --------------------------------------------------------------- collisions ----
RETAINED = ("rating", "title", "text", "verified", "helpful_vote", "asin", "images_text")


def classify(valid: pd.DataFrame) -> pd.DataFrame:
    """Add group_size, collision_class, is_survivor, selection_rank."""
    df = valid.copy()
    df["group_size"] = df.groupby("review_id")["review_id"].transform("size")
    # Sort key: helpful votes desc (nulls last), text length desc, row hash asc.
    df["_hv"] = df["helpful_vote"].fillna(-1).astype("int64")
    df = df.sort_values(["review_id", "_hv", "text_len", "row_hash"],
                        ascending=[True, False, False, True], kind="mergesort")
    df["selection_rank"] = df.groupby("review_id").cumcount() + 1

    def cls(g: pd.DataFrame) -> str | None:
        if len(g) == 1:
            return None
        differing = [f for f in RETAINED if g[f].astype(object).nunique(dropna=False) > 1]
        if "rating" in differing:
            return "unresolvable"
        return "exact" if not differing else "conflicting"

    multi = df[df["group_size"] > 1]
    classes = multi.groupby("review_id", sort=False).apply(cls, include_groups=False)
    df["collision_class"] = df["review_id"].map(classes)
    df["is_survivor"] = (df["selection_rank"] == 1) & (df["collision_class"] != "unresolvable")
    return df.drop(columns="_hv")


# ---------------------------------------------------------------- catalogue ----
def parse_price(raw: Any) -> Decimal | None:
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        try:
            return Decimal(str(raw)).quantize(Decimal("0.01"))
        except InvalidOperation:
            return None
    m = PRICE_RE.match(str(raw).strip())
    if not m:
        return None
    return Decimal(m.group(1).replace(",", "") + ("." + m.group(2) if m.group(2) else "")).quantize(Decimal("0.01"))


def read_catalogue(path: Path) -> pd.DataFrame:
    rows = []
    with path.open() as f:
        for line in f:
            d = json.loads(line)
            rows.append({"parent_asin": str(d["parent_asin"]).strip(), "product_title": str(d["title"]),
                         "main_category": d.get("main_category"), "store": d.get("store"),
                         "price": parse_price(d.get("price"))})
    df = pd.DataFrame(rows)
    if df["parent_asin"].duplicated().any():
        raise RuntimeError("metadata has duplicate parent_asin; the join would fan out")
    return df.set_index("parent_asin")


# ------------------------------------------------------------- spark reads ----
def read_pinned(spark, outputs: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    def t(name: str):
        o = outputs[name]
        return spark.read.option("snapshot-id", o["snapshot_id"]).table(o["table"])
    reviews = t("silver.reviews").select(
        "review_id", "rating", "helpful_vote", "text_word_count", "review_month",
        "product_title", "main_category", "store", "price").toPandas()
    rejects = t("silver.rejects").groupBy("reject_reason").count().toPandas()
    colls = t("silver.review_collisions").select(
        "collision_group_id", "canonical_row_hash", "collision_class", "is_survivor",
        "selection_rank").toPandas()
    return reviews, rejects, colls


# ------------------------------------------------------------------- main ----
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--reviews", default=None, help="raw review JSONL (default from --scope)")
    ap.add_argument("--meta", default=None, help="raw metadata JSONL (default: full category file)")
    args = ap.parse_args()
    reviews_path = Path(args.reviews or (
        f"data/raw/{args.category}.jsonl" if args.scope == "full"
        else f"data/sample/{args.category}.sample.jsonl"))
    meta_path = Path(args.meta or f"data/raw/meta_{args.category}.jsonl")

    row = runs.latest_success("silver", category=args.category, data_scope=args.scope)
    if row is None:
        sys.exit(f"no successful silver run for {args.category}/{args.scope}: SILVER_REPRO_GATE=FAIL")
    # Upper timestamp bound: silver used each row's Kafka ingestion time; that is not in the
    # raw file, so the run's own start time stands in for it (later than every ingestion).
    bound_ms = int(row["started_at"].timestamp() * 1000)

    # ---- independent derivation ----
    reason_counts = {r: 0 for r in REASONS}
    valid_rows: list[dict[str, Any]] = []
    raw_lines = 0
    with reviews_path.open() as f:
        for lineno, line in enumerate(f):
            raw_lines += 1
            reason, d = reject_reason(line, bound_ms)
            if reason:
                reason_counts[reason] += 1
            else:
                valid_rows.append(valid_row(d, lineno))
    valid = classify(pd.DataFrame(valid_rows))
    survivors = valid[valid["is_survivor"]].set_index("review_id")
    catalogue = read_catalogue(meta_path)
    mine = survivors.join(catalogue, on="parent_asin", how="left")
    multi = valid[valid["group_size"] > 1]
    groups = multi.groupby("review_id")["collision_class"].first()
    my_removed = len(multi) - len(groups) + int((groups == "unresolvable").sum())

    # ---- the run's tables ----
    spark = build("reproduce-silver", cores="local[4]", driver_memory="2g")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    try:
        theirs, their_rejects, their_colls = read_pinned(spark, row["outputs"])
    finally:
        spark.stop()
    their_reasons = {r: 0 for r in REASONS}
    for _, r in their_rejects.iterrows():
        their_reasons[r["reject_reason"]] = int(r["count"])
    theirs = theirs.set_index("review_id")

    # ---- comparisons ----
    checks: dict[str, bool] = {}
    print(f"SILVER_REPRO run_id={row['run_id']} raw_lines={raw_lines} bronze_rows={row['records_in']}")
    checks["rows_in"] = raw_lines == row["records_in"]
    for r in REASONS:
        print(f"SILVER_REPRO reject reason={r} mine={reason_counts[r]} theirs={their_reasons[r]}")
    checks["rejects"] = reason_counts == their_reasons

    only_mine = mine.index.difference(theirs.index)
    only_theirs = theirs.index.difference(mine.index)
    print(f"SILVER_REPRO survivors mine={len(mine)} theirs={len(theirs)} "
          f"only_mine={len(only_mine)} only_theirs={len(only_theirs)}")
    checks["survivor_ids"] = len(only_mine) == 0 and len(only_theirs) == 0
    checks["removed"] = my_removed == int(row["counts"]["collision_rows_removed"])
    print(f"SILVER_REPRO collision_rows_removed mine={my_removed} theirs={row['counts']['collision_rows_removed']}")

    common = mine.index.intersection(theirs.index)
    m, t = mine.loc[common], theirs.loc[common]

    def mismatch(name: str, a: pd.Series, b: pd.Series) -> int:
        a = a.astype(object).where(a.notna(), None)
        b = b.astype(object).where(b.notna(), None)
        n = int((a.values != b.values).sum())
        print(f"SILVER_REPRO field={name} compared={len(a)} mismatched={n}")
        checks[f"field:{name}"] = n == 0
        return n
    mismatch("rating", m["rating"], t["rating"])
    mismatch("helpful_vote", m["helpful_vote"], t["helpful_vote"])
    mismatch("text_word_count", m["word_count"], t["text_word_count"])
    mismatch("review_month", m["review_month"], pd.to_datetime(t["review_month"]).dt.strftime("%Y-%m-01"))
    mismatch("product_title", m["product_title"], t["product_title"])
    mismatch("main_category", m["main_category"], t["main_category"])
    mismatch("store", m["store"], t["store"])
    mismatch("price", m["price"], t["price"].map(lambda v: None if v is None or pd.isna(v)
                                                     else Decimal(str(v)).quantize(Decimal("0.01"))))
    print(f"SILVER_REPRO unmatched_catalogue mine={int(m['product_title'].isna().sum())} "
          f"theirs={int(t['product_title'].isna().sum())}")

    my_c = (multi.set_index(["review_id", "row_hash"])[["collision_class", "is_survivor", "selection_rank"]]
            .sort_index())
    th_c = (their_colls.rename(columns={"collision_group_id": "review_id", "canonical_row_hash": "row_hash"})
            .set_index(["review_id", "row_hash"]).sort_index())
    only_mine_c, only_theirs_c = my_c.index.difference(th_c.index), th_c.index.difference(my_c.index)
    # Exact-duplicate rows share a hash, so compare as multisets: index + per-key counts.
    same_keys = (len(only_mine_c) == 0 and len(only_theirs_c) == 0
                 and my_c.groupby(level=[0, 1]).size().equals(th_c.groupby(level=[0, 1]).size()))
    cls_mismatch = surv_mismatch = 0
    if same_keys:
        mc = my_c.groupby(level=[0, 1]).agg(cls=("collision_class", "first"), s=("is_survivor", "sum"))
        tc = th_c.groupby(level=[0, 1]).agg(cls=("collision_class", "first"), s=("is_survivor", "sum"))
        cls_mismatch = int((mc["cls"].values != tc["cls"].values).sum())
        surv_mismatch = int((mc["s"].values != tc["s"].values).sum())
    print(f"SILVER_REPRO collisions mine_rows={len(my_c)} theirs_rows={len(th_c)} groups_mine={len(groups)} "
          f"only_mine={len(only_mine_c)} only_theirs={len(only_theirs_c)} class_mismatch={cls_mismatch} "
          f"survivor_mismatch={surv_mismatch}")
    checks["collisions"] = same_keys and cls_mismatch == 0 and surv_mismatch == 0
    for c in ("exact", "conflicting", "unresolvable"):
        print(f"SILVER_REPRO class={c} groups_mine={int((groups == c).sum())} "
              f"groups_theirs={row['counts'][f'{c}_groups']}")

    v = L.attest(gate.repro(scope=args.scope, checks=list(checks.items())), "silver_repro")
    v.emit()
    E.record(v, capability="silver_repro", phase="P2 Silver", kind="reproducibility",
             protocol_hash=row["git_commit_sha"] or "uncommitted-worktree",
             population={"name": f"{reviews_path.as_posix()} vs silver.reviews",
                         "n": raw_lines,
                         "silver_snapshot_id": row["outputs"]["silver.reviews"]["snapshot_id"],
                         "survivors_compared": len(common)},
             pipeline_run_id=row["run_id"], scope=args.scope,
             notes=[("re-derived in pandas from the raw JSONL, importing nothing from "
                     "src/spark/silver.py or src/common/canonical.py (ADR-0007)")])
    sys.exit(0 if v.passed else 1)


if __name__ == "__main__":
    main()
