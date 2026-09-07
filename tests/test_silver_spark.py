"""Silver's pure DataFrame seams on a local Spark session (ADR-0007).

Parity: the Spark expressions for review_id, the survivor hash and the word count must
equal the Python reference in src/common/canonical.py and Python's own str.split().
Rules: reject precedence, collision classes and the survivor order on crafted rows.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.canonical import review_id, survivor_hash
from src.spark.silver import (
    classify_collisions,
    collision_counts,
    parse_and_validate,
    typed_valid_rows,
    word_count_col,
)

INGESTED = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
INGESTED_MS = int(INGESTED.timestamp() * 1000)

BRONZE_SCHEMA = StructType([
    StructField("payload", StringType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("ingested_at", TimestampType()),
])


def bronze_df(spark, payloads: list[str]):
    rows = [(p, 0, i, INGESTED) for i, p in enumerate(payloads)]
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


def review(**kw) -> str:
    base = {"rating": 5.0, "title": "Great", "text": "Works well and smells nice",
            "images": [], "asin": "A1", "parent_asin": "P1", "user_id": "U1",
            "timestamp": 1577836800000, "helpful_vote": 0, "verified_purchase": True}
    base.update(kw)
    return json.dumps(base)


# ------------------------------------------------------------- validation ----
def reasons(spark, payloads):
    df = parse_and_validate(bronze_df(spark, payloads))
    return [r["reject_reason"] for r in df.orderBy("kafka_offset").select("reject_reason").collect()]


def test_valid_row_has_no_reason(spark):
    assert reasons(spark, [review()]) == [None]


@pytest.mark.parametrize("payload,expected", [
    ("not json at all", "unparsable_json"),
    ("", "unparsable_json"),
    ('{"rating": 5.0, "images": "nope"}', "unparsable_json"),         # schema mismatch
    (review(user_id=None), "missing_key_field"),
    (review(user_id="   "), "missing_key_field"),
    (review(parent_asin=""), "missing_key_field"),
    (review(timestamp=None), "missing_key_field"),
    (review(rating=None), "invalid_rating"),
    (review(rating="five"), "invalid_rating"),
    (review(rating=4.5), "invalid_rating"),
    (review(rating=0), "invalid_rating"),
    (review(rating=6), "invalid_rating"),
    (review(timestamp=788918399999), "timestamp_out_of_range"),         # 1 ms before 1995
    (review(timestamp=INGESTED_MS + 1), "timestamp_out_of_range"),      # after ingest
    (review(timestamp="12.5"), "timestamp_out_of_range"),
    (review(text=""), None),                                            # empty text is not a reject
    (review(title=None, text=None), None),
    (review(timestamp=788918400000), None),                             # exactly 1995-01-01
    (review(timestamp=INGESTED_MS), None),
    (review(rating=3), None),                                           # integer-valued JSON int
])
def test_reject_reason(spark, payload, expected):
    assert reasons(spark, [payload]) == [expected]


def test_precedence_missing_key_beats_invalid_rating_beats_timestamp(spark):
    assert reasons(spark, [review(user_id=None, rating=9, timestamp=1)]) == ["missing_key_field"]
    assert reasons(spark, [review(rating=9, timestamp=1)]) == ["invalid_rating"]


def test_timestamp_diagnostics_name_which_bound(spark):
    df = parse_and_validate(bronze_df(spark, [review(timestamp=1), review(timestamp=INGESTED_MS + 5)]))
    d = [r["reject_diagnostics"] for r in df.orderBy("kafka_offset").collect()]
    assert d[0]["below_1995"] == "true" and d[0]["after_ingest"] == "false"
    assert d[1]["below_1995"] == "false" and d[1]["after_ingest"] == "true"


# ----------------------------------------------------------------- parity ----
TRICKY = [
    {"user_id": "U1", "parent_asin": "P1", "timestamp": 1577836800000, "rating": 5, "title": "Great",
         "text": "Works well", "helpful_vote": 3, "asin": "A1", "verified_purchase": True, "images": []},
    {"user_id": " U2 ", "parent_asin": "P2", "timestamp": 788918400000, "rating": 1, "title": "", "text": None,
         "helpful_vote": None, "asin": " A2 ", "verified_purchase": False, "images": None},
    {"user_id": "Ü3 💄", "parent_asin": "P3", "timestamp": 1700000000000, "rating": 3,
         "title": "Löve it 💄", "text": "tab\tand  double space nbsp", "helpful_vote": 12,
         "asin": "A3", "verified_purchase": None,
         "images": [{"small_image_url": "https://x/ü.jpg", "medium_image_url": None,
                  "large_image_url": "l", "attachment_type": "IMAGE"}]},
    {"user_id": "U4", "parent_asin": "P4", "timestamp": 1600000000000, "rating": 2, "title": "a|b",
         "text": "c", "helpful_vote": 0, "asin": "A4", "verified_purchase": True,
         "images": [{"small_image_url": "s"}, {"large_image_url": "l"}]},
]


def test_review_id_and_survivor_hash_match_python_reference(spark):
    payloads = [json.dumps(r) for r in TRICKY]
    df = typed_valid_rows(parse_and_validate(bronze_df(spark, payloads)))
    got = {r["kafka_offset"]: r for r in df.collect()}
    for i, ref in enumerate(TRICKY):
        assert got[i]["review_id"] == review_id(ref["user_id"], ref["parent_asin"], ref["timestamp"])
        imgs = ref["images"]
        if imgs is not None:
            imgs = [{k: v for k, v in img.items() if v is not None} for img in imgs]
        assert got[i]["canonical_row_hash"] == survivor_hash(
            rating=ref["rating"], title=ref["title"], text=ref["text"],
            verified_purchase=ref["verified_purchase"], helpful_vote=ref["helpful_vote"],
            asin=ref["asin"], images=imgs), f"row {i}"


@pytest.mark.parametrize("text", [
    None, "", "   ", "one", "two words", "tab\tsep", "double  space", "nbsp here",
    "ideographic　space", " line sep", "trail ", " lead", "x\x1cy", "\u200bzero-width",
    "emoji 💄 mid", "new\nline\r\n",
])
def test_word_count_matches_python_split(spark, text):
    df = spark.createDataFrame([(text,)], "text string").select(word_count_col(F.col("text")).alias("n"))
    assert df.first()["n"] == (len(text.split()) if text else 0)


# ------------------------------------------------------------- collisions ----
def classified(spark, payloads):
    df = classify_collisions(typed_valid_rows(parse_and_validate(bronze_df(spark, payloads))))
    return {r["kafka_offset"]: r for r in df.collect()}, df


def test_singleton_survives_with_only_row(spark):
    rows, _ = classified(spark, [review()])
    assert rows[0]["is_survivor"] and rows[0]["collision_class"] is None
    assert rows[0]["selection_reason"] == "only_row"


def test_exact_duplicates_keep_one(spark):
    rows, df = classified(spark, [review(), review(), review()])
    assert sorted(r["is_survivor"] for r in rows.values()) == [False, False, True]
    assert {r["collision_class"] for r in rows.values()} == {"exact"}
    assert all(r["differing_fields"] == [] for r in rows.values())
    c = collision_counts(df)
    assert c == {"collision_groups": 1, "exact_groups": 1, "conflicting_groups": 0,
                 "unresolvable_groups": 0, "collision_table_rows": 3, "collision_rows_removed": 2}


def test_conflicting_survivor_is_highest_helpful_vote_then_longest_text(spark):
    rows, _ = classified(spark, [review(helpful_vote=1, text="short"),
                                 review(helpful_vote=7, text="short"),
                                 review(helpful_vote=7, text="a much longer text")])
    assert rows[2]["is_survivor"] and rows[2]["selection_reason"] == "longest_text"
    assert not rows[0]["is_survivor"] and not rows[1]["is_survivor"]
    assert rows[2]["collision_class"] == "conflicting"
    assert set(rows[2]["differing_fields"]) == {"helpful_vote", "text"}

    rows, _ = classified(spark, [review(helpful_vote=None), review(helpful_vote=2)])
    assert rows[1]["is_survivor"] and rows[1]["selection_reason"] == "highest_helpful_vote"


def test_verified_is_not_preferred(spark):
    rows, _ = classified(spark, [review(verified_purchase=False, helpful_vote=5),
                                 review(verified_purchase=True, helpful_vote=1)])
    assert rows[0]["is_survivor"]
    assert rows[0]["differing_fields"] == ["verified_purchase", "helpful_vote"]


def test_rating_disagreement_has_no_survivor(spark):
    rows, df = classified(spark, [review(rating=5), review(rating=1)])
    assert not any(r["is_survivor"] for r in rows.values())
    assert {r["collision_class"] for r in rows.values()} == {"unresolvable"}
    assert {r["selection_reason"] for r in rows.values()} == {"no_survivor"}
    c = collision_counts(df)
    assert c["unresolvable_groups"] == 1 and c["collision_rows_removed"] == 2


def test_null_vs_present_field_counts_as_differing(spark):
    rows, _ = classified(spark, [review(title=None), review(title="Great")])
    assert "title" in rows[0]["differing_fields"]
    assert rows[0]["collision_class"] == "conflicting"


def test_trimmed_identity_collides_untrimmed(spark):
    rows, df = classified(spark, [review(user_id="U1"), review(user_id=" U1 ")])
    assert collision_counts(df)["collision_groups"] == 1
    assert rows[0]["review_id"] == rows[1]["review_id"]


def test_silver_names_follow_the_topic():
    from src.spark.silver import silver_names
    assert silver_names("reviews.raw")["reviews"].endswith(".silver.reviews")
    assert silver_names("reviews.raw.sample")["rejects"].endswith(".silver.rejects_sample")
    assert silver_names("reviews.eos")["collisions"].endswith(".silver.review_collisions_eos")
