"""The pandas reproduction must reach the frozen encoding and reject rules on its own.

It never imports src.common.canonical, so the golden values are restated here as literals
(the same ones tests/test_canonical.py pins for the reference implementation).
"""
from __future__ import annotations

import hashlib
import json

import pandas as pd
import pytest

from scripts import reproduce_silver as R

BOUND = 1_800_000_000_000  # 2027-01-15, above any timestamp in the file


def test_encoding_matches_the_pinned_golden_vector():
    expected = hashlib.sha256(
        b"\x00\x00\x00\x03usr" + b"\x00\x00\x00\x04B001" + b"\x00\x00\x00\x0d1577836800000"
    ).hexdigest()
    assert R.sha("usr", "B001", "1577836800000") == expected
    assert R.sha(None) != R.sha("")


def test_images_text_is_compact_sorted_nulls_dropped():
    imgs = [{"small_image_url": "s", "large_image_url": None, "attachment_type": "IMAGE"},
            {"small_image_url": "t"}]
    assert R.images_text(imgs) == '[{"attachment_type":"IMAGE","small_image_url":"s"},{"small_image_url":"t"}]'
    assert R.images_text([]) == "[]"
    assert R.images_text(None) is None


def _line(**over) -> str:
    d = {"rating": 5.0, "title": "t", "text": "x y", "images": [], "asin": "A", "parent_asin": "P",
         "user_id": "U", "timestamp": 1577836800000, "helpful_vote": 0, "verified_purchase": True}
    d.update(over)
    return json.dumps(d)


@pytest.mark.parametrize("line, expected", [
    ("{not json", "unparsable_json"),
    ("[1, 2]", "unparsable_json"),
    (_line(user_id="  "), "missing_key_field"),
    (_line(parent_asin=None), "missing_key_field"),
    (_line(timestamp=None), "missing_key_field"),
    (_line(rating=0), "invalid_rating"),
    (_line(rating=4.5), "invalid_rating"),
    (_line(rating="five"), "invalid_rating"),
    (_line(rating=True), "invalid_rating"),
    (_line(timestamp=1.5e12), "timestamp_out_of_range"),
    (_line(timestamp=788_918_399_999), "timestamp_out_of_range"),
    (_line(timestamp=BOUND + 1), "timestamp_out_of_range"),
    (_line(), None),
])
def test_reject_reasons(line, expected):
    assert R.reject_reason(line, BOUND)[0] == expected


def test_precedence_missing_key_beats_rating_beats_timestamp():
    assert R.reject_reason(_line(user_id="", rating=9, timestamp=1), BOUND)[0] == "missing_key_field"
    assert R.reject_reason(_line(rating=9, timestamp=1), BOUND)[0] == "invalid_rating"


def _row(i: int, **over) -> dict:
    d = json.loads(_line(**over))
    return R.valid_row(d, i)


def test_collision_classes_and_survivor():
    rows = [
        _row(0),                                               # singleton
        _row(1, user_id="U2"), _row(2, user_id="U2"),          # exact pair
        _row(3, user_id="U3", helpful_vote=1, text="short"),   # conflicting: survivor = hv 1
        _row(4, user_id="U3", helpful_vote=0, text="much longer text"),
        _row(5, user_id="U4", rating=5), _row(6, user_id="U4", rating=1),  # unresolvable
    ]
    df = R.classify(pd.DataFrame(rows)).set_index("lineno").sort_index()
    assert df.loc[0, "collision_class"] is None or pd.isna(df.loc[0, "collision_class"])
    assert df.loc[0, "is_survivor"]
    assert list(df.loc[[1, 2], "collision_class"]) == ["exact", "exact"]
    assert int(df.loc[[1, 2], "is_survivor"].sum()) == 1
    assert list(df.loc[[3, 4], "collision_class"]) == ["conflicting", "conflicting"]
    assert df.loc[3, "is_survivor"] and not df.loc[4, "is_survivor"]
    assert list(df.loc[[5, 6], "collision_class"]) == ["unresolvable", "unresolvable"]
    assert not df.loc[[5, 6], "is_survivor"].any()


def test_price_parse():
    assert R.parse_price("$1,234.5") == R.parse_price(1234.5)
    assert R.parse_price("abc") is None
    assert R.parse_price(None) is None
