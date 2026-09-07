"""The canonical encoding behind `review_id` and the survivor hash (ADR-0007 §4).

Two engines compute these hashes -- Python here, a Spark expression in silver --
and they must agree byte for byte, so the encoding is frozen by golden vectors.
The vectors below were generated once from this implementation and pinned;
a change that alters any of them changes every review identity downstream.
"""
from __future__ import annotations

import hashlib

import pytest

from src.common.canonical import (
    NULL_MARKER,
    canonical_bytes,
    encode_field,
    images_json,
    review_id,
    survivor_hash,
)


def test_length_prefix_is_four_byte_big_endian_utf8():
    assert encode_field("ab") == b"\x00\x00\x00\x02ab"
    assert encode_field("é") == b"\x00\x00\x00\x02" + "é".encode("utf-8")


def test_null_and_empty_are_different_things():
    assert encode_field(None) == NULL_MARKER == b"\xff\xff\xff\xff"
    assert encode_field("") == b"\x00\x00\x00\x00"
    assert canonical_bytes([None]) != canonical_bytes([""])


def test_embedded_separators_cannot_collide():
    # Length-prefixing means ("a|b", "c") and ("a", "b|c") never encode the same.
    assert canonical_bytes(["a|b", "c"]) != canonical_bytes(["a", "b|c"])
    assert canonical_bytes(["ab", ""]) != canonical_bytes(["a", "b"])


def test_review_id_is_sha256_hex_of_the_three_key_fields():
    expected = hashlib.sha256(
        b"\x00\x00\x00\x03usr" + b"\x00\x00\x00\x04B001" + b"\x00\x00\x00\x0d1577836800000"
    ).hexdigest()
    assert review_id("usr", "B001", 1577836800000) == expected


def test_identity_fields_are_trimmed_but_never_blank():
    assert review_id(" usr ", "B001", 5) == review_id("usr", "B001", 5)
    with pytest.raises(ValueError):
        review_id("  ", "B001", 5)
    with pytest.raises(ValueError):
        review_id("usr", "B001", None)


def test_images_json_is_compact_sorted_keys_nulls_dropped_order_kept():
    imgs = [{"small_image_url": "s", "large_image_url": None, "attachment_type": "IMAGE"},
            {"small_image_url": "t"}]
    assert images_json(imgs) == '[{"attachment_type":"IMAGE","small_image_url":"s"},{"small_image_url":"t"}]'
    assert images_json([]) == "[]"
    assert images_json(None) is None


def test_survivor_hash_preserves_title_and_text_exactly():
    base = dict(rating=5, title="T", text=" spaced ", verified_purchase=True,
                helpful_vote=3, asin="A1", images=None)
    assert survivor_hash(**base) != survivor_hash(**{**base, "text": "spaced"})
    assert survivor_hash(**base) != survivor_hash(**{**base, "rating": 4})
    assert survivor_hash(**base) != survivor_hash(**{**base, "helpful_vote": None})
    assert survivor_hash(**base) == survivor_hash(**{**base, "asin": " A1 "})


GOLDEN_REVIEW_IDS = [
    # (user_id, parent_asin, timestamp_ms) -> sha256 hex
    (("u1", "B00ABC", 1262304000000),
     "a2ae6da56fcbdb26d485528fb03cf2281fd375db493a58d3fb97f7b3f2aa9b4f"),
]


@pytest.mark.parametrize("key,expected", GOLDEN_REVIEW_IDS)
def test_golden_review_ids(key, expected):
    assert review_id(*key) == expected


GOLDEN_SURVIVOR_HASHES = [
    (dict(rating=1, title="", text=None, verified_purchase=False, helpful_vote=0,
          asin="X", images=[]),
     "cfb7a986a6e7f4f9e6eb95c0c7b8e01482f8019de0b249dedd5b73ae9d54e1c6"),
    (dict(rating=5, title="Löve it 💄", text="tab\tand  double space nbsp",
          verified_purchase=True, helpful_vote=12, asin="B0000001",
          images=[{"small_image_url": "s", "medium_image_url": "m",
                   "large_image_url": "l", "attachment_type": "IMAGE"}]),
     "22ce17fa60c3d413ed2dd9f26cc8252a26ad49368f3b58a5920e70a20dc46091"),
]


@pytest.mark.parametrize("fields,expected", GOLDEN_SURVIVOR_HASHES)
def test_golden_survivor_hashes(fields, expected):
    assert survivor_hash(**fields) == expected
