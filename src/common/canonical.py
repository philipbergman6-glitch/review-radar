"""Canonical encoding for review identity and the collision survivor hash (ADR-0007 §4).

`review_id = SHA-256(canonical(user_id, parent_asin, timestamp_ms))` is the one identity
every downstream table keys on, and it must be computable from the raw source file alone,
in any engine. So the encoding is fixed here in words and in code:

* fields in a fixed order, each written as a 4-byte big-endian byte length followed by
  the UTF-8 bytes;
* null is the length marker 0xFFFFFFFF with no bytes; the empty string is length 0 --
  null and empty are different values;
* identity fields (`user_id`, `parent_asin`, `asin`) are whitespace-trimmed before
  encoding and must not be blank; `title` and `text` are preserved exactly;
* numbers are decimal ASCII (rating `1`..`5`, votes and timestamps as integers),
  booleans are `true`/`false`;
* `images` is compact JSON with object keys sorted, null members dropped, array order
  preserved;
* digests are lowercase hex.

The Spark equivalent lives in `src.spark.silver` (`review_id_col`, `survivor_hash_col`);
`tests/test_canonical.py` pins golden vectors and `tests/test_silver_spark_parity.py`
checks the two engines agree. This module is the *reference* implementation; the
independent pandas reproduction (RR-09) must not import it.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

NULL_MARKER = b"\xff\xff\xff\xff"


def encode_field(value: str | None) -> bytes:
    if value is None:
        return NULL_MARKER
    raw = value.encode("utf-8")
    return len(raw).to_bytes(4, "big") + raw


def canonical_bytes(values: list[str | None]) -> bytes:
    return b"".join(encode_field(v) for v in values)


def _identity(name: str, value: str | None) -> str:
    if value is None or not str(value).strip():
        raise ValueError(f"identity field '{name}' is blank; validate before hashing")
    return str(value).strip()


def review_id(user_id: str, parent_asin: str, timestamp_ms: int) -> str:
    if timestamp_ms is None:
        raise ValueError("identity field 'timestamp' is null; validate before hashing")
    payload = canonical_bytes([
        _identity("user_id", user_id),
        _identity("parent_asin", parent_asin),
        str(int(timestamp_ms)),
    ])
    return hashlib.sha256(payload).hexdigest()


def images_json(images: list[dict[str, Any]] | None) -> str | None:
    if images is None:
        return None
    cleaned = [{k: v for k, v in img.items() if v is not None} for img in images]
    return json.dumps(cleaned, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def survivor_hash(*, rating: int, title: str | None, text: str | None,
                  verified_purchase: bool | None, helpful_vote: int | None,
                  asin: str | None, images: list[dict[str, Any]] | None) -> str:
    """Hash of the seven retained fields; the last tiebreak in the survivor rule."""
    payload = canonical_bytes([
        str(int(rating)),
        title,
        text,
        None if verified_purchase is None else ("true" if verified_purchase else "false"),
        None if helpful_vote is None else str(int(helpful_vote)),
        None if asin is None else asin.strip(),
        images_json(images),
    ])
    return hashlib.sha256(payload).hexdigest()
