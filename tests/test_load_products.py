"""Pure parts of the catalogue loader (ADR-0007 §5): price parsing and row shaping."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.catalogue.load_products import PriceParse, parse_price, shape_row


@pytest.mark.parametrize("raw,expected", [
    (None, PriceParse(None, "missing")),
    ("", PriceParse(None, "missing")),
    ("   ", PriceParse(None, "missing")),
    (9.99, PriceParse(Decimal("9.99"), "parsed")),
    (12, PriceParse(Decimal(12), "parsed")),
    ("9.99", PriceParse(Decimal("9.99"), "parsed")),
    ("$9.99", PriceParse(Decimal("9.99"), "parsed")),
    (" $1,299.50 ", PriceParse(Decimal("1299.50"), "parsed")),
    ("$9.99 - $19.99", PriceParse(None, "invalid")),
    ("from $5", PriceParse(None, "invalid")),
    ("free", PriceParse(None, "invalid")),
    (-3.0, PriceParse(None, "invalid")),
    (123456789.99, PriceParse(None, "invalid")),     # exceeds NUMERIC(10,2)
    (float("nan"), PriceParse(None, "invalid")),
])
def test_parse_price(raw, expected):
    assert parse_price(raw) == expected


def test_parse_price_rejects_unknown_types_loudly():
    with pytest.raises(TypeError):
        parse_price({"amount": 1})


def test_shape_row_keeps_empty_title_and_trims_key():
    row = shape_row({"parent_asin": " B01 ", "title": "", "main_category": "All Beauty",
                     "store": None, "price": None, "average_rating": 4.5, "rating_number": 3,
                     "categories": ["a", "b"]})
    assert row.parent_asin == "B01"
    assert row.title == ""
    assert row.categories == ["a", "b"]
    assert row.price is None


def test_shape_row_hard_fails_on_missing_key_or_title():
    with pytest.raises(ValueError):
        shape_row({"parent_asin": "", "title": "x"})
    with pytest.raises(ValueError):
        shape_row({"parent_asin": "B01", "title": None})
