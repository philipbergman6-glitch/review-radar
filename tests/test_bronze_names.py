"""The gate must never be able to touch the production table.

F3 in docs/AUDIT_REPORT_2026-09-01.md: `bronze.py` hard-coded its table and
checkpoint, so `--topic reviews.eos --reset-only` dropped the 701,528-row
production table. Names are now derived from the topic; these tests pin that
derivation, including the back-compat case where the default topic must keep
resolving to the table that already holds the data.
"""
from __future__ import annotations

import pytest

from src.common.spark import CATALOG
from src.spark.bronze import names_for


def test_default_topic_keeps_the_existing_production_names():
    table, checkpoint = names_for("reviews.raw")
    assert table == f"{CATALOG}.bronze.reviews_raw"
    assert checkpoint.name == "bronze_reviews_raw"


def test_gate_topic_resolves_somewhere_else_entirely():
    prod_table, prod_ckpt = names_for("reviews.raw")
    eos_table, eos_ckpt = names_for("reviews.eos")
    assert eos_table == f"{CATALOG}.bronze.reviews_eos"
    assert eos_table != prod_table
    assert eos_ckpt != prod_ckpt


@pytest.mark.parametrize("topic", ["a-b.c", "A.B", "x__y"])
def test_names_are_valid_sql_identifiers(topic):
    table, _ = names_for(topic)
    leaf = table.rsplit(".", 1)[1]
    assert leaf.replace("_", "").isalnum()
    assert not leaf[0].isdigit()


@pytest.mark.parametrize("topic", ["", "   ", "..."])
def test_unusable_topics_hard_fail(topic):
    with pytest.raises(ValueError):
        names_for(topic)
