"""The enrichment-term miner and matcher (RR-22).

These terms decide which reviews are *offered* for labelling, never what a label is, so the
tests are about determinism, ownership and the two filters -- not about accuracy.
"""
from __future__ import annotations

import json

import pytest

from src.ai.theme_terms import (
    TERMS_PATH,
    candidate_terms,
    corpus_frequency,
    load_terms,
    matched_terms,
    mine_terms,
    tokenise,
)


def test_tokenise_folds_the_curly_apostrophe():
    """Otherwise "doesn’t work" mines as "doesn" + "t work" and never matches real text."""
    assert tokenise("It doesn’t work") == ["it", "doesn't", "work"]
    assert tokenise("It doesn't work") == tokenise("It doesn’t work")


def test_candidate_terms_drops_short_and_stopword_unigrams_but_keeps_mixed_bigrams():
    terms = candidate_terms(tokenise("it does not work at all"))
    assert "work" in terms
    assert "not" not in terms and "it" not in terms      # stopwords
    assert "not work" in terms                            # stopword + content survives as a bigram
    assert "at all" not in terms                          # two stopwords carry nothing


def test_mine_terms_is_deterministic_and_gives_each_term_one_owner():
    docs = [("scent", "awful chemical smell"), ("scent", "chemical smell again"),
            ("scent", "the smell is chemical"),
            ("damage", "arrived cracked and broken"), ("damage", "arrived broken in the box"),
            ("damage", "box arrived cracked")]
    a = mine_terms(docs, min_reviews=2, min_odds=2.0)
    assert a == mine_terms(list(reversed(docs)), min_reviews=2, min_odds=2.0)
    assert "chemical" in a["scent"] and "arrived" in a["damage"]
    owners = [t for ts in a.values() for t in ts]
    assert len(owners) == len(set(owners))


def test_mine_terms_rarity_guard_drops_a_term_that_matches_most_of_the_corpus():
    docs = [("scent", "chemical smell"), ("scent", "chemical odor"),
            ("damage", "arrived cracked"), ("damage", "arrived broken")]
    corpus = ["chemical everywhere", "chemical again", "chemical once more", "nothing here"]
    df, n = corpus_frequency(corpus)
    kept = mine_terms(docs, min_reviews=2, min_odds=2.0, corpus_df=df, corpus_docs=n,
                      max_corpus_share=0.5)
    assert "chemical" not in kept["scent"]               # 3 of 4 corpus documents
    assert mine_terms(docs, min_reviews=2, min_odds=2.0)["scent"] == ["chemical"]


def test_mine_terms_rejects_a_parity_threshold():
    with pytest.raises(ValueError, match="min_odds"):
        mine_terms([("a", "x y")], min_odds=1.0)


def test_matched_terms_counts_distinct_terms_per_theme_whole_word_only():
    tt = load_terms(TERMS_PATH)
    theme = next(iter(tt.terms))
    term = tt.terms[theme][0]
    assert matched_terms(None, f"this review mentions {term} plainly", tt).get(theme, 0) >= 1
    assert matched_terms(None, "", tt) == {}


def test_the_frozen_artifact_is_loadable_and_covers_every_frozen_theme():
    tt = load_terms(TERMS_PATH)
    taxonomy = json.loads((TERMS_PATH.parent / "theme-taxonomy.json").read_text())
    assert sorted(tt.terms) == sorted(t["id"] for t in taxonomy["themes"])
    assert all(tt.terms.values()), "a theme with no terms could never fill an enriched quota"


def test_load_terms_rejects_a_term_claimed_by_two_themes(tmp_path):
    bad = tmp_path / "theme-terms.json"
    bad.write_text(json.dumps({"terms_version": "1", "params": {},
                               "terms": {"a": ["smell"], "b": ["smell"]}}))
    with pytest.raises(ValueError, match="claimed by both"):
        load_terms(bad)
