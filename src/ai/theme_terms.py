"""Enrichment terms per frozen theme: mined from discovery quotes, then frozen (RR-22).

ADR-0003 draws the audit's 120 enriched rows "by the frozen theme terms", but no artifact
said which strings those are: `conf/theme-taxonomy.json` holds prose written for a labeller,
and `merge-table.csv` holds the model's own aspect abstractions ("product effectiveness"),
which mostly do not occur in review text. RR-22 answers that with a *derived* artifact --
`conf/theme-terms.json`, mined here from the evidence quotes of the discovery complaints
whose aspect merged into each theme -- so the frame is reproducible from the repo alone
rather than resting on a second, unaudited hand list.

What this is not: it is not a classifier, it never enters a prompt, and it never touches a
label. It decides only which reviews are *offered* for labelling. A review matching
`does_not_work`'s terms is not thereby labelled `does_not_work`; the enriched rows are
expected to carry many true negatives, and that is why the audit also carries 80 rows no
term touched.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import Any

from src.common.config import PROJECT_ROOT

TERMS_PATH = PROJECT_ROOT / "conf" / "theme-terms.json"
TERMS_VERSION = "1"

_TOKEN = re.compile(r"[a-z0-9]+(?:'[a-z]+)?")

# Closed-class words carry no complaint signal and would match everything. Kept short and
# explicit rather than pulled from a library, so the artifact is reproducible without one.
STOPWORDS = frozenset(("a", "about", "after", "all", "also", "am", "an", "and", "any", "are",
    "as", "at", "be", "because", "been", "before", "being", "but", "by", "can", "cant",
    "could", "did", "didnt", "do", "does", "doesnt", "doing", "dont", "for", "from", "get",
    "got", "had", "has", "have", "having", "he", "her", "here", "hers", "him", "his", "how",
    "i", "if", "in", "into", "is", "it", "its", "just", "me", "more", "most", "my", "no",
    "nor", "not", "now", "of", "off", "on", "once", "one", "only", "or", "other", "our", "out",
    "over", "own", "same", "she", "should", "so", "some", "such", "than", "that", "the",
    "their", "them", "then", "there", "these", "they", "this", "those", "through", "to", "too",
    "under", "until", "up", "very", "was", "we", "were", "what", "when", "where", "which",
    "while", "who", "whom", "why", "will", "with", "would", "you", "your", "it's", "i've",
    "i'd", "don't", "didn't"))

MIN_TOKEN_LEN = 3


def tokenise(text: str | None) -> list[str]:
    # Curly apostrophes are folded first, or "doesn\u2019t" tokenises as "doesn" + "t".
    return _TOKEN.findall((text or "").replace("\u2019", "'").casefold())


def candidate_terms(tokens: list[str]) -> set[str]:
    """Whole-word unigrams and adjacent bigrams. A bigram of two stopwords is dropped."""
    out: set[str] = set()
    for t in tokens:
        if len(t) >= MIN_TOKEN_LEN and t not in STOPWORDS:
            out.add(t)
    for a, b in pairwise(tokens):
        if a in STOPWORDS and b in STOPWORDS:
            continue
        out.add(f"{a} {b}")
    return out


@dataclass(frozen=True)
class ThemeTerms:
    version: str
    terms: dict[str, list[str]]          # theme id -> terms, sorted
    params: dict[str, Any]
    file_hash: str

    @property
    def all_terms(self) -> dict[str, str]:
        """term -> owning theme. Every term is owned by exactly one theme."""
        return {t: theme for theme, ts in self.terms.items() for t in ts}


def corpus_frequency(texts: list[str]) -> tuple[dict[str, int], int]:
    """How many whole review texts each candidate term occurs in. The rarity guard's input."""
    df: dict[str, int] = defaultdict(int)
    for text in texts:
        for term in candidate_terms(tokenise(text)):
            df[term] += 1
    return dict(df), len(texts)


def mine_terms(docs: list[tuple[str, str]], *, min_reviews: int = 2, min_odds: float = 3.0,
               max_per_theme: int = 120, corpus_df: dict[str, int] | None = None,
               corpus_docs: int = 0, max_corpus_share: float = 0.10) -> dict[str, list[str]]:
    """Mine one term list per theme from `docs`, one (theme, quote text) per (review, theme).

    A term is kept for theme T when it occurs in at least `min_reviews` of T's documents and
    its rate in T is at least `min_odds` times its rate outside T. A term that clears the
    bar for two themes is owned by the higher-odds theme and by no other, so enrichment
    quotas never compete for the same string. Ties break on the theme id, so the artifact
    is a pure function of its inputs.

    The odds are measured between *quote* corpora, which are complaint-dense, so a common
    English word can clear them and still match most of the population -- "like" was 3x more
    frequent in `not_as_described` quotes than elsewhere. `corpus_df` closes that hole: a
    term occurring in more than `max_corpus_share` of whole review texts is dropped as
    non-discriminating, whatever its odds. Omitting `corpus_df` disables the guard.
    """
    if min_reviews < 1:
        raise ValueError("min_reviews must be >= 1")
    if min_odds <= 1.0:
        raise ValueError("min_odds must be > 1: a term at parity is not discriminating")
    if corpus_df is not None and corpus_docs <= 0:
        raise ValueError("corpus_df needs corpus_docs > 0 to form a share")
    if not 0.0 < max_corpus_share <= 1.0:
        raise ValueError("max_corpus_share must be in (0, 1]")
    themes = sorted({t for t, _ in docs})
    per_theme_docs: dict[str, int] = defaultdict(int)
    df: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for theme, text in docs:
        per_theme_docs[theme] += 1
        for term in candidate_terms(tokenise(text)):
            df[term][theme] += 1
    total_docs = len(docs)

    best: dict[str, tuple[float, str]] = {}   # term -> (odds, theme)
    for term, counts in df.items():
        if corpus_df is not None and corpus_df.get(term, 0) / corpus_docs > max_corpus_share:
            continue
        for theme in themes:
            a = counts.get(theme, 0)
            if a < min_reviews:
                continue
            n_t = per_theme_docs[theme]
            rest_docs = total_docs - n_t
            rest = sum(v for k, v in counts.items() if k != theme)
            p_t = a / n_t
            p_rest = (rest / rest_docs) if rest_docs else 0.0
            odds = p_t / p_rest if p_rest > 0 else math.inf
            if odds < min_odds:
                continue
            prev = best.get(term)
            if prev is None or (odds, theme) > (prev[0], prev[1]):
                best[term] = (odds, theme)

    owned: dict[str, list[tuple[float, int, str]]] = defaultdict(list)
    for term, (odds, theme) in best.items():
        owned[theme].append((odds, df[term][theme], term))
    out: dict[str, list[str]] = {}
    for theme in themes:
        ranked = sorted(owned.get(theme, []), key=lambda r: (-r[0], -r[1], r[2]))[:max_per_theme]
        out[theme] = sorted(t for _, _, t in ranked)
    return out


def matched_terms(title: str | None, text: str | None, tt: ThemeTerms) -> dict[str, int]:
    """How many distinct terms of each theme occur in this review.

    Matching runs through the same `candidate_terms(tokenise(...))` generator that produced
    the artifact, so a term matches here exactly when it would have been mined there -- whole
    word, apostrophes folded, bigrams adjacent. That is cheaper than a regex alternation
    (one pass over the tokens, not one pass per term) and cannot drift from the miner.

    The count matters, not just the hit: a review carrying three `irritation_or_harm` terms
    is far likelier to be a real positive than one carrying only the artifact's noisiest
    single term, and the enriched draw orders on it.
    """
    owner = tt.all_terms
    hits: dict[str, set[str]] = defaultdict(set)
    for term in candidate_terms(tokenise(f"{title or ''} {text or ''}")):
        theme = owner.get(term)
        if theme is not None:
            hits[theme].add(term)
    return {theme: len(v) for theme, v in hits.items()}


def load_terms(path: Path = TERMS_PATH) -> ThemeTerms:
    if not path.exists():
        raise FileNotFoundError(
            f"{path} is missing: run scripts/freeze_theme_terms.py before drawing an enriched frame")
    raw = path.read_bytes()
    doc = json.loads(raw)
    if str(doc["terms_version"]) != TERMS_VERSION:
        raise ValueError(f"theme terms version {doc['terms_version']!r} != {TERMS_VERSION}")
    terms = {k: list(v) for k, v in doc["terms"].items()}
    for theme, ts in terms.items():
        if not ts:
            raise ValueError(f"theme {theme!r} has no terms; an enriched quota could never be filled")
        if len(set(ts)) != len(ts):
            raise ValueError(f"theme {theme!r} repeats a term")
    seen: dict[str, str] = {}
    for theme, ts in terms.items():
        for t in ts:
            if t in seen:
                raise ValueError(f"term {t!r} is claimed by both {seen[t]!r} and {theme!r}")
            seen[t] = theme
    return ThemeTerms(version=str(doc["terms_version"]), terms=terms, params=doc["params"],
                      file_hash=hashlib.sha256(raw).hexdigest())
