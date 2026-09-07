"""Job `catalogue_load`: the product metadata JSONL -> PostgreSQL `products` (ADR-0007 §5).

Stream the file with orjson, `COPY` the shaped rows into a staging table, validate the
count and key uniqueness, then replace `products` inside one transaction. Every product
row carries `catalogue_load_id` = this run's `pipeline_runs.run_id`, which is what silver
records as its catalogue input. A second load is a second run id; nothing is appended.

Price is the only dirty column: numbers and optional-dollar decimal strings parse, ranges
and words become NULL and are counted as invalid. No `details` keys are parsed.

Run:  ./run.sh python -m src.catalogue.load_products [--source PATH] [--scope full|sample]
"""
from __future__ import annotations

import argparse
import hashlib
import math
import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import orjson

from src.common import config as C
from src.common import runs
from src.common.pg import connect

# Frozen: an optional dollar sign, digits with optional thousands separators, an optional
# two-or-fewer-digit fraction. Anything else (ranges, "from", words) is invalid.
PRICE_RE = re.compile(r"^\$?(\d{1,3}(?:,\d{3})*|\d+)(?:\.(\d{1,2}))?$")
PRICE_MAX = Decimal("99999999.99")           # NUMERIC(10, 2)

COLUMNS = ("parent_asin", "title", "main_category", "store", "price", "average_rating",
           "rating_number", "categories")


@dataclass(frozen=True)
class PriceParse:
    value: Decimal | None
    status: str                                  # parsed | missing | invalid


def parse_price(raw: Any) -> PriceParse:
    if raw is None:
        return PriceParse(None, "missing")
    if isinstance(raw, bool):
        raise TypeError(f"price is a bool: {raw!r}")
    if isinstance(raw, (int, float)):
        if isinstance(raw, float) and not math.isfinite(raw):
            return PriceParse(None, "invalid")
        value = Decimal(str(raw))
    elif isinstance(raw, str):
        s = raw.strip()
        if not s:
            return PriceParse(None, "missing")
        m = PRICE_RE.match(s)
        if not m:
            return PriceParse(None, "invalid")
        value = Decimal(m.group(1).replace(",", "") + (f".{m.group(2)}" if m.group(2) else ""))
    else:
        raise TypeError(f"price has unexpected type {type(raw).__name__}: {raw!r}")
    if value < 0 or value > PRICE_MAX:
        return PriceParse(None, "invalid")
    return PriceParse(value.quantize(Decimal("0.01")), "parsed")


@dataclass(frozen=True)
class ProductRow:
    parent_asin: str
    title: str
    main_category: str | None
    store: str | None
    price: Decimal | None
    price_status: str
    average_rating: float | None
    rating_number: int | None
    categories: list[str] | None

    def as_copy_tuple(self) -> tuple:
        return (self.parent_asin, self.title, self.main_category, self.store, self.price,
                self.average_rating, self.rating_number, self.categories)


def shape_row(d: dict[str, Any]) -> ProductRow:
    key = d.get("parent_asin")
    if not key or not str(key).strip():
        raise ValueError(f"product without parent_asin: {str(d)[:200]}")
    title = d.get("title")
    if title is None:
        raise ValueError(f"product {key} has a null title")
    price = parse_price(d.get("price"))
    cats = d.get("categories")
    return ProductRow(
        parent_asin=str(key).strip(), title=str(title), main_category=d.get("main_category"),
        store=d.get("store"), price=price.value, price_status=price.status,
        average_rating=d.get("average_rating"), rating_number=d.get("rating_number"),
        categories=None if cats is None else [str(c) for c in cats])


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def iter_rows(path: Path):
    with path.open("rb") as f:
        for line in f:
            if line.strip():
                yield shape_row(orjson.loads(line))


def load(source: Path, *, scope: str, category: str) -> dict[str, Any]:
    run = runs.start("catalogue_load", runs.CATALOGUE_LOAD_SPEC_VERSION, category=category,
                     data_scope=scope,
                     inputs={"source": {"path": str(source), "sha256": sha256_of(source)}},
                     params={"price_regex": PRICE_RE.pattern, "columns": list(COLUMNS)})
    counts: dict[str, int] = {"source_rows": 0, "parsed_prices": 0, "missing_prices": 0,
                              "invalid_prices": 0, "empty_titles": 0}
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute("""CREATE TEMP TABLE products_staging (
                               parent_asin TEXT, title TEXT, main_category TEXT, store TEXT,
                               price NUMERIC(10,2), average_rating REAL, rating_number INTEGER,
                               categories TEXT[]) ON COMMIT DROP""")
            with cur.copy(f"COPY products_staging ({', '.join(COLUMNS)}) FROM STDIN") as copy:
                for row in iter_rows(source):
                    copy.write_row(row.as_copy_tuple())
                    counts["source_rows"] += 1
                    counts[f"{row.price_status}_prices"] += 1
                    if not row.title.strip():
                        counts["empty_titles"] += 1
            staged, distinct = cur.execute(
                "SELECT count(*), count(DISTINCT parent_asin) FROM products_staging").fetchone()
            if staged != counts["source_rows"]:
                raise RuntimeError(f"staged {staged} rows but read {counts['source_rows']}")
            if distinct != staged:
                raise RuntimeError(f"parent_asin not unique in source: {staged - distinct} repeats")
            cur.execute("TRUNCATE products")
            cur.execute(f"""INSERT INTO products ({', '.join(COLUMNS)}, catalogue_load_id)
                            SELECT {', '.join(COLUMNS)}, %s FROM products_staging""", (run.run_id,))
            counts["rows_loaded"] = cur.rowcount
            counts["final_count"] = cur.execute("SELECT count(*) FROM products").fetchone()[0]
            load_ids = cur.execute("SELECT count(DISTINCT catalogue_load_id) FROM products").fetchone()[0]
            if load_ids != 1:
                raise RuntimeError(f"products holds {load_ids} distinct catalogue_load_id values")
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", counts=counts)
        raise
    runs.success(run, records_in=counts["source_rows"], records_out=counts["rows_loaded"],
                 records_rejected=0,
                 outputs={"products": {"table": "products", "catalogue_load_id": run.run_id}},
                 counts=counts)
    for k in ("source_rows", "parsed_prices", "missing_prices", "invalid_prices", "empty_titles",
              "rows_loaded", "final_count"):
        print(f"CATALOGUE_LOAD {k}={counts[k]}")
    print(f"CATALOGUE_LOAD catalogue_load_id={run.run_id} scope={scope}")
    return {"run_id": run.run_id, **counts}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--source", default=None, help="override the metadata JSONL path")
    args = ap.parse_args()
    if args.source:
        source = Path(args.source)
    elif args.scope == "sample":
        source = C.DATA_SAMPLE / f"meta_{args.category}.sample.jsonl"
    else:
        source = C.DATA_RAW / f"meta_{args.category}.jsonl"
    if not source.exists():
        raise FileNotFoundError(f"{source} not found. Run scripts/download_data.py first.")
    load(source, scope=args.scope, category=args.category)


if __name__ == "__main__":
    main()
