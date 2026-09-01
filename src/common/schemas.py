"""Explicit Spark schemas for the Amazon Reviews 2023 files.

Why explicit rather than inferred:

1. Cost -- schema inference makes Spark read the whole file once before it reads
   it for real. On the 11 GB category that is a wasted full scan.
2. Correctness -- the product `details` field is a free-form dict whose keys vary
   per product and collide under Spark's default case-INsensitive resolution
   (real example: "Assembly Required" vs "assembly required"), which makes
   inference fail outright with COLUMN_ALREADY_EXISTS.
3. Stability -- a declared schema means a new stray field upstream cannot
   silently change our table layout.

`details` is therefore typed as StringType: Spark keeps the raw JSON text and we
parse what we need on read (schema-on-read), instead of exploding an unbounded
key space into columns.
"""
from __future__ import annotations

from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

# ---------------------------------------------------------------- reviews ----
REVIEW_SCHEMA = StructType([
    StructField("rating", DoubleType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("images", ArrayType(StructType([
        StructField("small_image_url", StringType()),
        StructField("medium_image_url", StringType()),
        StructField("large_image_url", StringType()),
        StructField("attachment_type", StringType()),
    ]))),
    StructField("asin", StringType()),
    StructField("parent_asin", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", LongType()),          # epoch millis
    StructField("helpful_vote", IntegerType()),
    StructField("verified_purchase", BooleanType()),
])

# ------------------------------------------------------------ product meta ---
META_SCHEMA = StructType([
    StructField("main_category", StringType()),
    StructField("title", StringType()),
    StructField("average_rating", DoubleType()),
    StructField("rating_number", IntegerType()),
    StructField("features", ArrayType(StringType())),
    StructField("description", ArrayType(StringType())),
    StructField("price", StringType()),            # dirty: null / "9.99" / "$9.99" / ranges
    StructField("images", ArrayType(StructType([
        StructField("thumb", StringType()),
        StructField("large", StringType()),
        StructField("variant", StringType()),
        StructField("hi_res", StringType()),
    ]))),
    StructField("videos", ArrayType(StructType([
        StructField("title", StringType()),
        StructField("url", StringType()),
        StructField("user_id", StringType()),
    ]))),
    StructField("store", StringType()),
    StructField("categories", ArrayType(StringType())),
    StructField("details", StringType()),          # raw JSON, see module docstring
    StructField("parent_asin", StringType()),
    StructField("bought_together", StringType()),
])
