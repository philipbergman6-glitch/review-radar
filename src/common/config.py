"""Central configuration, loaded from .env with sane local defaults.

Hard-fails on anything that is required but missing, rather than silently
falling back -- a wrong endpoint should crash immediately, not half-way
through a streaming job.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_SAMPLE = PROJECT_ROOT / "data" / "sample"
CHECKPOINTS = PROJECT_ROOT / "checkpoints"


def _req(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Required config '{key}' is missing. Copy .env.example to .env and fill it in.")
    return val


# ---- Kafka ----
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_REVIEWS = os.getenv("KAFKA_TOPIC_REVIEWS", "reviews.raw")

# ---- MinIO / S3 ----
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
# Credentials have no default on purpose: a missing .env must fail here, not
# silently connect with a well-known password.
S3_ACCESS_KEY = _req("S3_ACCESS_KEY")
S3_SECRET_KEY = _req("S3_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")

# ---- Elasticsearch ----
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

# ---- Postgres ----
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "catalog")
PG_USER = os.getenv("PG_USER", "bigdata")
PG_PASSWORD = _req("PG_PASSWORD")
PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

# ---- AI ----
EMBED_MODEL = os.getenv("EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
LLM_MODEL = os.getenv("LLM_MODEL", "claude-haiku-4-5-20251001")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# ---- Dataset ----
CATEGORY = os.getenv("CATEGORY", "All_Beauty")
