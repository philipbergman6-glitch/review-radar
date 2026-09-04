"""End-to-end connectivity check for the whole local stack.

Proves that every service is not just "up" in Docker but actually usable from
Python with our credentials. Run this before any pipeline work -- it turns a
confusing mid-job failure into an immediate, specific error.

Exit code 0 = everything green, 1 = at least one component failed.
"""
from __future__ import annotations

import sys
import time
import uuid

from src.common import config as C

OK, FAIL = "  OK  ", " FAIL "
results: list[tuple[str, bool, str]] = []


def record(name: str, fn) -> None:
    t0 = time.time()
    try:
        detail = fn()
        results.append((name, True, f"{detail}  ({time.time() - t0:.2f}s)"))
    except Exception as exc:  # noqa: BLE001 - we want to report, not crash
        results.append((name, False, f"{type(exc).__name__}: {exc}"))


# ------------------------------------------------------------------ Kafka ----
def check_kafka() -> str:
    from confluent_kafka import Consumer, Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    topic = f"healthcheck.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": C.KAFKA_BOOTSTRAP})
    md = admin.list_topics(timeout=10)
    n_brokers = len(md.brokers)

    fs = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    fs[topic].result(timeout=15)

    p = Producer({"bootstrap.servers": C.KAFKA_BOOTSTRAP})
    p.produce(topic, key=b"k", value=b"roundtrip")
    if p.flush(15) != 0:
        raise RuntimeError("producer failed to flush")

    c = Consumer({"bootstrap.servers": C.KAFKA_BOOTSTRAP,
                  "group.id": f"hc-{uuid.uuid4().hex[:8]}",
                  "auto.offset.reset": "earliest"})
    c.subscribe([topic])
    deadline, got = time.time() + 30, None
    while time.time() < deadline and got is None:
        msg = c.poll(1.0)
        if msg is not None and not msg.error():
            got = msg.value()
    c.close()
    admin.delete_topics([topic])

    if got != b"roundtrip":
        raise RuntimeError(f"round-trip mismatch: {got!r}")
    return f"{n_brokers} broker(s), produce+consume round-trip verified"


# ------------------------------------------------------------------ MinIO ----
def check_minio() -> str:
    import boto3

    s3 = boto3.client("s3", endpoint_url=C.S3_ENDPOINT,
                      aws_access_key_id=C.S3_ACCESS_KEY,
                      aws_secret_access_key=C.S3_SECRET_KEY,
                      region_name="us-east-1")
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if C.S3_BUCKET not in buckets:
        raise RuntimeError(f"bucket '{C.S3_BUCKET}' missing (found: {buckets})")
    key = f"_healthcheck/{uuid.uuid4().hex}.txt"
    s3.put_object(Bucket=C.S3_BUCKET, Key=key, Body=b"ok")
    body = s3.get_object(Bucket=C.S3_BUCKET, Key=key)["Body"].read()
    s3.delete_object(Bucket=C.S3_BUCKET, Key=key)
    if body != b"ok":
        raise RuntimeError("object round-trip mismatch")
    return f"buckets={buckets}, put/get/delete verified"


# ---------------------------------------------------------- Elasticsearch ----
def check_elasticsearch() -> str:
    from elasticsearch import Elasticsearch

    es = Elasticsearch(C.ES_HOST, request_timeout=30)
    info = es.info()
    health = es.cluster.health()

    idx = f"healthcheck-{uuid.uuid4().hex[:8]}"
    # Exercise dense_vector explicitly: the whole semantic-search plan depends on it.
    es.indices.create(index=idx, mappings={"properties": {
        "text": {"type": "text"},
        "vec": {"type": "dense_vector", "dims": 4, "index": True, "similarity": "cosine"},
    }})
    es.index(index=idx, id="1", document={"text": "hello", "vec": [1.0, 0.0, 0.0, 0.0]},
             refresh="wait_for")
    hits = es.search(index=idx, knn={"field": "vec", "query_vector": [1.0, 0.0, 0.0, 0.0],
                                     "k": 1, "num_candidates": 10})["hits"]["hits"]
    es.indices.delete(index=idx)
    if not hits:
        raise RuntimeError("kNN query returned no hits")
    return (f"v{info['version']['number']}, status={health['status']}, "
            f"dense_vector kNN verified")


# --------------------------------------------------------------- Postgres ----
def check_postgres() -> str:
    import psycopg

    dsn = (f"host={C.PG_HOST} port={C.PG_PORT} dbname={C.PG_DB} "
           f"user={C.PG_USER} password={C.PG_PASSWORD}")
    with psycopg.connect(dsn, connect_timeout=10) as conn, conn.cursor() as cur:
        cur.execute("SELECT version()")
        ver = cur.fetchone()[0].split(",")[0]
        cur.execute("""SELECT table_name FROM information_schema.tables
                       WHERE table_schema = 'public' ORDER BY table_name""")
        tables = [r[0] for r in cur.fetchall()]
    for required in ("products", "pipeline_runs"):
        if required not in tables:
            raise RuntimeError(f"table '{required}' missing (found: {tables})")
    return f"{ver}, tables={tables}"


# ------------------------------------------------------------------ Spark ----
def check_spark() -> str:
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.master("local[2]").appName("healthcheck")
             .config("spark.ui.enabled", "false").getOrCreate())
    try:
        n = spark.range(1000).filter("id % 7 = 0").count()
        if n != 143:
            raise RuntimeError(f"unexpected count {n}")
        jvm_ver = spark.sparkContext._jvm.System.getProperty("java.version")
        return f"Spark {spark.version} on Java {jvm_ver}"
    finally:
        spark.stop()


def main() -> None:
    print("Checking local stack...\n")
    record("Kafka", check_kafka)
    record("MinIO (S3)", check_minio)
    record("Elasticsearch", check_elasticsearch)
    record("PostgreSQL", check_postgres)
    record("Spark", check_spark)

    print()
    for name, ok, detail in results:
        print(f"[{OK if ok else FAIL}] {name:<15} {detail}")

    failed = [n for n, ok, _ in results if not ok]
    print()
    if failed:
        print(f"{len(failed)} component(s) failed: {', '.join(failed)}")
        sys.exit(1)
    print("All components healthy.")


if __name__ == "__main__":
    main()
