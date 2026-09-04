"""Prove the Iceberg-on-MinIO catalog works before building pipeline jobs on it.

Creates a table, writes twice, and reads the snapshot history back -- which also
demonstrates the time-travel capability the demo relies on.
"""
from __future__ import annotations

from src.common.spark import CATALOG, build

spark = build("verify-iceberg")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.smoke")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.smoke.t PURGE")
spark.sql(f"CREATE TABLE {CATALOG}.smoke.t (id BIGINT, note STRING) USING iceberg")

spark.range(5).selectExpr("id", "'first' AS note") \
     .writeTo(f"{CATALOG}.smoke.t").append()
spark.range(5, 8).selectExpr("id", "'second' AS note") \
     .writeTo(f"{CATALOG}.smoke.t").append()

print("\nrows:")
spark.table(f"{CATALOG}.smoke.t").orderBy("id").show()

print("snapshot history (this is what makes time travel possible):")
spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {CATALOG}.smoke.t.snapshots") \
     .show(truncate=False)

first = spark.sql(
    f"SELECT snapshot_id FROM {CATALOG}.smoke.t.snapshots ORDER BY committed_at"
).first()[0]
n_then = spark.read.option("snapshot-id", first).table(f"{CATALOG}.smoke.t").count()
n_now = spark.table(f"{CATALOG}.smoke.t").count()
print(f"rows at first snapshot: {n_then}   rows now: {n_now}")
assert (n_then, n_now) == (5, 8), f"unexpected counts: {n_then}, {n_now}"

spark.sql(f"DROP TABLE {CATALOG}.smoke.t PURGE")
spark.stop()
print("\nIceberg on MinIO with a Postgres JDBC catalog: verified.")
