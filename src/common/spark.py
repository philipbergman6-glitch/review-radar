"""Shared SparkSession builder wired for Iceberg-on-MinIO and Kafka.

Catalog choice, which is worth being able to defend:

An Iceberg catalog has one job -- to hold the pointer to each table's current
metadata file and to swap that pointer *atomically* when a write commits. The
`hadoop` catalog implements that swap with a file rename, and object stores have
no atomic rename, so two concurrent writers can both believe they won. We
therefore use the `jdbc` catalog: the pointer lives in a PostgreSQL row and the
swap is a transactional UPDATE, which is genuinely atomic. Data files still live
in MinIO. Metadata in the RDBMS, data in the object store.

Jars are resolved by Maven coordinates at session start (cached in ~/.ivy2 after
the first run) so the repository stays free of vendored binaries.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from src.common import config as C

ICEBERG_VERSION = "1.6.1"
SCALA_BINARY = "2.12"

PACKAGES = ",".join([
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_{SCALA_BINARY}:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_BINARY}:3.5.3",
    "org.postgresql:postgresql:42.7.4",
])

CATALOG = "lake"
WAREHOUSE = f"s3://{C.S3_BUCKET}/iceberg"


def build(app_name: str, *, cores: str = "local[6]", driver_memory: str = "4g",
          shuffle_partitions: int = 12) -> SparkSession:
    # The AWS SDK inside iceberg-aws-bundle reads credentials from the environment.
    os.environ.setdefault("AWS_ACCESS_KEY_ID", C.S3_ACCESS_KEY)
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", C.S3_SECRET_KEY)
    os.environ.setdefault("AWS_REGION", "us-east-1")

    b = (SparkSession.builder
         .master(cores)
         .appName(app_name)
         .config("spark.jars.packages", PACKAGES)
         .config("spark.driver.memory", driver_memory)
         .config("spark.sql.session.timeZone", "UTC")
         # Local single-machine run: the default 200 shuffle partitions creates
         # hundreds of tiny tasks and tiny files. 12 matches the core count.
         .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         # ---- Iceberg catalog: metadata in Postgres, data in MinIO ----
         .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
         .config(f"spark.sql.catalog.{CATALOG}.type", "jdbc")
         .config(f"spark.sql.catalog.{CATALOG}.uri", C.PG_JDBC_URL)
         .config(f"spark.sql.catalog.{CATALOG}.jdbc.user", C.PG_USER)
         .config(f"spark.sql.catalog.{CATALOG}.jdbc.password", C.PG_PASSWORD)
         .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
         .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
         .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", C.S3_ENDPOINT)
         # MinIO serves buckets as a path, not as a DNS subdomain.
         .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
         .config(f"spark.sql.catalog.{CATALOG}.client.region", "us-east-1"))

    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
