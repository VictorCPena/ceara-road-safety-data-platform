"""Native Spark/Delta execution, distinct from the lightweight delta-rs writer."""

import argparse
import hashlib
import json
import os
import time
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
from lakehouse.core import normalize
from lakehouse.fetch import download


def run(csv: Path, root: Path, jars: Path):
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    files = []
    for artifact in ["delta-spark_2.12", "delta-storage"]:
        jar = jars / f"{artifact}-3.2.1.jar"
        if not jar.exists():
            download(
                f"https://repo.maven.apache.org/maven2/io/delta/{artifact}/3.2.1/{artifact}-3.2.1.jar",
                jar,
                20_000_000,
            )
        files.append(str(jar.resolve()))
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("Native-Spark-Delta")
        .config("spark.jars", ",".join(files))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .getOrCreate()
    )
    root.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()

    def upsert(name, frame, key, versioned=False):
        path = str(root / name)
        if not DeltaTable.isDeltaTable(spark, path):
            frame.write.format("delta").partitionBy("uf").save(path)
        else:
            update = DeltaTable.forPath(spark, path).alias("t").merge(frame.alias("s"), f"t.{key}=s.{key}")
            if versioned:
                update = update.whenMatchedUpdateAll(condition="s.version>t.version")
            else:
                update = update.whenMatchedUpdateAll()
            update.whenNotMatchedInsertAll().execute()

    try:
        raw, good, bad = normalize(spark, csv)
        digest = hashlib.sha256(csv.read_bytes()).hexdigest()
        bronze = raw.withColumn("source_sha256", F.lit(digest)).withColumn(
            "row_key",
            F.sha2(F.concat(F.lit(digest), F.to_json(F.struct(*[F.col(c) for c in raw.columns]))), 256),
        )
        upsert("bronze", bronze, "row_key")
        silver = good.withColumn("version", F.lit(1).cast("long")).withColumn("deleted", F.lit(False))
        upsert("silver", silver, "id", True)
        accepted = good.count()
        rejected = bad.count()
        live = spark.read.format("delta").load(str(root / "silver")).filter("NOT deleted")
        gold = live.groupBy("uf", F.year("event_time").alias("year"), "br").agg(
            F.count("*").alias("accidents"), F.sum("mortos").alias("deaths")
        )
        gold.write.format("delta").mode("overwrite").partitionBy("uf").save(str(root / "gold"))
        before = live.count()
        upsert("silver", silver, "id", True)
        after = spark.read.format("delta").load(str(root / "silver")).count()
        if before != after:
            raise AssertionError("Native Delta replay was not idempotent")
        # Revisions and delete applied in a separate transaction demonstration table.
        demo = root / "transaction_demo"
        silver.limit(3).write.format("delta").mode("overwrite").save(str(demo))
        table = DeltaTable.forPath(spark, str(demo))
        first = table.toDF().orderBy("id").first()["id"]
        table.update(condition=F.col("id") == first, set={"version": "2", "mortos": "mortos + 1"})
        table.delete(F.col("id") == first)
        history = [
            r.asDict(recursive=True)
            for r in table.history().select("version", "operation", "operationMetrics").collect()
        ]
        result = {
            "accepted": accepted,
            "quarantined": rejected,
            "replay_rows_before": before,
            "replay_rows_after": after,
            "gold_rows": gold.count(),
            "seconds": time.perf_counter() - started,
            "source_sha256": digest,
            "engine": "Apache Spark 3.5.8 local[2] + native Delta Lake 3.2.1",
            "transaction_demo_history": history,
            "limitations": "Single-node execution; Spark engine used distributed operators locally. No multi-node scale or Databricks deployment claimed.",
        }
        Path("evidence/native_spark_delta.json").write_text(json.dumps(result, indent=2, default=str))
        return result
    finally:
        spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--csv", type=Path, required=True)
    p.add_argument("--root", type=Path, default=Path("work/native_delta"))
    p.add_argument("--jars", type=Path, default=Path("work/jars"))
    a = p.parse_args()
    print(json.dumps(run(a.csv, a.root, a.jars), indent=2, default=str))
