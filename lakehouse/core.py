"""Spark transformations with transactional Delta storage (delta-rs writer)."""

from __future__ import annotations
import hashlib
import json
import os
import time
from pathlib import Path
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from pyspark.sql import SparkSession, functions as F


def spark_session():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    return (
        SparkSession.builder.master("local[2]")
        .appName("PRF-Lakehouse")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def merge(path: Path, frame: pd.DataFrame, keys: list[str], versioned: bool = False):
    if frame.empty:
        return
    table = pa.Table.from_pandas(frame, preserve_index=False)
    if not (path / "_delta_log").exists():
        write_deltalake(str(path), table, partition_by=["uf"] if "uf" in frame else None)
        return
    predicate = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    update = "s.version > t.version" if versioned else None
    (
        DeltaTable(str(path))
        .merge(table, predicate, source_alias="s", target_alias="t")
        .when_matched_update_all(predicate=update)
        .when_not_matched_insert_all()
        .execute()
    )


def normalize(spark, csv: Path):
    raw = spark.read.option("header", True).option("sep", ";").option("encoding", "ISO-8859-1").csv(str(csv))
    required = {"id", "data_inversa", "horario", "uf", "br", "municipio", "mortos", "pessoas"}
    if not required.issubset(raw.columns):
        raise ValueError(f"Missing PRF fields: {required - set(raw.columns)}")
    typed = raw.select(
        F.col("id").cast("long").cast("string").alias("id"),
        F.to_timestamp(F.concat_ws(" ", "data_inversa", "horario"), "yyyy-MM-dd HH:mm:ss").alias(
            "event_time"
        ),
        F.upper(F.trim("uf")).alias("uf"),
        F.col("br").cast("int").alias("br"),
        F.trim("municipio").alias("municipio"),
        F.col("mortos").cast("int").alias("mortos"),
        F.col("pessoas").cast("int").alias("pessoas"),
    )
    valid = (
        F.col("id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("uf").rlike("^[A-Z]{2}$")
        & (F.col("mortos") >= 0)
        & (F.col("pessoas") >= F.col("mortos"))
        & F.col("municipio").isNotNull()
    )
    good, bad = typed.filter(valid), typed.filter(~F.coalesce(valid, F.lit(False)))
    if good.groupBy("id").count().filter("count > 1").limit(1).count():
        raise ValueError("Duplicate accident IDs in one snapshot; resolve before ingest")
    return raw, good, bad


def refresh_gold(root: Path):
    silver = DeltaTable(str(root / "silver")).to_pandas()
    live = silver[~silver.deleted].copy()
    live["year"] = live.event_time.dt.year.astype("int32")
    gold = (
        live.groupby(["uf", "year", "br"], dropna=False)
        .agg(accidents=("id", "size"), deaths=("mortos", "sum"), people=("pessoas", "sum"))
        .reset_index()
    )
    write_deltalake(
        str(root / "gold"),
        pa.Table.from_pandas(gold, preserve_index=False),
        mode="overwrite",
        partition_by=["uf"],
    )
    live[["id", "event_time", "uf", "br", "mortos", "pessoas"]].to_parquet(
        root / "fact_accident.parquet", index=False
    )
    live[["uf", "municipio"]].drop_duplicates().to_parquet(root / "dim_location.parquet", index=False)
    return gold


def _ingest(csv: Path, root: Path, spark=None, version: int = 1, fail_after_silver: bool = False):
    if version < 1:
        raise ValueError("Snapshot version must be a positive monotonic source revision")
    root.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    digest = hashlib.sha256(csv.read_bytes()).hexdigest()
    owns_spark = spark is None
    spark = spark or spark_session()
    try:
        raw, good, bad = normalize(spark, csv)
        bronze = raw.toPandas().astype(str)
        bronze["source_sha256"] = digest
        bronze["row_key"] = [
            hashlib.sha256((digest + str(i)).encode()).hexdigest() for i in range(len(bronze))
        ]
        merge(root / "bronze", bronze, ["row_key"])
        rejected = bad.toPandas()
        rejected.to_parquet(root / "quarantine.parquet", index=False)
        frame = good.toPandas()
        frame["event_time"] = pd.to_datetime(frame.event_time).astype("datetime64[us]")
        frame["version"] = version
        frame["deleted"] = False
        check_revisions(root / "silver", frame)
        merge(root / "silver", frame, ["id"], versioned=True)
        if fail_after_silver:
            raise RuntimeError("Injected failure after committed silver; rerun repairs gold")
        gold = refresh_gold(root)
        result = {
            "source_sha256": digest,
            "accepted": len(frame),
            "quarantined": len(rejected),
            "gold_rows": len(gold),
            "seconds": time.perf_counter() - started,
            "engine": "PySpark local[2] transform + delta-rs transactions",
            "source_version": version,
        }
        (root / "last_success.json").write_text(json.dumps(result, indent=2))
        (root / "metrics.prom").write_text(
            f"prf_accepted_rows {len(frame)}\nprf_quarantined_rows {len(rejected)}\n"
        )
        print(json.dumps({"event": "batch_committed", **result}), flush=True)
        return result
    finally:
        if owns_spark:
            spark.stop()


def _apply_events(root: Path, events: pd.DataFrame):
    required = {"id", "event_time", "uf", "br", "municipio", "mortos", "pessoas", "version", "deleted"}
    if not required.issubset(events):
        raise ValueError("Invalid event schema")
    if events[list(required)].isna().any().any():
        raise ValueError("Null event fields")
    if (events.version < 1).any() or (events.mortos < 0).any() or (events.pessoas < events.mortos).any():
        raise ValueError("Invalid event values")
    # Reject conflicting same-ID/same-version messages rather than choose arbitrary winner.
    unique = events.drop_duplicates()
    if unique.duplicated(["id", "version"]).any():
        raise ValueError("Conflicting payload for one event revision")
    latest = unique.sort_values("version").drop_duplicates("id", keep="last").copy()
    latest["event_time"] = pd.to_datetime(latest.event_time).astype("datetime64[us]")
    check_revisions(root / "silver", latest)
    merge(root / "silver", latest, ["id"], versioned=True)
    refresh_gold(root)


def check_revisions(path: Path, incoming: pd.DataFrame):
    if not (path / "_delta_log").exists():
        return
    current = DeltaTable(str(path)).to_pandas()
    pairs = incoming.merge(current, on=["id", "version"], suffixes=("_new", "_old"))
    for name in ["event_time", "uf", "br", "municipio", "mortos", "pessoas", "deleted"]:
        if name + "_new" in pairs and not pairs[name + "_new"].equals(
            pairs[name + "_old"].rename(name + "_new")
        ):
            # equals also checks dtypes, so compare values after normalizing representation.
            left = pairs[name + "_new"].astype(str).to_numpy()
            right = pairs[name + "_old"].astype(str).to_numpy()
            if (left != right).any():
                raise ValueError("Conflicting payload for a committed revision; increment source version")


def writer_lock(root: Path):
    from contextlib import contextmanager
    import fcntl

    @contextmanager
    def acquire():
        root.mkdir(parents=True, exist_ok=True)
        with (root / ".writer.lock").open("a") as file:
            deadline = time.monotonic() + 30
            while True:
                try:
                    fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() > deadline:
                        raise TimeoutError("Another local writer holds the lakehouse lock") from None
                    time.sleep(0.05)
            try:
                yield
            finally:
                fcntl.flock(file, fcntl.LOCK_UN)

    return acquire()


def ingest(csv: Path, root: Path, **kwargs):
    with writer_lock(root):
        try:
            return _ingest(csv, root, **kwargs)
        except Exception as exc:
            (root / "last_failure.json").write_text(
                json.dumps({"type": type(exc).__name__, "message": str(exc)})
            )
            raise


def apply_events(root: Path, events: pd.DataFrame):
    with writer_lock(root):
        return _apply_events(root, events)
