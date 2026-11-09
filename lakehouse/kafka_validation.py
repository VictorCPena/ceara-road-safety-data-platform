"""Bounded historical PRF replay through a real Kafka broker; never a live PRF feed."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyspark
from confluent_kafka.admin import AdminClient, NewTopic
from deltalake import DeltaTable
from pyspark.errors import StreamingQueryException
from pyspark.sql import SparkSession, functions as F

from lakehouse.core import normalize
from lakehouse.streaming import produce, run_stream


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def snapshot(root: Path) -> pd.DataFrame:
    return DeltaTable(str(root / "silver")).to_pandas().sort_values("id").reset_index(drop=True)


def dlq_rows(root: Path) -> int:
    return sum(
        bool(line.strip())
        for path in (root / "dlq").glob("part-*.json")
        for line in path.read_text().splitlines()
    )


def kafka_spark():
    if pyspark.__version__ != "3.5.8":
        raise RuntimeError("This validation requires the pinned PySpark 3.5.8 environment")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    return (
        SparkSession.builder.master("local[2]")
        .appName("PRF-Kafka-Validation")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8")
        .getOrCreate()
    )


def validate(csv: Path, brokers: str, output: Path, rows: int = 20) -> dict:
    if not csv.is_file():
        raise FileNotFoundError(f"PRF CSV not found: {csv}; run the batch ingestion first")
    if not 2 <= rows <= 1000:
        raise ValueError("Use 2..1000 rows for this bounded local validation")
    output.mkdir(parents=True, exist_ok=False)
    root = output / "lake"
    checkpoint = output / "checkpoint"
    topic = "prf-validation-" + uuid.uuid4().hex[:12]
    report = {
        "status": "running",
        "started_at_utc": datetime.now(UTC).isoformat(),
        "dataset": "Historical PRF CSV replay; not a live PRF feed",
        "source_csv": str(csv.resolve()),
        "source_sha256": hashlib.sha256(csv.read_bytes()).hexdigest(),
        "topic": topic,
        "brokers": brokers,
        "output": str(output.resolve()),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "pyspark": pyspark.__version__,
        "requested_rows": rows,
        "checks": {},
        "limitations": [
            "Local single-broker, single-node study; not an availability or scale benchmark",
            "Injected revisions/invalid envelope are synthetic; base rows come from the PRF CSV",
            "Failure is injected in foreachBatch after Delta/Gold commit; no broker outage or process kill tested",
            "At-least-once foreachBatch with idempotent Delta effects; no global exactly-once claim",
            "Two-day watermark; older corrections require batch reconciliation",
            "Topic and output are retained for inspection; each invocation uses a fresh topic/root",
        ],
    }
    started = time.perf_counter()
    spark = None
    try:
        admin = AdminClient({"bootstrap.servers": brokers, "broker.address.family": "v4"})
        futures = admin.create_topics(
            [NewTopic(topic, num_partitions=1, replication_factor=1)],
            operation_timeout=15,
            request_timeout=20,
        )
        futures[topic].result(timeout=25)
        report["checks"]["topic_created"] = True
        spark = kafka_spark()
        _, good, _ = normalize(spark, csv)
        # Prefer Ceará and use a compact time window to avoid late-event ambiguity.
        selected = (
            good.filter((F.col("uf") == "CE") & F.col("br").isNotNull())
            .orderBy(F.col("event_time").desc(), F.col("id"))
            .limit(rows)
            .toPandas()
        )
        require(len(selected) >= 2, "Not enough valid Ceará records in the CSV")
        cutoff = selected.event_time.max() - pd.Timedelta(days=2)
        selected = selected[selected.event_time > cutoff].copy()
        require(len(selected) >= 2, "Need two Ceará records within the same two-day window")
        rows = len(selected)
        selected["event_time"] = pd.to_datetime(selected.event_time).astype("datetime64[us]")
        selected["version"] = 1
        selected["deleted"] = False
        events = selected.to_dict("records")
        (output / "replayed_events.json").write_text(json.dumps(events, default=str, indent=2))
        invalid = {**events[0], "id": "invalid-envelope-probe", "mortos": -1}
        produce([*events, events[0], invalid], brokers, topic=topic, ipv4=True)
        report["first_progress"] = run_stream(spark, root, brokers, checkpoint, kafka=True, topic=topic)
        expected = selected.sort_values("id").reset_index(drop=True)
        pd.testing.assert_frame_equal(snapshot(root), expected, check_dtype=False, check_like=True)
        require(dlq_rows(root) == 1, "Expected exactly one invalid-envelope DLQ record")
        report["checks"].update(
            initial_rows_match_source=True, duplicate_deduplicated=True, invalid_envelope_in_dlq=True
        )

        # A deliberately simulated correction exercises failed-batch replay.
        correction = {**events[0], "version": 2, "deleted": True}
        produce([events[0], correction], brokers, topic=topic, ipv4=True)
        expected.loc[expected.id == correction["id"], ["version", "deleted"]] = [2, True]
        try:
            run_stream(spark, root, brokers, checkpoint, kafka=True, topic=topic, fail_after_commit=True)
        except StreamingQueryException as exc:
            if "Injected failure after Delta commit" not in str(exc):
                raise
            report["checks"]["injected_failure_observed"] = True
        else:
            raise AssertionError("The controlled failure did not occur")
        pd.testing.assert_frame_equal(snapshot(root), expected, check_dtype=False, check_like=True)
        report["checks"]["delta_commit_survived_failure"] = True

        report["recovery_progress"] = run_stream(spark, root, brokers, checkpoint, kafka=True, topic=topic)
        pd.testing.assert_frame_equal(snapshot(root), expected, check_dtype=False, check_like=True)
        require(dlq_rows(root) == 1, "Recovery duplicated DLQ records")
        require(
            sum(int(p["numInputRows"]) for p in report["recovery_progress"]) > 0,
            "Recovery did not replay the uncommitted Kafka batch",
        )
        gold = DeltaTable(str(root / "gold")).to_pandas()
        require(int(gold.accidents.sum()) == rows - 1, "Gold did not apply the simulated tombstone")
        require(any((checkpoint / "main" / "commits").glob("[0-9]*")), "No main checkpoint commits")
        report["checks"].update(
            checkpoint_replay_idempotent=True, tombstone_applied=True, gold_reconciled=True
        )
        report["empty_restart_progress"] = run_stream(
            spark, root, brokers, checkpoint, kafka=True, topic=topic
        )
        pd.testing.assert_frame_equal(snapshot(root), expected, check_dtype=False, check_like=True)
        require(
            sum(int(p["numInputRows"]) for p in report["empty_restart_progress"]) == 0,
            "An empty restart unexpectedly consumed data",
        )
        report["checks"]["empty_restart_no_reconsumption"] = True
        report.update(
            status="passed",
            source_rows=rows,
            first_messages=rows + 2,
            correction_messages=2,
            silver_rows=len(expected),
            live_gold_accidents=rows - 1,
            dlq_rows=dlq_rows(root),
        )
        return report
    except BaseException as exc:
        report.update(status="failed", error_type=type(exc).__name__, error=str(exc)[:2000])
        raise
    finally:
        report["seconds"] = time.perf_counter() - started
        (output / "result.json").write_text(json.dumps(report, indent=2, default=str))
        print(json.dumps({"event": "kafka_validation_result", **report}, indent=2, default=str), flush=True)
        if spark is not None:
            spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=Path("data/raw/datatran2024.csv"))
    parser.add_argument("--brokers", default="127.0.0.1:19092")
    parser.add_argument("--rows", type=int, default=20)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    parser.add_argument("--output", type=Path, default=Path("work/kafka-validation") / stamp)
    args = parser.parse_args()
    validate(args.csv, args.brokers, args.output, args.rows)


if __name__ == "__main__":
    main()
