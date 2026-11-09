"""Historical PRF replay, NOT a real-time PRF feed. File and Kafka adapters."""

import json
from pathlib import Path
from pyspark.sql import functions as F, types as T
from lakehouse.core import apply_events

SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType()),
        T.StructField("event_time", T.TimestampType()),
        T.StructField("uf", T.StringType()),
        T.StructField("br", T.IntegerType()),
        T.StructField("municipio", T.StringType()),
        T.StructField("mortos", T.IntegerType()),
        T.StructField("pessoas", T.IntegerType()),
        T.StructField("version", T.LongType()),
        T.StructField("deleted", T.BooleanType()),
    ]
)


def run_stream(
    spark,
    root: Path,
    source: str,
    checkpoint: Path,
    kafka: bool = False,
    *,
    topic: str = "prf-replay",
    fail_after_commit: bool = False,
):
    if kafka:
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", source)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        stream = raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")).select("e.*")
    else:
        stream = spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(source)
    valid = (
        F.col("id").isNotNull()
        & F.col("event_time").isNotNull()
        & (F.col("version") > 0)
        & F.col("uf").rlike("^[A-Z]{2}$")
        & F.col("br").isNotNull()
        & F.col("municipio").isNotNull()
        & F.col("deleted").isNotNull()
        & (F.col("mortos") >= 0)
        & (F.col("pessoas") >= F.col("mortos"))
    )
    # Split malformed envelopes before the event-time operator.
    dlq_writer = (
        stream.filter(~F.coalesce(valid, F.lit(False)))
        .writeStream.format("json")
        .option("path", str(root / "dlq"))
        .option("checkpointLocation", str(checkpoint / "dlq"))
        .trigger(availableNow=True)
    )
    cleaned = (
        stream.filter(valid)
        .withColumn(
            "_payload_sha", F.sha2(F.to_json(F.struct(*[F.col(field.name) for field in SCHEMA.fields])), 256)
        )
        .withWatermark("event_time", "2 days")
        .dropDuplicatesWithinWatermark(["id", "version", "_payload_sha"])
    )

    def sink(batch, batch_id):
        if not batch.isEmpty():
            apply_events(root, batch.drop("_payload_sha").toPandas())
            if fail_after_commit:
                raise RuntimeError("Injected failure after Delta commit; checkpoint batch not committed")
        print(json.dumps({"event": "stream_batch", "batch_id": batch_id}), flush=True)

    main_writer = (
        cleaned.writeStream.foreachBatch(sink)
        .option("checkpointLocation", str(checkpoint / "main"))
        .trigger(availableNow=True)
    )
    active_queries = []
    try:
        dlq = dlq_writer.start()
        active_queries.append(dlq)
        query = main_writer.start()
        active_queries.append(query)
        query.awaitTermination()
        dlq.awaitTermination()
        return query.recentProgress
    finally:
        for active in active_queries:
            if active.isActive:
                active.stop()


def produce(rows, brokers="localhost:19092", *, topic="prf-replay", ipv4=False):
    from confluent_kafka import Producer

    config = {"bootstrap.servers": brokers, "enable.idempotence": True, "message.timeout.ms": 30000}
    if ipv4:
        config["broker.address.family"] = "v4"
    producer = Producer(config)
    failures = []

    def delivered(error, message):
        if error:
            failures.append(str(error))

    for row in rows:
        row = dict(row)
        if hasattr(row.get("event_time"), "isoformat"):
            row["event_time"] = row["event_time"].isoformat()
        producer.produce(topic, key=str(row["id"]), value=json.dumps(row, default=str), callback=delivered)
        producer.poll(0)
    remaining = producer.flush(30)
    if remaining or failures:
        raise RuntimeError(f"Undelivered: {remaining}; errors: {failures}")
