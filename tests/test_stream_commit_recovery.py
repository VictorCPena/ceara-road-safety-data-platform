"""Real Spark file-source tests for the same sink used by Kafka.

These tests do not assert that a Kafka broker was exercised.
"""

import json

import pandas as pd
import pytest
from deltalake import DeltaTable
from pyspark.errors import StreamingQueryException

from lakehouse.core import spark_session
from lakehouse.streaming import run_stream


def test_failed_batch_replays_after_delta_commit(tmp_path):
    spark = spark_session()
    source = tmp_path / "source"
    source.mkdir()
    root, checkpoint = tmp_path / "lake", tmp_path / "checkpoint"
    event = {
        "id": "recovery-probe",
        "event_time": "2024-01-02T12:00:00",
        "uf": "CE",
        "br": 116,
        "municipio": "FORTALEZA",
        "mortos": 0,
        "pessoas": 2,
        "version": 1,
        "deleted": False,
    }
    (source / "one.json").write_text(json.dumps(event) + "\n" + json.dumps(event) + "\n")
    try:
        with pytest.raises(StreamingQueryException, match="Injected failure after Delta commit"):
            run_stream(spark, root, str(source), checkpoint, fail_after_commit=True)
        before = DeltaTable(str(root / "silver")).to_pandas()
        assert len(before) == 1
        assert not spark.streams.active
        progress = run_stream(spark, root, str(source), checkpoint)
        after = DeltaTable(str(root / "silver")).to_pandas()
        pd.testing.assert_frame_equal(before, after)
        assert sum(int(p["numInputRows"]) for p in progress) == 2
        assert DeltaTable(str(root / "gold")).to_pandas().accidents.sum() == 1
        assert list((checkpoint / "main" / "commits").glob("[0-9]*"))
        assert not spark.streams.active
    finally:
        spark.stop()


def test_start_failure_does_not_leave_dlq_running(tmp_path, monkeypatch):
    from pyspark.sql.streaming import DataStreamWriter

    spark = spark_session()
    source = tmp_path / "source"
    source.mkdir()
    original_start = DataStreamWriter.start
    calls = 0

    def fail_second_start(self, *args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("Controlled main startup failure")
        return original_start(self, *args, **kwargs)

    monkeypatch.setattr(DataStreamWriter, "start", fail_second_start)
    try:
        with pytest.raises(RuntimeError, match="Controlled main startup failure"):
            run_stream(spark, tmp_path / "lake", str(source), tmp_path / "checkpoint")
        assert calls == 2
        assert not spark.streams.active
    finally:
        spark.stop()
