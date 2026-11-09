import json
import pandas as pd
import pytest
from deltalake import DeltaTable
from lakehouse.core import ingest, spark_session, apply_events
from lakehouse.streaming import run_stream


@pytest.fixture(scope="module")
def spark():
    s = spark_session()
    yield s
    s.stop()


@pytest.fixture
def csv(tmp_path):
    path = tmp_path / "input.csv"
    pd.DataFrame(
        [
            {
                "id": 1,
                "data_inversa": "2024-01-01",
                "horario": "12:00:00",
                "uf": "CE",
                "br": 116,
                "municipio": "FORTALEZA",
                "mortos": 0,
                "pessoas": 2,
            },
            {
                "id": 2,
                "data_inversa": "2024-01-02",
                "horario": "12:00:00",
                "uf": "CE",
                "br": 222,
                "municipio": "CAUCAIA",
                "mortos": 1,
                "pessoas": 3,
            },
            {
                "id": 3,
                "data_inversa": "bad",
                "horario": "bad",
                "uf": "CE",
                "br": 222,
                "municipio": "CAUCAIA",
                "mortos": 0,
                "pessoas": 1,
            },
        ]
    ).to_csv(path, sep=";", index=False, encoding="latin1")
    return path


def test_replay_failure_merge_delete_history(csv, tmp_path, spark):
    root = tmp_path / "lake"
    r = ingest(csv, root, spark=spark)
    assert r["accepted"] == 2 and r["quarantined"] == 1
    original = DeltaTable(str(root / "gold")).to_pandas().deaths.sum()
    ingest(csv, root, spark=spark)
    assert DeltaTable(str(root / "bronze")).to_pyarrow_table().num_rows == 3
    assert DeltaTable(str(root / "silver")).to_pyarrow_table().num_rows == 2
    with pytest.raises(RuntimeError):
        ingest(csv, root, spark=spark, version=2, fail_after_silver=True)
    ingest(csv, root, spark=spark, version=2)
    assert DeltaTable(str(root / "gold")).to_pandas().deaths.sum() == original
    before = DeltaTable(str(root / "silver")).version()
    events = DeltaTable(str(root / "silver")).to_pandas().sort_values("id")
    events["version"] = 3
    events.loc[events.id == "1", "mortos"] = 1
    events.loc[events.id == "2", "deleted"] = True
    apply_events(root, events)
    current = DeltaTable(str(root / "silver")).to_pandas()
    assert current.deleted.sum() == 1
    assert DeltaTable(str(root / "gold")).to_pandas().accidents.sum() == 1
    assert DeltaTable(str(root / "silver"), version=before).to_pandas().deleted.sum() == 0
    events["version"] = 1
    events["deleted"] = False
    apply_events(root, events)
    assert DeltaTable(str(root / "silver")).to_pandas().deleted.sum() == 1
    DeltaTable(str(root / "silver")).delete("deleted = true")
    assert DeltaTable(str(root / "silver")).to_pyarrow_table().num_rows == 1


def test_stream_restart_dedup_dlq(csv, tmp_path, spark):
    root = tmp_path / "lake"
    ingest(csv, root, spark=spark)
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    checkpoint = tmp_path / "checkpoint"
    row = {
        "id": "1",
        "event_time": "2024-01-02T12:00:00",
        "uf": "CE",
        "br": 116,
        "municipio": "FORTALEZA",
        "mortos": 1,
        "pessoas": 2,
        "version": 2,
        "deleted": False,
    }
    (incoming / "one.json").write_text(
        json.dumps(row) + "\n" + json.dumps(row) + "\n" + json.dumps({"id": None})
    )
    run_stream(spark, root, str(incoming), checkpoint)
    first = DeltaTable(str(root / "silver")).to_pandas().sort_values("id").reset_index(drop=True)
    run_stream(spark, root, str(incoming), checkpoint)
    pd.testing.assert_frame_equal(
        first, DeltaTable(str(root / "silver")).to_pandas().sort_values("id").reset_index(drop=True)
    )
    assert first[first.id == "1"].mortos.iloc[0] == 1
    assert list((root / "dlq").glob("*.json"))
    assert list((checkpoint / "main" / "commits").glob("*"))
    newer = {**row, "event_time": "2024-01-10T12:00:00", "version": 3}
    (incoming / "two.json").write_text(json.dumps(newer) + "\n")
    run_stream(spark, root, str(incoming), checkpoint)
    late = {**row, "id": "2", "event_time": "2024-01-01T12:00:00", "mortos": 0, "version": 4}
    (incoming / "three.json").write_text(json.dumps(late) + "\n")
    run_stream(spark, root, str(incoming), checkpoint)
    # A truly late correction is excluded from streaming; batch reconciliation must recover it.
    final = DeltaTable(str(root / "silver")).to_pandas()
    assert final[final.id == "2"].version.iloc[0] == 1


def test_conflicting_events_rejected(csv, tmp_path, spark):
    root = tmp_path / "lake"
    ingest(csv, root, spark=spark)
    e = DeltaTable(str(root / "silver")).to_pandas().iloc[:1].copy()
    e["version"] = 2
    other = e.copy()
    other["mortos"] = 1
    with pytest.raises(ValueError, match="Conflicting"):
        apply_events(root, pd.concat([e, other]))


def test_committed_revision_conflict(csv, tmp_path, spark):
    root = tmp_path / "lake"
    ingest(csv, root, spark=spark)
    e = DeltaTable(str(root / "silver")).to_pandas().iloc[:1].copy()
    e["mortos"] = e["mortos"] + 1
    with pytest.raises(ValueError, match="committed revision"):
        apply_events(root, e)


def test_stream_does_not_hide_payload_conflict(csv, tmp_path, spark):
    from pyspark.errors import StreamingQueryException

    root = tmp_path / "lake"
    ingest(csv, root, spark=spark)
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    row = {
        "id": "1",
        "event_time": "2024-01-02T12:00:00",
        "uf": "CE",
        "br": 116,
        "municipio": "FORTALEZA",
        "mortos": 0,
        "pessoas": 2,
        "version": 2,
        "deleted": False,
    }
    other = {**row, "mortos": 1}
    (incoming / "conflict.json").write_text(json.dumps(row) + "\n" + json.dumps(other) + "\n")
    with pytest.raises(StreamingQueryException, match="Conflicting"):
        run_stream(spark, root, str(incoming), tmp_path / "checkpoint")
    assert not spark.streams.active
