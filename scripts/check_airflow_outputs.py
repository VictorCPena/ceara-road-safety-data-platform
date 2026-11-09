"""Cross-check committed batch, Delta, dbt artifacts and analytical tables."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
from deltalake import DeltaTable


def verify(project: Path) -> dict:
    root = project / "work/lakehouse"
    marker = json.loads((root / "last_success.json").read_text())
    digest = hashlib.sha256((project / "data/raw/datatran2024.csv").read_bytes()).hexdigest()
    if marker["source_sha256"] != digest:
        raise ValueError("Batch marker does not match current PRF CSV")
    silver = DeltaTable(str(root / "silver")).to_pandas()
    gold = DeltaTable(str(root / "gold")).to_pandas()
    bronze_rows = DeltaTable(str(root / "bronze")).to_pyarrow_table().num_rows
    accepted = marker["accepted"]
    if len(silver) != accepted or bool(silver.deleted.any()) or int(gold.accidents.sum()) != accepted:
        raise ValueError("Delta layers do not reconcile with accepted batch rows")
    if bronze_rows != accepted + marker["quarantined"]:
        raise ValueError("Bronze/quarantine counts do not reconcile")
    with duckdb.connect(str(project / "work/lakehouse_analytics_v2.duckdb"), read_only=True) as conn:
        facts = conn.execute("select count(*) from fct_accident").fetchone()[0]
        accidents, deaths = conn.execute(
            "select sum(accidents), sum(deaths) from mart_daily_safety"
        ).fetchone()
    if facts != accepted or accidents != accepted or deaths != int(gold.deaths.sum()):
        raise ValueError("dbt analytical tables do not reconcile with Delta")
    artifacts = json.loads((project / "work/dbt-target/run_results.json").read_text())["results"]
    models = [r for r in artifacts if r["unique_id"].startswith("model.")]
    tests = [r for r in artifacts if r["unique_id"].startswith("test.")]
    if len(models) != 3 or len(tests) != 7 or any(r["status"] not in {"success", "pass"} for r in artifacts):
        raise ValueError("Expected 3 successful dbt models and 7 passing data tests")
    return {
        "status": "passed",
        "source_sha256": digest,
        "accepted": accepted,
        "quarantined": marker["quarantined"],
        "bronze_rows": bronze_rows,
        "silver_rows": len(silver),
        "gold_accidents": int(gold.accidents.sum()),
        "dbt_fact_rows": facts,
        "dbt_models": len(models),
        "dbt_data_tests": len(tests),
    }


if __name__ == "__main__":
    print(json.dumps(verify(Path.cwd()), indent=2))
