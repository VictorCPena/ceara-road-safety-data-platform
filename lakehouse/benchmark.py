"""Isolated operation benchmarks. Same input, aggregation and checksum in each engine."""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
import numpy as np
import pandas as pd


def operation(engine, path):
    started = time.perf_counter()
    if engine == "pandas":
        d = pd.read_parquet(path)
        r = d.groupby("uf", as_index=False).agg(accidents=("id", "size"), deaths=("mortos", "sum"))
    elif engine == "duckdb":
        import duckdb

        c = duckdb.connect()
        c.execute("SET threads=2")
        r = c.execute(
            "SELECT uf,COUNT(*) accidents,SUM(mortos) deaths FROM read_parquet(?) GROUP BY uf", [str(path)]
        ).df()
        c.close()
    else:
        from lakehouse.core import spark_session

        s = spark_session()
        r = (
            s.read.parquet(str(path))
            .groupBy("uf")
            .agg({"id": "count", "mortos": "sum"})
            .toPandas()
            .rename(columns={"count(id)": "accidents", "sum(mortos)": "deaths"})
        )
        s.stop()
    import resource

    memory = {
        "python_peak_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
        "max_child_peak_rss_mb": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024,
    }
    return {
        **memory,
        "engine": engine,
        "seconds": time.perf_counter() - started,
        "rows": int(r.accidents.sum()),
        "deaths": int(r.deaths.sum()),
        "groups": len(r),
    }


def suite(source, output):
    from deltalake import DeltaTable, write_deltalake
    import pyarrow as pa
    import psutil
    import tempfile

    records = []
    base = pd.read_parquet(source)
    with tempfile.TemporaryDirectory() as temp:
        temp = Path(temp)
        for mult in (1, 4):
            path = temp / f"input-{mult}.parquet"
            d = pd.concat([base] * mult, ignore_index=True)
            d.to_parquet(path, index=False)
            for engine in ("pandas", "duckdb", "spark"):
                for repetition in (1, 2, 3):
                    proc = subprocess.Popen(
                        [
                            sys.executable,
                            "-m",
                            "lakehouse.benchmark",
                            "--engine",
                            engine,
                            "--input",
                            str(path),
                        ],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL,
                        text=True,
                    )
                    peak = 0
                    while proc.poll() is None:
                        try:
                            ps = psutil.Process(proc.pid)
                            rss = ps.memory_info().rss
                            for child in ps.children(recursive=True):
                                try:
                                    rss += child.memory_info().rss
                                except psutil.NoSuchProcess:
                                    pass
                            peak = max(peak, rss)
                        except psutil.NoSuchProcess:
                            pass
                        time.sleep(0.02)
                    stdout, _ = proc.communicate()
                    if proc.returncode:
                        raise RuntimeError(f"Benchmark failed: {engine}")
                    result = json.loads(stdout.strip().splitlines()[-1])
                    result.update(
                        {
                            "multiplier": mult,
                            "repetition": repetition,
                            "external_tree_rss_mb": peak / 1024**2 if peak > 10_000_000 else None,
                            "throughput_rows_s": len(d) / result["seconds"],
                        }
                    )
                    records.append(result)
        partition = []
        for partitioned in (False, True):
            path = temp / f"delta-{partitioned}"
            write_deltalake(
                str(path),
                pa.Table.from_pandas(base, preserve_index=False),
                partition_by=["uf"] if partitioned else None,
            )
            times = []
            counts = []
            for _ in range(5):
                started = time.perf_counter()
                t = DeltaTable(str(path))
                table = (
                    t.to_pyarrow_table(partitions=[("uf", "=", "CE")])
                    if partitioned
                    else t.to_pyarrow_table(filters=[("uf", "=", "CE")])
                )
                times.append(time.perf_counter() - started)
                counts.append(table.num_rows)
            partition.append(
                {
                    "partitioned_by_uf": partitioned,
                    "ce_rows": counts,
                    "seconds": times,
                    "median_seconds": float(np.median(times)),
                    "files_total": len(DeltaTable(str(path)).file_uris()),
                }
            )
    output.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(records).to_csv(output / "engine_benchmarks.csv", index=False)
    result = {
        "runs": records,
        "partition_pruning": partition,
        "method": "3 cold interpreter runs per engine/size; Spark startup included; warmed OS cache uncontrolled; input is real PRF repeated 4x, not independent observations; rusage self peak and separate maximum exited-child peak; concurrent process-tree RSS unavailable in sandbox; local only, no cluster scaling inference",
    }
    (output / "benchmark.json").write_text(json.dumps(result, indent=2))
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    group = pd.DataFrame(records).groupby(["multiplier", "engine"]).seconds.median().unstack()
    group.plot(kind="bar", logy=True)
    plt.ylabel("Median wall seconds (log scale)")
    plt.xlabel("Input multiplier")
    plt.tight_layout()
    plt.savefig(output / "engine_benchmarks.png", dpi=160)
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--engine", choices=["pandas", "duckdb", "spark"])
    p.add_argument("--input", type=Path, default=Path("work/lakehouse/fact_accident.parquet"))
    a = p.parse_args()
    print(
        json.dumps(operation(a.engine, a.input)) if a.engine else json.dumps(suite(a.input, Path("evidence")))
    )
