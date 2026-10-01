from pathlib import Path

import duckdb


SILVER_GLOB = (
    "data/silver/prf/occurrence/"
    "year=*/part-000.parquet"
)


def run_query(
    connection: duckdb.DuckDBPyConnection,
    query: str,
):
    return connection.execute(
        query
    ).fetchone()[0]


def check(
    name: str,
    value,
    expected,
):
    success = value == expected

    status = (
        "PASS"
        if success
        else "FAIL"
    )

    print(
        f"{status:<6} | "
        f"{name:<40} | "
        f"actual={value} "
        f"expected={expected}"
    )

    return success


def main():

    con = duckdb.connect()

    print()
    print("=" * 90)
    print("SILVER DATA QUALITY - OCCURRENCE")
    print("=" * 90)

    # ====================================
    # TOTAL
    # ====================================

    total_rows = run_query(
        con,
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{SILVER_GLOB}'
        )
        """,
    )

    print(
        f"\nTotal de registros: "
        f"{total_rows:,}\n"
    )

    results = []

    # ====================================
    # ID NULL
    # ====================================

    null_ids = run_query(
        con,
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{SILVER_GLOB}'
        )
        WHERE accident_id IS NULL
        """,
    )

    results.append(
        check(
            "accident_id sem NULL",
            null_ids,
            0,
        )
    )

    # ====================================
    # ID DUPLICADO
    # ====================================

    duplicate_ids = run_query(
        con,
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT
                accident_id
            FROM read_parquet(
                '{SILVER_GLOB}'
            )
            GROUP BY accident_id
            HAVING COUNT(*) > 1
        )
        """,
    )

    results.append(
        check(
            "accident_id único",
            duplicate_ids,
            0,
        )
    )

    # ====================================
    # DATA
    # ====================================

    null_dates = run_query(
        con,
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{SILVER_GLOB}'
        )
        WHERE event_date IS NULL
        """,
    )

    results.append(
        check(
            "event_date sem NULL",
            null_dates,
            0,
        )
    )

    # ====================================
    # UF
    # ====================================

    invalid_states = run_query(
        con,
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{SILVER_GLOB}'
        )
        WHERE
            state IS NULL
            OR LENGTH(state) <> 2
        """,
    )

    results.append(
        check(
            "UF válida",
            invalid_states,
            0,
        )
    )

    # ====================================
    # LATITUDE
    # ====================================

    invalid_latitudes = run_query(
        con,
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{SILVER_GLOB}'
        )
        WHERE
            latitude IS NOT NULL
            AND (
                latitude < -90
                OR latitude > 90
            )
        """,
    )

    results.append(
        check(
            "latitude no intervalo",
            invalid_latitudes,
            0,
        )
    )

    # ====================================
    # LONGITUDE
    # ====================================

    invalid_longitudes = run_query(
        con,
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{SILVER_GLOB}'
        )
        WHERE
            longitude IS NOT NULL
            AND (
                longitude < -180
                OR longitude > 180
            )
        """,
    )

    results.append(
        check(
            "longitude no intervalo",
            invalid_longitudes,
            0,
        )
    )

    # ====================================
    # MÉTRICAS NEGATIVAS
    # ====================================

    negative_metrics = run_query(
        con,
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{SILVER_GLOB}'
        )
        WHERE
            deaths < 0
            OR minor_injuries < 0
            OR serious_injuries < 0
            OR injured < 0
            OR vehicles < 0
            OR people_count < 0
        """,
    )

    results.append(
        check(
            "métricas não negativas",
            negative_metrics,
            0,
        )
    )

    # ====================================
    # RESULTADO FINAL
    # ====================================

    print()
    print("=" * 90)

    passed = sum(results)
    total = len(results)

    print(
        f"Resultado: {passed}/{total} checks passaram."
    )

    if all(results):

        print(
            "STATUS: SILVER APROVADA"
        )

    else:

        print(
            "STATUS: SILVER COM PROBLEMAS"
        )

        raise SystemExit(1)


if __name__ == "__main__":
    main()
