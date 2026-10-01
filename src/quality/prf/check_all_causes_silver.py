import duckdb


CAUSE_GLOB = (
    "data/silver/prf/accident_cause/"
    "year=*/part-000.parquet"
)

TYPE_GLOB = (
    "data/silver/prf/accident_type/"
    "year=*/part-000.parquet"
)

ACCIDENT_GLOB = (
    "data/silver/prf/occurrence/"
    "year=*/part-000.parquet"
)


def value(
    con,
    query,
):

    return (
        con.execute(query)
        .fetchone()[0]
    )


def check(
    name,
    actual,
    expected,
):

    success = (
        actual == expected
    )

    status = (
        "PASS"
        if success
        else "FAIL"
    )

    print(
        f"{status:<6} | "
        f"{name:<60} | "
        f"actual={actual} "
        f"expected={expected}"
    )

    return success


def main():

    con = duckdb.connect()

    results = []

    # ========================================================
    # CAUSES
    # ========================================================

    print()
    print("=" * 120)
    print(
        "SILVER DATA QUALITY - ACCIDENT_CAUSE"
    )
    print("=" * 120)
    print()

    cause_total = value(
        con,
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{CAUSE_GLOB}'
        )
        """
    )

    print(
        f"Total accident_cause: "
        f"{cause_total:,}"
    )

    print()

    # --------------------------------------------------------
    # PK
    # --------------------------------------------------------

    results.append(
        check(
            "cause_record_id NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{CAUSE_GLOB}'
                )
                WHERE cause_record_id IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "cause_record_id duplicado",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT
                        cause_record_id
                    FROM read_parquet(
                        '{CAUSE_GLOB}'
                    )
                    GROUP BY
                        cause_record_id
                    HAVING COUNT(*) > 1
                )
                """
            ),
            0,
        )
    )

    # --------------------------------------------------------
    # Campos obrigatórios
    # --------------------------------------------------------

    results.append(
        check(
            "accident_id NULL em cause",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{CAUSE_GLOB}'
                )
                WHERE accident_id IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "accident_cause NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{CAUSE_GLOB}'
                )
                WHERE accident_cause IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "is_primary_cause NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{CAUSE_GLOB}'
                )
                WHERE is_primary_cause IS NULL
                """
            ),
            0,
        )
    )

    # --------------------------------------------------------
    # Natural key
    # --------------------------------------------------------

    results.append(
        check(
            "accident_id + accident_cause duplicado",
            value(
                con,
                f"""
                SELECT COUNT(*)

                FROM (

                    SELECT
                        _source_year,
                        accident_id,
                        accident_cause,
                        COUNT(*) AS n

                    FROM read_parquet(
                        '{CAUSE_GLOB}'
                    )

                    GROUP BY
                        _source_year,
                        accident_id,
                        accident_cause

                    HAVING COUNT(*) > 1
                )
                """
            ),
            0,
        )
    )

    # --------------------------------------------------------
    # FK
    # --------------------------------------------------------

    results.append(
        check(
            "causas apontando para acidente inexistente",
            value(
                con,
                f"""
                SELECT COUNT(*)

                FROM read_parquet(
                    '{CAUSE_GLOB}'
                ) c

                LEFT JOIN read_parquet(
                    '{ACCIDENT_GLOB}'
                ) a

                    ON
                        c._source_year
                        = a._source_year

                    AND
                        c.accident_id
                        = a.accident_id

                WHERE
                    a.accident_id IS NULL
                """
            ),
            0,
        )
    )

    # --------------------------------------------------------
    # Cobertura
    # --------------------------------------------------------

    results.append(
        check(
            "acidentes sem nenhuma causa",
            value(
                con,
                f"""
                SELECT COUNT(*)

                FROM read_parquet(
                    '{ACCIDENT_GLOB}'
                ) a

                LEFT JOIN (
                    SELECT DISTINCT
                        _source_year,
                        accident_id
                    FROM read_parquet(
                        '{CAUSE_GLOB}'
                    )
                ) c

                    ON
                        a._source_year
                        = c._source_year

                    AND
                        a.accident_id
                        = c.accident_id

                WHERE
                    c.accident_id IS NULL
                """
            ),
            0,
        )
    )

    # --------------------------------------------------------
    # Causa principal
    # --------------------------------------------------------

    multiple_primary_causes = value(
        con,
        f"""
        SELECT COUNT(*)

        FROM (

            SELECT
                _source_year,
                accident_id,

                SUM(
                    CASE
                        WHEN is_primary_cause
                        THEN 1
                        ELSE 0
                    END
                ) AS primary_causes

            FROM read_parquet(
                '{CAUSE_GLOB}'
            )

            GROUP BY
                _source_year,
                accident_id

            HAVING primary_causes > 1
        )
        """
    )

    results.append(
        check(
            "acidentes com mais de uma causa principal",
            multiple_primary_causes,
            0,
        )
    )

    # ========================================================
    # TYPES
    # ========================================================

    print()
    print("=" * 120)
    print(
        "SILVER DATA QUALITY - ACCIDENT_TYPE"
    )
    print("=" * 120)
    print()

    type_total = value(
        con,
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{TYPE_GLOB}'
        )
        """
    )

    print(
        f"Total accident_type: "
        f"{type_total:,}"
    )

    print()

    # --------------------------------------------------------
    # PK
    # --------------------------------------------------------

    results.append(
        check(
            "type_record_id NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{TYPE_GLOB}'
                )
                WHERE type_record_id IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "type_record_id duplicado",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT
                        type_record_id
                    FROM read_parquet(
                        '{TYPE_GLOB}'
                    )
                    GROUP BY
                        type_record_id
                    HAVING COUNT(*) > 1
                )
                """
            ),
            0,
        )
    )

    # --------------------------------------------------------
    # Campos obrigatórios
    # --------------------------------------------------------

    results.append(
        check(
            "accident_id NULL em type",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{TYPE_GLOB}'
                )
                WHERE accident_id IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "accident_type_order NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{TYPE_GLOB}'
                )
                WHERE accident_type_order IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "accident_type_order <= 0",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{TYPE_GLOB}'
                )
                WHERE accident_type_order <= 0
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "accident_type NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{TYPE_GLOB}'
                )
                WHERE accident_type IS NULL
                """
            ),
            0,
        )
    )

    # --------------------------------------------------------
    # Ordem deve ser única dentro do acidente
    # --------------------------------------------------------

    results.append(
        check(
            "ordem repetida dentro do mesmo acidente",
            value(
                con,
                f"""
                SELECT COUNT(*)

                FROM (

                    SELECT
                        _source_year,
                        accident_id,
                        accident_type_order,
                        COUNT(*) AS n

                    FROM read_parquet(
                        '{TYPE_GLOB}'
                    )

                    GROUP BY
                        _source_year,
                        accident_id,
                        accident_type_order

                    HAVING COUNT(*) > 1
                )
                """
            ),
            0,
        )
    )

    # --------------------------------------------------------
    # FK
    # --------------------------------------------------------

    results.append(
        check(
            "tipos apontando para acidente inexistente",
            value(
                con,
                f"""
                SELECT COUNT(*)

                FROM read_parquet(
                    '{TYPE_GLOB}'
                ) t

                LEFT JOIN read_parquet(
                    '{ACCIDENT_GLOB}'
                ) a

                    ON
                        t._source_year
                        = a._source_year

                    AND
                        t.accident_id
                        = a.accident_id

                WHERE
                    a.accident_id IS NULL
                """
            ),
            0,
        )
    )

    # --------------------------------------------------------
    # Cobertura
    # --------------------------------------------------------

    results.append(
        check(
            "acidentes sem nenhum tipo",
            value(
                con,
                f"""
                SELECT COUNT(*)

                FROM read_parquet(
                    '{ACCIDENT_GLOB}'
                ) a

                LEFT JOIN (

                    SELECT DISTINCT
                        _source_year,
                        accident_id

                    FROM read_parquet(
                        '{TYPE_GLOB}'
                    )

                ) t

                    ON
                        a._source_year
                        = t._source_year

                    AND
                        a.accident_id
                        = t.accident_id

                WHERE
                    t.accident_id IS NULL
                """
            ),
            0,
        )
    )

    # ========================================================
    # MÉTRICAS
    # ========================================================

    print()
    print("=" * 120)
    print(
        "MÉTRICAS DA DECOMPOSIÇÃO"
    )
    print("=" * 120)

    cause_metrics = con.execute(
        f"""
        SELECT

            _source_year AS year,

            COUNT(*) AS cause_rows,

            COUNT(
                DISTINCT accident_id
            ) AS accidents,

            ROUND(
                COUNT(*)::DOUBLE
                /
                COUNT(
                    DISTINCT accident_id
                ),
                2
            ) AS avg_causes_per_accident,

            MAX(cause_count)
                AS max_causes_per_accident

        FROM (

            SELECT

                *,

                COUNT(*)
                OVER (
                    PARTITION BY
                        _source_year,
                        accident_id
                ) AS cause_count

            FROM read_parquet(
                '{CAUSE_GLOB}'
            )
        )

        GROUP BY
            _source_year

        ORDER BY
            _source_year
        """
    ).fetchdf()

    print()
    print("ACCIDENT CAUSE")
    print()

    print(
        cause_metrics.to_string(
            index=False
        )
    )

    type_metrics = con.execute(
        f"""
        SELECT

            _source_year AS year,

            COUNT(*) AS type_rows,

            COUNT(
                DISTINCT accident_id
            ) AS accidents,

            ROUND(
                COUNT(*)::DOUBLE
                /
                COUNT(
                    DISTINCT accident_id
                ),
                2
            ) AS avg_types_per_accident,

            MAX(type_count)
                AS max_types_per_accident

        FROM (

            SELECT

                *,

                COUNT(*)
                OVER (
                    PARTITION BY
                        _source_year,
                        accident_id
                ) AS type_count

            FROM read_parquet(
                '{TYPE_GLOB}'
            )
        )

        GROUP BY
            _source_year

        ORDER BY
            _source_year
        """
    ).fetchdf()

    print()
    print("ACCIDENT TYPE")
    print()

    print(
        type_metrics.to_string(
            index=False
        )
    )

    # ========================================================
    # RESULTADO
    # ========================================================

    print()
    print("=" * 120)

    passed = sum(results)
    total = len(results)

    print(
        f"Resultado: "
        f"{passed}/{total} "
        f"checks passaram."
    )

    if all(results):

        print()
        print(
            "STATUS: ALL_CAUSES SILVER APROVADA"
        )

    else:

        print()
        print(
            "STATUS: INVESTIGAÇÃO NECESSÁRIA"
        )

        raise SystemExit(1)


if __name__ == "__main__":
    main()
