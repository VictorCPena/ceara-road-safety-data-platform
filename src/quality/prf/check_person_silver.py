import duckdb


PERSON_GLOB = (
    "data/silver/prf/person/"
    "year=*/part-000.parquet"
)

ACCIDENT_GLOB = (
    "data/silver/prf/occurrence/"
    "year=*/part-000.parquet"
)


def value(con, query):

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
        f"{name:<48} | "
        f"actual={actual} "
        f"expected={expected}"
    )

    return success


def main():

    con = duckdb.connect()

    print()
    print("=" * 105)
    print(
        "SILVER DATA QUALITY - PERSON"
    )
    print("=" * 105)

    total = value(
        con,
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{PERSON_GLOB}'
        )
        """
    )

    print()
    print(
        f"Total registros: {total:,}"
    )
    print()

    results = []

    # ========================================================
    # RECORD ID
    # ========================================================

    results.append(
        check(
            "record_id NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{PERSON_GLOB}'
                )
                WHERE record_id IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "record_id duplicado",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT record_id
                    FROM read_parquet(
                        '{PERSON_GLOB}'
                    )
                    GROUP BY record_id
                    HAVING COUNT(*) > 1
                )
                """
            ),
            0,
        )
    )

    # ========================================================
    # ACCIDENT ID
    # ========================================================

    results.append(
        check(
            "accident_id NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{PERSON_GLOB}'
                )
                WHERE accident_id IS NULL
                """
            ),
            0,
        )
    )

    # ========================================================
    # IDENTIDADE
    # ========================================================

    results.append(
        check(
            "person_id e vehicle_id ambos NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{PERSON_GLOB}'
                )
                WHERE
                    person_id IS NULL
                    AND vehicle_id IS NULL
                """
            ),
            0,
        )
    )

    # ========================================================
    # PERSON ID REAL
    # ========================================================

    results.append(
        check(
            "person_id real duplicado",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT person_id
                    FROM read_parquet(
                        '{PERSON_GLOB}'
                    )
                    WHERE person_id IS NOT NULL
                    GROUP BY person_id
                    HAVING COUNT(*) > 1
                )
                """
            ),
            0,
        )
    )

    # ========================================================
    # REFERENTIAL INTEGRITY
    # ========================================================

    results.append(
        check(
            "acidentes órfãos",
            value(
                con,
                f"""
                SELECT COUNT(*)

                FROM read_parquet(
                    '{PERSON_GLOB}'
                ) p

                LEFT JOIN read_parquet(
                    '{ACCIDENT_GLOB}'
                ) a

                ON
                    p.accident_id
                    = a.accident_id

                WHERE
                    a.accident_id IS NULL
                """
            ),
            0,
        )
    )

    # ========================================================
    # IDADE SILVER
    # ========================================================

    results.append(
        check(
            "idade Silver fora de 0..120",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{PERSON_GLOB}'
                )
                WHERE
                    age IS NOT NULL
                    AND
                    (
                        age < 0
                        OR age > 120
                    )
                """
            ),
            0,
        )
    )

    # ========================================================
    # AGE QUALITY FLAG
    # ========================================================

    results.append(
        check(
            "age_quality_flag desconhecida",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{PERSON_GLOB}'
                )
                WHERE
                    age_quality_flag NOT IN (
                        'valid',
                        'missing',
                        'missing_source',
                        'invalid_format',
                        'invalid_out_of_range'
                    )
                """
            ),
            0,
        )
    )

    # ========================================================
    # COORDENADAS
    # ========================================================

    results.append(
        check(
            "coordenadas inválidas",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{PERSON_GLOB}'
                )
                WHERE
                    (
                        latitude IS NOT NULL
                        AND
                        (
                            latitude < -90
                            OR latitude > 90
                        )
                    )
                    OR
                    (
                        longitude IS NOT NULL
                        AND
                        (
                            longitude < -180
                            OR longitude > 180
                        )
                    )
                """
            ),
            0,
        )
    )

    # ========================================================
    # MÉTRICAS DE QUALIDADE
    # ========================================================

    print()
    print("=" * 105)
    print("MÉTRICAS DE QUALIDADE")
    print("=" * 105)

    metrics = con.execute(
        f"""
        SELECT

            _source_year AS year,

            COUNT(*) AS records,

            COUNT(person_id)
                AS person_id_known,

            SUM(
                CASE
                    WHEN person_id IS NULL
                    THEN 1
                    ELSE 0
                END
            ) AS person_id_missing,

            SUM(
                CASE
                    WHEN age_quality_flag = 'valid'
                    THEN 1
                    ELSE 0
                END
            ) AS valid_age,

            SUM(
                CASE
                    WHEN age_quality_flag
                        = 'invalid_out_of_range'
                    THEN 1
                    ELSE 0
                END
            ) AS invalid_age,

            SUM(
                CASE
                    WHEN vehicle_manufacture_year
                        IS NULL
                    THEN 1
                    ELSE 0
                END
            ) AS vehicle_year_missing

        FROM read_parquet(
            '{PERSON_GLOB}'
        )

        GROUP BY _source_year

        ORDER BY _source_year
        """
    ).fetchdf()

    print(
        metrics.to_string(
            index=False
        )
    )

    print()
    print("=" * 105)

    passed = sum(results)

    print(
        f"Resultado: "
        f"{passed}/{len(results)} checks passaram."
    )

    if all(results):

        print(
            "STATUS: PERSON SILVER APROVADA"
        )

    else:

        print(
            "STATUS: INVESTIGAÇÃO NECESSÁRIA"
        )

        raise SystemExit(1)


if __name__ == "__main__":
    main()