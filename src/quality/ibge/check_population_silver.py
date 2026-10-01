import duckdb


POPULATION_GLOB = (
    "data/silver/ibge/"
    "municipality_population/"
    "year=*/part-000.parquet"
)

MUNICIPALITY_FILE = (
    "data/silver/ibge/"
    "municipality/part-000.parquet"
)

EXPECTED_YEARS = {
    2024,
    2025,
    2026,
}

EXPECTED_TOTALS = {
    2024: 9_233_656,
    2025: 9_268_836,
    2026: 9_302_211,
}


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
        f"{name:<55} | "
        f"actual={actual} "
        f"expected={expected}"
    )

    return success


def main():

    con = duckdb.connect()

    print()
    print("=" * 115)
    print(
        "SILVER DATA QUALITY - "
        "IBGE MUNICIPALITY POPULATION"
    )
    print("=" * 115)
    print()

    results = []

    results.append(
        check(
            "total de registros",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{POPULATION_GLOB}'
                )
                """
            ),
            184 * 3,
        )
    )

    results.append(
        check(
            "ibge_code NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{POPULATION_GLOB}'
                )
                WHERE ibge_code IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "year NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{POPULATION_GLOB}'
                )
                WHERE year IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "population NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{POPULATION_GLOB}'
                )
                WHERE population IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "population <= 0",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{POPULATION_GLOB}'
                )
                WHERE population <= 0
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "município + ano duplicado",
            value(
                con,
                f"""
                SELECT COUNT(*)

                FROM (

                    SELECT
                        ibge_code,
                        year,
                        COUNT(*) AS n

                    FROM read_parquet(
                        '{POPULATION_GLOB}'
                    )

                    GROUP BY
                        ibge_code,
                        year

                    HAVING COUNT(*) > 1
                )
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "códigos IBGE sem cadastro municipal",
            value(
                con,
                f"""
                SELECT COUNT(*)

                FROM (

                    SELECT DISTINCT
                        p.ibge_code

                    FROM read_parquet(
                        '{POPULATION_GLOB}'
                    ) p

                    LEFT JOIN read_parquet(
                        '{MUNICIPALITY_FILE}'
                    ) m

                        ON
                            p.ibge_code
                            = m.ibge_code

                    WHERE
                        m.ibge_code
                        IS NULL
                )
                """
            ),
            0,
        )
    )

    years = {
        row[0]
        for row in con.execute(
            f"""
            SELECT DISTINCT year
            FROM read_parquet(
                '{POPULATION_GLOB}'
            )
            ORDER BY year
            """
        ).fetchall()
    }

    results.append(
        check(
            "anos disponíveis",
            years,
            EXPECTED_YEARS,
        )
    )

    print()
    print("=" * 115)
    print(
        "REGISTROS E POPULAÇÃO POR ANO"
    )
    print("=" * 115)

    summary = con.execute(
        f"""
        SELECT

            year,

            COUNT(*) AS municipalities,

            SUM(population)
                AS population

        FROM read_parquet(
            '{POPULATION_GLOB}'
        )

        GROUP BY year

        ORDER BY year
        """
    ).fetchdf()

    print()
    print(
        summary.to_string(
            index=False
        )
    )

    for year, expected in (
        EXPECTED_TOTALS.items()
    ):

        actual = value(
            con,
            f"""
            SELECT SUM(population)

            FROM read_parquet(
                '{POPULATION_GLOB}'
            )

            WHERE year = {year}
            """
        )

        results.append(
            check(
                f"população total CE {year}",
                actual,
                expected,
            )
        )

    print()
    print("=" * 115)

    passed = sum(
        results
    )

    print(
        f"Resultado: "
        f"{passed}/{len(results)} "
        f"checks passaram."
    )

    if all(results):

        print(
            "STATUS: IBGE POPULATION "
            "SILVER APROVADA"
        )

    else:

        print(
            "STATUS: INVESTIGAÇÃO NECESSÁRIA"
        )

        raise SystemExit(1)


if __name__ == "__main__":
    main()
