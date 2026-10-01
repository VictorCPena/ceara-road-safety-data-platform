import duckdb


SILVER_FILE = (
    "data/silver/ibge/"
    "municipality/part-000.parquet"
)

EXPECTED_MUNICIPALITIES = 184


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
        f"{name:<50} | "
        f"actual={actual} "
        f"expected={expected}"
    )

    return success


def main():

    con = duckdb.connect()

    print()
    print("=" * 105)
    print(
        "SILVER DATA QUALITY - IBGE MUNICIPALITY"
    )
    print("=" * 105)
    print()

    results = []

    total = value(
        con,
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{SILVER_FILE}'
        )
        """
    )

    results.append(
        check(
            "quantidade de municípios",
            total,
            EXPECTED_MUNICIPALITIES,
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
                    '{SILVER_FILE}'
                )
                WHERE ibge_code IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "ibge_code duplicado",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT
                        ibge_code
                    FROM read_parquet(
                        '{SILVER_FILE}'
                    )
                    GROUP BY
                        ibge_code
                    HAVING COUNT(*) > 1
                )
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "ibge_code fora de 7 dígitos",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{SILVER_FILE}'
                )
                WHERE
                    NOT regexp_matches(
                        ibge_code,
                        '^[0-9]{{7}}$'
                    )
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "municipality NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{SILVER_FILE}'
                )
                WHERE municipality IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "municipality_match_key duplicada",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT
                        municipality_match_key
                    FROM read_parquet(
                        '{SILVER_FILE}'
                    )
                    GROUP BY
                        municipality_match_key
                    HAVING COUNT(*) > 1
                )
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "UF diferente de CE",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{SILVER_FILE}'
                )
                WHERE state_abbr <> 'CE'
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "state_code diferente de 23",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{SILVER_FILE}'
                )
                WHERE state_code <> '23'
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "immediate_region NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{SILVER_FILE}'
                )
                WHERE immediate_region IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "intermediate_region NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{SILVER_FILE}'
                )
                WHERE intermediate_region IS NULL
                """
            ),
            0,
        )
    )

    print()
    print("=" * 105)

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
            "STATUS: IBGE MUNICIPALITY "
            "SILVER APROVADA"
        )

    else:

        print(
            "STATUS: INVESTIGAÇÃO NECESSÁRIA"
        )

        raise SystemExit(1)


if __name__ == "__main__":
    main()
