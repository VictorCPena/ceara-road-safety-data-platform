import duckdb


PERSON_GLOB = (
    "data/silver/prf/person/"
    "year=*/part-000.parquet"
)


def main():

    con = duckdb.connect()

    print()
    print("=" * 90)
    print("PERSON_ID DUPLICADOS")
    print("=" * 90)

    duplicates = con.execute(
        f"""
        SELECT
            person_id,
            COUNT(*) AS occurrences
        FROM read_parquet(
            '{PERSON_GLOB}'
        )
        GROUP BY person_id
        HAVING COUNT(*) > 1
        ORDER BY occurrences DESC
        """
    ).fetchdf()

    print(duplicates.to_string(index=False))

    print()
    print("=" * 90)
    print("REGISTROS COMPLETOS DOS PERSON_ID DUPLICADOS")
    print("=" * 90)

    rows = con.execute(
        f"""
        SELECT *
        FROM read_parquet(
            '{PERSON_GLOB}'
        )
        WHERE person_id IN (
            SELECT person_id
            FROM read_parquet(
                '{PERSON_GLOB}'
            )
            GROUP BY person_id
            HAVING COUNT(*) > 1
        )
        ORDER BY
            person_id,
            accident_id,
            vehicle_id
        """
    ).fetchdf()

    print(
        rows.to_string(index=False)
    )

    print()
    print("=" * 90)
    print("TESTE DE CHAVES CANDIDATAS")
    print("=" * 90)

    tests = {
        "person_id": """
            person_id
        """,

        "person_id + accident_id": """
            person_id,
            accident_id
        """,

        "person_id + vehicle_id": """
            person_id,
            vehicle_id
        """,

        "person_id + accident_id + vehicle_id": """
            person_id,
            accident_id,
            vehicle_id
        """,
    }

    for name, columns in tests.items():

        duplicated_groups = con.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT
                    {columns},
                    COUNT(*) AS n
                FROM read_parquet(
                    '{PERSON_GLOB}'
                )
                GROUP BY
                    {columns}
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

        print(
            f"{name:<45} "
            f"duplicated_groups={duplicated_groups}"
        )


if __name__ == "__main__":
    main()
