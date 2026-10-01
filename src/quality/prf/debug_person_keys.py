import duckdb


PERSON_GLOB = (
    "data/silver/prf/person/"
    "year=*/part-000.parquet"
)


def main():
    con = duckdb.connect()

    print("\n=== CONTAGENS GERAIS ===")

    result = con.execute(
        f"""
        SELECT
            COUNT(*) AS rows,
            COUNT(person_id) AS person_id_not_null,
            COUNT(DISTINCT person_id) AS distinct_person_id
        FROM read_parquet('{PERSON_GLOB}')
        """
    ).fetchone()

    print(f"linhas: {result[0]:,}")
    print(f"person_id não nulos: {result[1]:,}")
    print(f"person_id distintos: {result[2]:,}")
    print(
        f"excesso de linhas sobre person_id distintos: "
        f"{result[0] - result[2]:,}"
    )

    print("\n=== PERSON_ID DUPLICADO ===")

    rows = con.execute(
        f"""
        SELECT
            person_id,
            COUNT(*) AS n
        FROM read_parquet('{PERSON_GLOB}')
        GROUP BY person_id
        HAVING COUNT(*) > 1
        ORDER BY n DESC
        """
    ).fetchdf()

    print(rows.to_string(index=False))

    print("\n=== REGISTROS DESSE PERSON_ID ===")

    duplicated_rows = con.execute(
        f"""
        SELECT
            person_id,
            accident_id,
            vehicle_id,
            event_date,
            state,
            municipality,
            person_type,
            age,
            sex,
            _source_year,
            _source_file
        FROM read_parquet('{PERSON_GLOB}')
        WHERE person_id IN (
            SELECT person_id
            FROM read_parquet('{PERSON_GLOB}')
            GROUP BY person_id
            HAVING COUNT(*) > 1
        )
        ORDER BY person_id, accident_id, vehicle_id
        """
    ).fetchdf()

    print(
        duplicated_rows.to_string(index=False)
    )

    print("\n=== DUPLICIDADE POR CHAVE ===")

    queries = {
        "person_id": """
            SELECT person_id
        """,

        "person_id + accident_id": """
            SELECT person_id, accident_id
        """,

        "person_id + vehicle_id": """
            SELECT person_id, vehicle_id
        """,

        "person_id + accident_id + vehicle_id": """
            SELECT person_id, accident_id, vehicle_id
        """,
    }

    for name, select_columns in queries.items():

        columns = (
            select_columns
            .replace("SELECT", "")
            .strip()
        )

        n = con.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT
                    {columns},
                    COUNT(*) AS n
                FROM read_parquet('{PERSON_GLOB}')
                GROUP BY {columns}
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

        print(
            f"{name:<45} "
            f"grupos duplicados = {n:,}"
        )


if __name__ == "__main__":
    main()
