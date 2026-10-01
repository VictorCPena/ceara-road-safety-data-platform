import duckdb


REJECTED_GLOB = (
    "data/rejected/prf/person/"
    "year=*/rejected.parquet"
)


def main():

    con = duckdb.connect()

    print()
    print("=" * 90)
    print("INVESTIGAÇÃO DE IDADES INVÁLIDAS")
    print("=" * 90)

    print()
    print("VALORES DE IDADE")
    print("-" * 90)

    ages = con.execute(
        f"""
        SELECT
            age,
            COUNT(*) AS records
        FROM read_parquet(
            '{REJECTED_GLOB}'
        )
        GROUP BY age
        ORDER BY records DESC, age
        """
    ).fetchdf()

    print(
        ages.to_string(
            index=False
        )
    )

    print()
    print("POR ANO")
    print("-" * 90)

    by_year = con.execute(
        f"""
        SELECT
            _source_year AS year,
            age,
            COUNT(*) AS records
        FROM read_parquet(
            '{REJECTED_GLOB}'
        )
        GROUP BY
            _source_year,
            age
        ORDER BY
            _source_year,
            records DESC,
            age
        """
    ).fetchdf()

    print(
        by_year.to_string(
            index=False
        )
    )

    print()
    print("MENOR E MAIOR IDADE REJEITADA")
    print("-" * 90)

    stats = con.execute(
        f"""
        SELECT
            MIN(age) AS min_age,
            MAX(age) AS max_age,
            COUNT(*) AS records
        FROM read_parquet(
            '{REJECTED_GLOB}'
        )
        """
    ).fetchdf()

    print(
        stats.to_string(
            index=False
        )
    )

    print()
    print("EXEMPLOS")
    print("-" * 90)

    examples = con.execute(
        f"""
        SELECT
            accident_id,
            person_id,
            age,
            sex,
            person_type,
            physical_condition,
            state,
            municipality,
            event_date,
            _source_year
        FROM read_parquet(
            '{REJECTED_GLOB}'
        )
        ORDER BY age DESC
        LIMIT 50
        """
    ).fetchdf()

    print(
        examples.to_string(
            index=False
        )
    )


if __name__ == "__main__":
    main()
