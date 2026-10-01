import duckdb


REJECTED_GLOB = (
    "data/rejected/prf/person/"
    "year=*/rejected.parquet"
)


def main():

    con = duckdb.connect()

    print()
    print("=" * 100)
    print("ANÁLISE DOS REGISTROS REJEITADOS - PERSON")
    print("=" * 100)

    # ========================================================
    # TOTAL
    # ========================================================

    total = con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{REJECTED_GLOB}'
        )
        """
    ).fetchone()[0]

    print()
    print(
        f"Total rejeitado: {total:,}"
    )

    # ========================================================
    # MOTIVOS
    # ========================================================

    print()
    print("=" * 100)
    print("MOTIVOS DE REJEIÇÃO")
    print("=" * 100)

    reasons = con.execute(
        f"""
        SELECT
            rejection_reason,
            COUNT(*) AS records
        FROM read_parquet(
            '{REJECTED_GLOB}'
        )
        GROUP BY rejection_reason
        ORDER BY records DESC
        """
    ).fetchdf()

    print(
        reasons.to_string(
            index=False
        )
    )

    # ========================================================
    # POR ANO
    # ========================================================

    print()
    print("=" * 100)
    print("REJEIÇÕES POR ANO")
    print("=" * 100)

    by_year = con.execute(
        f"""
        SELECT
            _source_year AS year,
            rejection_reason,
            COUNT(*) AS records
        FROM read_parquet(
            '{REJECTED_GLOB}'
        )
        GROUP BY
            _source_year,
            rejection_reason
        ORDER BY
            _source_year,
            records DESC
        """
    ).fetchdf()

    print(
        by_year.to_string(
            index=False
        )
    )

    # ========================================================
    # ANO DE FABRICAÇÃO
    # ========================================================

    print()
    print("=" * 100)
    print("VALORES DE ANO DE FABRICAÇÃO MAIS COMUNS")
    print("=" * 100)

    manufacture_years = con.execute(
        f"""
        SELECT
            vehicle_manufacture_year,
            COUNT(*) AS records
        FROM read_parquet(
            '{REJECTED_GLOB}'
        )
        GROUP BY vehicle_manufacture_year
        ORDER BY records DESC
        LIMIT 30
        """
    ).fetchdf()

    print(
        manufacture_years.to_string(
            index=False
        )
    )

    # ========================================================
    # EXEMPLOS
    # ========================================================

    print()
    print("=" * 100)
    print("EXEMPLOS DOS REJEITADOS")
    print("=" * 100)

    examples = con.execute(
        f"""
        SELECT
            record_id,
            accident_id,
            person_id,
            vehicle_id,
            event_date,
            vehicle_manufacture_year,
            state,
            municipality,
            rejection_reason,
            _source_year
        FROM read_parquet(
            '{REJECTED_GLOB}'
        )
        LIMIT 30
        """
    ).fetchdf()

    print(
        examples.to_string(
            index=False
        )
    )


if __name__ == "__main__":
    main()
