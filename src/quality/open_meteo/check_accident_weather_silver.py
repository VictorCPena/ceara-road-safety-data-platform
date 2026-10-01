import duckdb


WEATHER_GLOB = (
    "data/silver/open_meteo/"
    "accident_weather/"
    "year=*/part-000.parquet"
)

OCCURRENCE_GLOB = (
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
        "OPEN-METEO ACCIDENT WEATHER"
    )
    print("=" * 115)
    print()

    expected = value(
        con,
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{OCCURRENCE_GLOB}'
        )
        WHERE
            state = 'CE'
            AND latitude IS NOT NULL
            AND longitude IS NOT NULL
            AND event_timestamp IS NOT NULL
        """
    )

    actual = value(
        con,
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{WEATHER_GLOB}'
        )
        """
    )

    results = []

    results.append(
        check(
            "cobertura de acidentes",
            actual,
            expected,
        )
    )

    results.append(
        check(
            "accident_key NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{WEATHER_GLOB}'
                )
                WHERE accident_key IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "accident_key duplicada",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT
                        accident_key
                    FROM read_parquet(
                        '{WEATHER_GLOB}'
                    )
                    GROUP BY accident_key
                    HAVING COUNT(*) > 1
                )
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "temperature_2m NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{WEATHER_GLOB}'
                )
                WHERE temperature_2m IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "relative_humidity_2m fora de 0..100",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{WEATHER_GLOB}'
                )
                WHERE
                    relative_humidity_2m IS NULL
                    OR relative_humidity_2m < 0
                    OR relative_humidity_2m > 100
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "precipitation negativa",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{WEATHER_GLOB}'
                )
                WHERE
                    precipitation IS NULL
                    OR precipitation < 0
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "weather_code NULL",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{WEATHER_GLOB}'
                )
                WHERE weather_code IS NULL
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "wind_speed_10m negativa",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{WEATHER_GLOB}'
                )
                WHERE
                    wind_speed_10m IS NULL
                    OR wind_speed_10m < 0
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "match horário > 30 minutos",
            value(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet(
                    '{WEATHER_GLOB}'
                )
                WHERE
                    ABS(
                        weather_time_offset_minutes
                    ) > 30
                """
            ),
            0,
        )
    )

    results.append(
        check(
            "weather sem acidente PRF",
            value(
                con,
                f"""
                SELECT COUNT(*)

                FROM read_parquet(
                    '{WEATHER_GLOB}'
                ) w

                LEFT JOIN read_parquet(
                    '{OCCURRENCE_GLOB}'
                ) a

                    ON
                        w.source_year
                        = a._source_year

                    AND
                        w.accident_id
                        = a.accident_id

                WHERE
                    a.accident_id IS NULL
                """
            ),
            0,
        )
    )

    print()
    print("=" * 115)

    summary = con.execute(
        f"""
        SELECT

            source_year AS year,

            COUNT(*) AS records,

            ROUND(
                AVG(
                    temperature_2m
                ),
                2
            ) AS avg_temperature,

            ROUND(
                AVG(
                    relative_humidity_2m
                ),
                2
            ) AS avg_humidity,

            ROUND(
                SUM(
                    CASE
                        WHEN precipitation > 0
                        THEN 1
                        ELSE 0
                    END
                )
                * 100.0
                /
                COUNT(*),
                2
            ) AS pct_accidents_with_precipitation

        FROM read_parquet(
            '{WEATHER_GLOB}'
        )

        GROUP BY source_year

        ORDER BY source_year
        """
    ).fetchdf()

    print(
        summary.to_string(
            index=False
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
            "STATUS: OPEN-METEO "
            "ACCIDENT WEATHER SILVER APROVADA"
        )

    else:

        print(
            "STATUS: INVESTIGAÇÃO NECESSÁRIA"
        )

        raise SystemExit(1)


if __name__ == "__main__":
    main()
