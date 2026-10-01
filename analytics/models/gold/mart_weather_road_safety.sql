select

    source_year as year,

    weather_group,

    is_precipitating,

    count(*) as accidents,

    sum(
        coalesce(
            deaths,
            0
        )
    ) as deaths,

    sum(
        coalesce(
            serious_injuries,
            0
        )
    ) as serious_injuries,

    sum(
        coalesce(
            injured,
            0
        )
    ) as injured,

    sum(
        case
            when is_fatal_accident
            then 1
            else 0
        end
    ) as fatal_accidents,

    round(
        avg(
            severity_score
        ),
        2
    ) as avg_severity_score,

    round(
        avg(
            temperature_2m
        ),
        2
    ) as avg_temperature,

    round(
        avg(
            relative_humidity_2m
        ),
        2
    ) as avg_humidity,

    round(
        avg(
            precipitation
        ),
        3
    ) as avg_precipitation_mm,

    round(
        avg(
            visibility_km
        ),
        2
    ) as avg_visibility_km,

    round(
        avg(
            wind_speed_10m
        ),
        2
    ) as avg_wind_speed_kmh,

    round(
        (
            sum(
                coalesce(
                    deaths,
                    0
                )
            )
            * 100.0
        )
        /
        nullif(
            count(*),
            0
        ),
        2
    ) as deaths_per_100_accidents,

    round(
        (
            sum(
                case
                    when is_fatal_accident
                    then 1
                    else 0
                end
            )
            * 100.0
        )
        /
        nullif(
            count(*),
            0
        ),
        2
    ) as fatal_accident_pct

from {{ ref(
    'fct_accident_weather'
) }}

group by

    source_year,
    weather_group,
    is_precipitating
