with weather as (

    select *

    from {{ source(
        'silver_open_meteo',
        'accident_weather'
    ) }}

),

joined as (

    select

        a.accident_key,

        a.accident_id,

        a.source_year,

        a.municipality_key,

        a.date_key,

        a.event_timestamp,

        a.deaths,

        a.serious_injuries,

        a.minor_injuries,

        a.injured,

        a.is_fatal_accident,

        a.severity_score,

        a.weather_condition
            as prf_weather_condition,

        w.weather_timestamp,

        w.weather_time_offset_minutes,

        w.temperature_2m,

        w.relative_humidity_2m,

        w.precipitation,

        w.rain,

        w.weather_code,

        w.cloud_cover,

        w.visibility,

        w.wind_speed_10m,

        w.wind_gusts_10m,

        w.requested_latitude,

        w.requested_longitude,

        w.weather_grid_latitude,

        w.weather_grid_longitude,

        w.weather_grid_elevation,

        w._source_snapshot,

        w._source_file

    from {{ ref(
        'fct_accident'
    ) }} a

    inner join weather w

        on
            a.source_year
            = w.source_year

        and
            a.accident_id
            = w.accident_id

),

classified as (

    select

        *,

        md5(
            accident_key
            || '|OPEN_METEO'
        ) as accident_weather_key,

        case

            when weather_code = 0
                then 'clear'

            when weather_code = 1
                then 'mainly_clear'

            when weather_code = 2
                then 'partly_cloudy'

            when weather_code = 3
                then 'overcast'

            when weather_code in (
                45,
                48
            )
                then 'fog'

            when weather_code in (
                51,
                53,
                55,
                56,
                57
            )
                then 'drizzle'

            when weather_code in (
                61,
                63,
                65,
                66,
                67
            )
                then 'rain'

            when weather_code in (
                71,
                73,
                75,
                77
            )
                then 'snow'

            when weather_code in (
                80,
                81,
                82
            )
                then 'rain_showers'

            when weather_code in (
                85,
                86
            )
                then 'snow_showers'

            when weather_code in (
                95,
                96,
                97,
                99
            )
                then 'thunderstorm'

            else 'unknown'

        end as weather_group,

        case

            when precipitation > 0
                then true

            else false

        end as is_precipitating,

        visibility / 1000.0
            as visibility_km

    from joined

)

select *

from classified
