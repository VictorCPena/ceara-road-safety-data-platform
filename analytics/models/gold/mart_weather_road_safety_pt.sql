{{ config(materialized='table') }}

select
    year,
    weather_group,
    {{ weather_group_pt('weather_group') }} as weather_group_label,
    is_precipitating,

    case
        when is_precipitating then 'Com precipitação'
        else 'Sem precipitação'
    end as precipitation_label,

    accidents,
    deaths,
    serious_injuries,
    injured,
    fatal_accidents,
    avg_severity_score,
    avg_temperature,
    avg_humidity,
    avg_precipitation_mm,
    avg_visibility_km,
    avg_wind_speed_kmh,
    deaths_per_100_accidents,
    fatal_accident_pct

from {{ ref('mart_weather_road_safety') }}
