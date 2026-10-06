{{ config(materialized='table') }}

select
    year,
    weather_group,

    case lower(weather_group)
        when 'clear' then 'Céu limpo'
        when 'mainly_clear' then 'Predominantemente limpo'
        when 'partly_cloudy' then 'Parcialmente nublado'
        when 'overcast' then 'Nublado'
        when 'fog' then 'Neblina'
        when 'drizzle' then 'Garoa'
        when 'rain' then 'Chuva'
        when 'snow' then 'Neve'
        when 'rain_showers' then 'Pancadas de chuva'
        when 'snow_showers' then 'Pancadas de neve'
        when 'thunderstorm' then 'Tempestade'
        when 'unknown' then 'Não informado'
        else coalesce(weather_group, 'Não informado')
    end as weather_group_label,

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
