{{ config(materialized='table') }}

with base as (

    select
        fa.accident_key,
        fa.accident_id,
        fa.source_year as year,

        dm.municipality_key,
        dm.ibge_code,
        dm.municipality,

        fa.event_timestamp,
        fa.highway,
        fa.km,

        fa.latitude,
        fa.longitude,

        fa.accident_classification,
        fa.day_phase,
        fa.weather_condition,

        fa.deaths,
        fa.injured,
        fa.serious_injuries,
        fa.severity_score,

        case
            when fa.latitude is null or fa.longitude is null
                then 'missing'
            when fa.latitude not between -90 and 90
              or fa.longitude not between -180 and 180
                then 'invalid_global'
            when fa.latitude not between -9.0 and -2.0
              or fa.longitude not between -42.5 and -36.5
                then 'outside_ceara_bounds'
            else 'valid'
        end as coordinate_quality

    from {{ ref('fct_accident') }} fa

    left join {{ ref('dim_municipality') }} dm
      on dm.municipality_key = fa.municipality_key
)

select
    *,

    case
        when coordinate_quality = 'valid' then latitude
        else null
    end as map_latitude,

    case
        when coordinate_quality = 'valid' then longitude
        else null
    end as map_longitude

from base
