{{ config(materialized='table') }}

with base as (

    select
        fa.source_year as year,
        fa.highway,
        fa.municipality_key,
        dm.municipality,

        floor(fa.km / 10.0) * 10.0 as segment_start_km,
        floor(fa.km / 10.0) * 10.0 + 10.0 as segment_end_km,

        fa.accident_key,
        fa.is_fatal_accident,
        fa.deaths,
        fa.serious_injuries,
        fa.injured,
        fa.vehicles,
        fa.severity_score,
        fa.latitude,
        fa.longitude

    from {{ ref('fct_accident') }} fa

    left join {{ ref('dim_municipality') }} dm
      on dm.municipality_key = fa.municipality_key

    where
        fa.highway is not null
        and fa.km is not null
        and fa.km >= 0
)

select
    year,
    highway,
    municipality_key,
    municipality,
    segment_start_km,
    segment_end_km,

    concat(
        'BR-',
        cast(highway as varchar),
        ' · km ',
        cast(segment_start_km as integer),
        '–',
        cast(segment_end_km as integer)
    ) as highway_segment,

    count(*) as accidents,
    sum(case when is_fatal_accident then 1 else 0 end) as fatal_accidents,
    sum(deaths) as deaths,
    sum(serious_injuries) as serious_injuries,
    sum(injured) as injured,
    sum(vehicles) as vehicles,

    avg(severity_score) as avg_severity_score,

    avg(latitude) as center_latitude,
    avg(longitude) as center_longitude

from base

group by
    year,
    highway,
    municipality_key,
    municipality,
    segment_start_km,
    segment_end_km
