{{ config(materialized='table') }}

select
    fa.source_year as year,
    fa.highway,

    count(*) as accidents,
    sum(case when fa.is_fatal_accident then 1 else 0 end) as fatal_accidents,
    sum(fa.deaths) as deaths,
    sum(fa.serious_injuries) as serious_injuries,
    sum(fa.injured) as injured,
    sum(fa.vehicles) as vehicles,

    avg(fa.severity_score) as avg_severity_score,

    min(fa.km) as min_km_observed,
    max(fa.km) as max_km_observed,

    avg(fa.latitude) as center_latitude,
    avg(fa.longitude) as center_longitude

from {{ ref('fct_accident') }} fa

where fa.highway is not null

group by
    fa.source_year,
    fa.highway
