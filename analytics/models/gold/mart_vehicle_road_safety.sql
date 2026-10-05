{{ config(materialized='table') }}

select
    source_year as year,
    vehicle_type,

    count(*) as vehicles_involved,
    count(distinct accident_key) as accidents,

    sum(occupants) as occupants,
    sum(deaths) as deaths,
    sum(serious_injuries) as serious_injuries,
    sum(minor_injuries) as minor_injuries,
    sum(uninjured) as uninjured,

    sum(case when has_death then 1 else 0 end) as vehicles_with_death,
    sum(case when has_serious_injury then 1 else 0 end) as vehicles_with_serious_injury,

    avg(vehicle_age) as avg_vehicle_age,

    100.0 * sum(deaths)
        / nullif(count(*), 0) as deaths_per_100_vehicles,

    100.0 * sum(serious_injuries)
        / nullif(count(*), 0) as serious_injuries_per_100_vehicles

from {{ ref('fct_vehicle_involvement') }}

group by
    source_year,
    vehicle_type
