{{ config(materialized='table') }}

select
    source_year as year,
    vehicle_type,
    vehicle_make,
    vehicle_model,
    vehicle_make_model,

    count(*) as vehicles_involved,
    count(distinct accident_key) as accidents,

    sum(occupants) as occupants,
    sum(deaths) as deaths,
    sum(serious_injuries) as serious_injuries,
    sum(minor_injuries) as minor_injuries,
    sum(uninjured) as uninjured,

    avg(vehicle_age) as avg_vehicle_age,

    100.0 * sum(deaths)
        / nullif(count(*), 0) as deaths_per_100_vehicles,

    100.0 * sum(serious_injuries)
        / nullif(count(*), 0) as serious_injuries_per_100_vehicles

from {{ ref('fct_vehicle_involvement') }}

group by
    source_year,
    vehicle_type,
    vehicle_make,
    vehicle_model,
    vehicle_make_model
