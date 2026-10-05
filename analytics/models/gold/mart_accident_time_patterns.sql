{{ config(materialized='table') }}

with base as (

    select
        fa.source_year as year,
        fa.municipality_key,
        dm.ibge_code,
        dm.municipality,

        extract(month from fa.event_timestamp)::integer as month,
        extract(hour from fa.event_timestamp)::integer as hour,

        strftime(fa.event_timestamp, '%w')::integer as day_of_week_number,

        case strftime(fa.event_timestamp, '%w')
            when '0' then 'Domingo'
            when '1' then 'Segunda'
            when '2' then 'Terça'
            when '3' then 'Quarta'
            when '4' then 'Quinta'
            when '5' then 'Sexta'
            when '6' then 'Sábado'
        end as day_of_week,

        case extract(month from fa.event_timestamp)::integer
            when 1 then 'Jan'
            when 2 then 'Fev'
            when 3 then 'Mar'
            when 4 then 'Abr'
            when 5 then 'Mai'
            when 6 then 'Jun'
            when 7 then 'Jul'
            when 8 then 'Ago'
            when 9 then 'Set'
            when 10 then 'Out'
            when 11 then 'Nov'
            when 12 then 'Dez'
        end as month_name,

        strftime(fa.event_timestamp, '%w') in ('0', '6') as is_weekend,

        fa.day_phase,

        fa.accident_key,
        fa.is_fatal_accident,
        fa.deaths,
        fa.serious_injuries,
        fa.injured,
        fa.severity_score

    from {{ ref('fct_accident') }} fa

    left join {{ ref('dim_municipality') }} dm
      on dm.municipality_key = fa.municipality_key

    where fa.event_timestamp is not null
)

select
    year,
    municipality_key,
    ibge_code,
    municipality,

    month,
    month_name,
    day_of_week_number,
    day_of_week,
    hour,
    is_weekend,
    day_phase,

    count(*) as accidents,
    sum(case when is_fatal_accident then 1 else 0 end) as fatal_accidents,
    sum(deaths) as deaths,
    sum(serious_injuries) as serious_injuries,
    sum(injured) as injured,
    avg(severity_score) as avg_severity_score

from base

group by
    year,
    municipality_key,
    ibge_code,
    municipality,
    month,
    month_name,
    day_of_week_number,
    day_of_week,
    hour,
    is_weekend,
    day_phase
