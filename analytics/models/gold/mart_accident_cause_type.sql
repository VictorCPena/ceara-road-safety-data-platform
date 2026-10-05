{{ config(materialized='table') }}

with accident_pairs as (

    select distinct
        fa.source_year as year,
        fa.accident_key,
        fa.municipality_key,
        dm.municipality,

        bac.accident_cause,
        bac.is_primary_cause,
        bat.accident_type,

        fa.is_fatal_accident,
        fa.deaths,
        fa.serious_injuries,
        fa.injured,
        fa.severity_score

    from {{ ref('fct_accident') }} fa

    join {{ ref('bridge_accident_cause') }} bac
      on bac.accident_key = fa.accident_key

    join {{ ref('bridge_accident_type') }} bat
      on bat.accident_key = fa.accident_key

    left join {{ ref('dim_municipality') }} dm
      on dm.municipality_key = fa.municipality_key

    where
        bac.accident_cause is not null
        and bat.accident_type is not null
)

select
    year,
    municipality_key,
    municipality,
    accident_cause,
    accident_type,

    count(distinct accident_key) as accidents,

    count(
        distinct case
            when is_primary_cause then accident_key
        end
    ) as accidents_where_primary_cause,

    count(
        distinct case
            when is_fatal_accident then accident_key
        end
    ) as fatal_accidents,

    sum(deaths) as deaths,
    sum(serious_injuries) as serious_injuries,
    sum(injured) as injured,
    avg(severity_score) as avg_severity_score

from accident_pairs

group by
    year,
    municipality_key,
    municipality,
    accident_cause,
    accident_type
