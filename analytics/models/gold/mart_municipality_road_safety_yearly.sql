with accident_metrics as (

    select

        municipality_key,

        source_year as year,

        count(*) as accidents,

        sum(
            coalesce(
                people_count,
                0
            )
        ) as people_in_accidents,

        sum(
            coalesce(
                vehicles,
                0
            )
        ) as vehicles_in_accidents,

        sum(
            coalesce(
                deaths,
                0
            )
        ) as deaths,

        sum(
            coalesce(
                serious_injuries,
                0
            )
        ) as serious_injuries,

        sum(
            coalesce(
                minor_injuries,
                0
            )
        ) as minor_injuries,

        sum(
            coalesce(
                injured,
                0
            )
        ) as injured,

        sum(
            coalesce(
                severity_score,
                0
            )
        ) as total_severity_score,

        sum(
            case
                when is_fatal_accident
                then 1
                else 0
            end
        ) as fatal_accidents,

        max(
            event_date
        ) as observed_through_date

    from {{ ref(
        'fct_accident'
    ) }}

    group by

        municipality_key,
        source_year

),

year_coverage as (

    select

        source_year as year,

        min(
            event_date
        ) as first_event_date,

        max(
            event_date
        ) as observed_through_date

    from {{ ref(
        'fct_accident'
    ) }}

    group by
        source_year

),

population as (

    select

        p.population_key,

        p.municipality_key,

        p.year,

        p.population

    from {{ ref(
        'fct_municipality_population'
    ) }} p

),

final as (

    select

        md5(
            cast(
                p.municipality_key
                as varchar
            )
            || '|'
            ||
            cast(
                p.year
                as varchar
            )
        ) as municipality_year_key,

        p.municipality_key,

        m.ibge_code,

        m.municipality,

        m.state,

        m.immediate_region,

        m.intermediate_region,

        p.year,

        p.population,

        coalesce(
            a.accidents,
            0
        ) as accidents,

        coalesce(
            a.fatal_accidents,
            0
        ) as fatal_accidents,

        coalesce(
            a.deaths,
            0
        ) as deaths,

        coalesce(
            a.serious_injuries,
            0
        ) as serious_injuries,

        coalesce(
            a.minor_injuries,
            0
        ) as minor_injuries,

        coalesce(
            a.injured,
            0
        ) as injured,

        coalesce(
            a.people_in_accidents,
            0
        ) as people_in_accidents,

        coalesce(
            a.vehicles_in_accidents,
            0
        ) as vehicles_in_accidents,

        coalesce(
            a.total_severity_score,
            0
        ) as total_severity_score,

        y.first_event_date,

        y.observed_through_date,

        case

            when p.year < year(current_date)
                then 'closed_year'

            when p.year = year(current_date)
                then 'partial_ytd'

            else 'future_year'

        end as year_status,

        round(
            (
                coalesce(
                    a.accidents,
                    0
                )
                * 100000.0
            )
            /
            nullif(
                p.population,
                0
            ),
            2
        ) as accidents_per_100k,

        round(
            (
                coalesce(
                    a.fatal_accidents,
                    0
                )
                * 100000.0
            )
            /
            nullif(
                p.population,
                0
            ),
            2
        ) as fatal_accidents_per_100k,

        round(
            (
                coalesce(
                    a.deaths,
                    0
                )
                * 100000.0
            )
            /
            nullif(
                p.population,
                0
            ),
            2
        ) as deaths_per_100k,

        round(
            (
                coalesce(
                    a.serious_injuries,
                    0
                )
                * 100000.0
            )
            /
            nullif(
                p.population,
                0
            ),
            2
        ) as serious_injuries_per_100k,

        round(
            (
                coalesce(
                    a.injured,
                    0
                )
                * 100000.0
            )
            /
            nullif(
                p.population,
                0
            ),
            2
        ) as injured_per_100k

    from population p

    inner join {{ ref(
        'dim_municipality'
    ) }} m

        on
            p.municipality_key
            = m.municipality_key

    left join accident_metrics a

        on
            p.municipality_key
            = a.municipality_key

        and
            p.year
            = a.year

    left join year_coverage y

        on
            p.year
            = y.year

)

select *

from final
