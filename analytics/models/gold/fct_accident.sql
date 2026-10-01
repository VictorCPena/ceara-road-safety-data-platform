with accidents as (

    select *

    from {{ source(
        'silver_prf',
        'occurrence'
    ) }}

    where state = 'CE'

),

enriched as (

    select

        a.*,

        m.municipality_key,

        m.ibge_code

    from accidents a

    left join {{ ref(
        'dim_municipality'
    ) }} m

        on
            upper(
                strip_accents(
                    trim(
                        a.municipality
                    )
                )
            )
            =
            m.municipality_match_key

)

select

    md5(
        cast(
            _source_year
            as varchar
        )
        || '|'
        ||
        cast(
            accident_id
            as varchar
        )
    ) as accident_key,

    accident_id,

    _source_year
        as source_year,

    cast(
        strftime(
            event_date,
            '%Y%m%d'
        )
        as integer
    ) as date_key,

    municipality_key,

    ibge_code,

    municipality
        as municipality_source_name,

    event_date,

    event_timestamp,

    highway,

    km,

    latitude,

    longitude,

    accident_classification,

    day_phase,

    road_direction,

    weather_condition,

    road_type,

    road_layout,

    land_use,

    people_count,

    vehicles,

    deaths,

    minor_injuries,

    serious_injuries,

    injured,

    uninjured,

    unknown_status,

    case

        when deaths > 0
            then true

        else false

    end as is_fatal_accident,

    case

        when serious_injuries > 0
            then true

        else false

    end as has_serious_injury,

    case

        when injured > 0
            then true

        else false

    end as has_injuries,

    (
        coalesce(
            deaths,
            0
        ) * 5

        +

        coalesce(
            serious_injuries,
            0
        ) * 3

        +

        coalesce(
            minor_injuries,
            0
        )

    ) as severity_score,

    regional,

    police_station,

    uop,

    _source_snapshot,

    _source_file

from enriched