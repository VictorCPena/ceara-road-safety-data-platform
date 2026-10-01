with people as (

    select

        p.*,

        a.accident_key

    from {{ source(
        'silver_prf',
        'person'
    ) }} p

    inner join {{ ref(
        'fct_accident'
    ) }} a

        on
            p._source_year
            = a.source_year

        and
            p.accident_id
            = a.accident_id

)

select

    record_id
        as involvement_key,

    accident_key,

    accident_id,

    person_id,

    vehicle_id,

    person_type,

    physical_condition,

    age,

    age_raw,

    age_quality_flag,

    sex,

    vehicle_type,

    vehicle_brand,

    vehicle_manufacture_year,

    vehicle_year_quality_flag,

    uninjured,

    minor_injury,

    serious_injury,

    death,

    _source_year
        as source_year

from people