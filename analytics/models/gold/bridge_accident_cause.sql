with causes as (

    select

        c.*,

        a.accident_key

    from {{ source(
        'silver_prf',
        'accident_cause'
    ) }} c

    inner join {{ ref(
        'fct_accident'
    ) }} a

        on
            c._source_year
            = a.source_year

        and
            c.accident_id
            = a.accident_id

)

select

    cause_record_id
        as accident_cause_key,

    accident_key,

    accident_id,

    accident_cause,

    is_primary_cause,

    _source_year
        as source_year

from causes