with types as (

    select

        t.*,

        a.accident_key

    from {{ source(
        'silver_prf',
        'accident_type'
    ) }} t

    inner join {{ ref(
        'fct_accident'
    ) }} a

        on
            t._source_year
            = a.source_year

        and
            t.accident_id
            = a.accident_id

)

select

    type_record_id
        as accident_type_key,

    accident_key,

    accident_id,

    accident_type_order,

    accident_type,

    _source_year
        as source_year

from types