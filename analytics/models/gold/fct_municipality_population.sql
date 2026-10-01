with population as (

    select *

    from {{ source(
        'silver_ibge',
        'municipality_population'
    ) }}

)

select

    md5(
        cast(
            ibge_code
            as varchar
        )
        || '|'
        ||
        cast(
            year
            as varchar
        )
    ) as population_key,

    cast(
        ibge_code
        as varchar
    ) as municipality_key,

    cast(
        ibge_code
        as varchar
    ) as ibge_code,

    year,

    population,

    _source_table,

    _source_variable,

    _source_snapshot,

    _source_file

from population
