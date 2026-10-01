select

    cast(
        ibge_code
        as varchar
    ) as municipality_key,

    cast(
        ibge_code
        as varchar
    ) as ibge_code,

    municipality,

    municipality_match_key,

    state_code,

    state_abbr as state,

    state_name,

    region_code,

    region_abbr,

    region_name,

    immediate_region_code,

    immediate_region,

    intermediate_region_code,

    intermediate_region

from {{ source(
    'silver_ibge',
    'municipality'
) }}