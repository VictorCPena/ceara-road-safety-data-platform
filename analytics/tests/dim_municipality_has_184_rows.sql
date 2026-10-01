select

    count(*) as municipality_count

from {{ ref(
    'dim_municipality'
) }}

having count(*) <> 184
