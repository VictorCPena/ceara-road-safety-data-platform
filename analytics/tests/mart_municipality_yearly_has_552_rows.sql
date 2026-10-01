select

    count(*) as records

from {{ ref(
    'mart_municipality_road_safety_yearly'
) }}

having count(*) <> 552
