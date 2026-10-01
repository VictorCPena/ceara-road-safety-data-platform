select *

from {{ ref(
    'mart_municipality_road_safety_yearly'
) }}

where

    accidents_per_100k < 0

    or deaths_per_100k < 0

    or serious_injuries_per_100k < 0

    or injured_per_100k < 0
