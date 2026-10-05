select *
from {{ ref('mart_accident_map') }}
where
    coordinate_quality = 'valid'
    and (
        map_latitude is null
        or map_longitude is null
        or map_latitude not between -9.0 and -2.0
        or map_longitude not between -42.5 and -36.5
    )
