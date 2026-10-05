select *
from {{ ref('mart_highway_segment_safety') }}
where
    segment_start_km < 0
    or segment_end_km <= segment_start_km
    or (segment_end_km - segment_start_km) <> 10
