select 'mart_vehicle_road_safety' as model_name
from {{ ref('mart_vehicle_road_safety') }}
where
    vehicles_involved < 0
    or accidents < 0
    or deaths < 0
    or serious_injuries < 0

union all

select 'mart_accident_time_patterns' as model_name
from {{ ref('mart_accident_time_patterns') }}
where
    accidents < 0
    or fatal_accidents < 0
    or deaths < 0
    or serious_injuries < 0

union all

select 'mart_highway_safety' as model_name
from {{ ref('mart_highway_safety') }}
where
    accidents < 0
    or fatal_accidents < 0
    or deaths < 0
    or serious_injuries < 0

union all

select 'mart_highway_segment_safety' as model_name
from {{ ref('mart_highway_segment_safety') }}
where
    accidents < 0
    or fatal_accidents < 0
    or deaths < 0
    or serious_injuries < 0

union all

select 'mart_accident_cause_type' as model_name
from {{ ref('mart_accident_cause_type') }}
where
    accidents < 0
    or fatal_accidents < 0
    or deaths < 0
    or serious_injuries < 0
