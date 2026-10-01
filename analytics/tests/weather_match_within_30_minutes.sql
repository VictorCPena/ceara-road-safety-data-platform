select *

from {{ ref(
    'fct_accident_weather'
) }}

where abs(
    weather_time_offset_minutes
) > 30
