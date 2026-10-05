select *
from {{ ref('mart_accident_time_patterns') }}
where
    month not between 1 and 12
    or hour not between 0 and 23
    or day_of_week_number not between 0 and 6
