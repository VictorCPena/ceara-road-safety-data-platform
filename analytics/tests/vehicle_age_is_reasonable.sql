select *
from {{ ref('fct_vehicle_involvement') }}
where
    vehicle_age is not null
    and (vehicle_age < 0 or vehicle_age > 100)
