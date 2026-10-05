select *
from {{ ref('fct_vehicle_involvement') }}
where
    occupants < 0
    or deaths < 0
    or serious_injuries < 0
    or minor_injuries < 0
    or uninjured < 0
