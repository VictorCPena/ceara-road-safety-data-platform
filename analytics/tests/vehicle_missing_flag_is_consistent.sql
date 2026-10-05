select *
from {{ ref('fct_vehicle_involvement') }}
where
    lower(vehicle_make) in (
        'não informado',
        'nao informado',
        'unknown',
        'not informed'
    )
    and is_vehicle_make_missing = false
