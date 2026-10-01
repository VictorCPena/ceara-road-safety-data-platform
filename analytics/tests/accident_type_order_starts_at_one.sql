select

    accident_key,

    min(accident_type_order)
        as first_order

from {{ ref('bridge_accident_type') }}

group by accident_key

having min(accident_type_order) <> 1
