select

    accident_key,

    sum(
        case
            when is_primary_cause
            then 1
            else 0
        end
    ) as primary_cause_count

from {{ ref('bridge_accident_cause') }}

group by accident_key

having primary_cause_count <> 1
