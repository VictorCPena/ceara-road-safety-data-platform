select * from {{ ref('mart_daily_safety') }} where deaths < 0 or people < deaths or accidents < 1
