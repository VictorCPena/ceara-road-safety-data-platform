select distinct cast(event_time as date) as date_key, year(event_time) as calendar_year, month(event_time) as calendar_month from {{ ref('fct_accident') }}
