select uf, cast(event_time as date) date_key, count(*) accidents, sum(mortos) deaths, sum(pessoas) people from {{ ref('fct_accident') }} group by 1,2
