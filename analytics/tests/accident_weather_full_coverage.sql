select

    (
        select count(*)
        from {{ ref('fct_accident') }}
    ) as accident_count,

    (
        select count(*)
        from {{ ref('fct_accident_weather') }}
    ) as weather_count

where

    (
        select count(*)
        from {{ ref('fct_accident') }}
    )

    <>

    (
        select count(*)
        from {{ ref('fct_accident_weather') }}
    )
