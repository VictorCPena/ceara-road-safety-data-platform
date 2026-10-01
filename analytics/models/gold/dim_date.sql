with dates as (

    select distinct

        event_date

    from {{ source(
        'silver_prf',
        'occurrence'
    ) }}

    where
        state = 'CE'
        and event_date is not null

)

select

    cast(
        strftime(
            event_date,
            '%Y%m%d'
        )
        as integer
    ) as date_key,

    event_date,

    year(
        event_date
    ) as year,

    quarter(
        event_date
    ) as quarter,

    month(
        event_date
    ) as month,

    day(
        event_date
    ) as day,

    dayofweek(
        event_date
    ) as day_of_week,

    case

        when dayofweek(event_date) = 0
            then 'Domingo'

        when dayofweek(event_date) = 1
            then 'Segunda-feira'

        when dayofweek(event_date) = 2
            then 'Terça-feira'

        when dayofweek(event_date) = 3
            then 'Quarta-feira'

        when dayofweek(event_date) = 4
            then 'Quinta-feira'

        when dayofweek(event_date) = 5
            then 'Sexta-feira'

        when dayofweek(event_date) = 6
            then 'Sábado'

    end as weekday_name,

    case

        when month(event_date)
            in (1, 2, 3)
            then 1

        when month(event_date)
            in (4, 5, 6)
            then 2

        when month(event_date)
            in (7, 8, 9)
            then 3

        else 4

    end as quarter_number

from dates
