{{ config(materialized='table') }}

with person_vehicle as (

    select
        accident_key,
        accident_id,
        source_year,
        vehicle_id,

        max({{ normalize_missing_text('vehicle_type') }}) as vehicle_type_raw,
        max({{ normalize_missing_text('vehicle_brand') }}) as vehicle_make_model_raw,
        max(vehicle_manufacture_year) as vehicle_manufacture_year,

        count(*) as occupants,

        sum(coalesce(uninjured, 0)) as uninjured,
        sum(coalesce(minor_injury, 0)) as minor_injuries,
        sum(coalesce(serious_injury, 0)) as serious_injuries,
        sum(coalesce(death, 0)) as deaths

    from {{ ref('fct_person_involvement') }}

    where vehicle_id is not null

    group by
        accident_key,
        accident_id,
        source_year,
        vehicle_id
),

split_make_model as (

    select
        *,

        case
            when vehicle_make_model_raw is null then null
            when strpos(vehicle_make_model_raw, '/') > 0
                then split_part(vehicle_make_model_raw, '/', 1)
            else vehicle_make_model_raw
        end as vehicle_make_candidate,

        case
            when vehicle_make_model_raw is null then null
            when strpos(vehicle_make_model_raw, '/') > 0
                then substr(
                    vehicle_make_model_raw,
                    strpos(vehicle_make_model_raw, '/') + 1
                )
            else null
        end as vehicle_model_candidate

    from person_vehicle
),

cleaned as (

    select
        *,

        {{ normalize_missing_text('vehicle_make_candidate') }}
            as vehicle_make_raw,

        {{ normalize_missing_text('vehicle_model_candidate') }}
            as vehicle_model_raw

    from split_make_model
),

normalized as (

    select
        *,

        {{ normalize_vehicle_make('vehicle_make_raw') }}
            as vehicle_make,

        case
            when vehicle_model_raw is null then null
            else upper(vehicle_model_raw)
        end as vehicle_model,

        case
            when vehicle_type_raw is null then 'Não informado'
            else vehicle_type_raw
        end as vehicle_type

    from cleaned
),

final as (

    select
        md5(
            concat(
                cast(accident_key as varchar),
                '||',
                cast(vehicle_id as varchar)
            )
        ) as vehicle_involvement_key,

        accident_key,
        accident_id,
        source_year,
        vehicle_id,

        vehicle_type,

        vehicle_make_model_raw,

        coalesce(vehicle_make, 'Não informado') as vehicle_make,
        coalesce(vehicle_model, 'Não informado') as vehicle_model,

        case
            when vehicle_make is null and vehicle_model is null
                then 'Não informado'
            when vehicle_model is null
                then vehicle_make
            else concat(vehicle_make, ' ', vehicle_model)
        end as vehicle_make_model,

        vehicle_manufacture_year,

        case
            when vehicle_manufacture_year is null then null
            when vehicle_manufacture_year < 1900 then null
            when vehicle_manufacture_year > source_year then null
            else source_year - vehicle_manufacture_year
        end as vehicle_age,

        occupants,
        uninjured,
        minor_injuries,
        serious_injuries,
        deaths,

        (deaths > 0) as has_death,
        (serious_injuries > 0) as has_serious_injury,
        ((minor_injuries + serious_injuries + deaths) > 0) as has_casualty,

        (vehicle_make is null) as is_vehicle_make_missing,
        (vehicle_model is null) as is_vehicle_model_missing

    from normalized
)

select *
from final
