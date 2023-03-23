{{
    config(
        materialized="incremental",
        unique_key=["station_id", "measurement_type_id", "measurement_date"],
        incremental_strategy="merge",
    )
}}

with

    measurement_data as (

        select
            station as station_id,
            datatype as measurement_type_id,
            parse_date('%Y-%m-%d', left(date, 10)) as measurement_date,
            cast(value as float64) as measurement,
            row_number() over (
                partition by station, datatype order by publish_time desc
            ) as rn
        from weather_ods.source_measurement_dejson_v
        qualify rn = 1

    ),

    final as (

        select
            station_id,
            measurement_type_id,
            measurement_date,
            measurement,
            current_datetime() as dbt_loaded_at

        from
            measurement_data

            {% if is_incremental() %}

            -- this filter will only be applied on an incremental run
            -- where date_day >= (select max(date_day) from {{ this }})
            {% endif %}

    )

select *
from final
