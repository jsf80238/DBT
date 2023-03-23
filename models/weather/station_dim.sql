{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge'
    )
}}

with

    station as (

        select
        id
        , name
        , cast(elevation as float64) as elevation
        , cast(latitude as float64) as latitude
        , cast(longitude as float64) as longitude
        , row_number() over (partition by id order by publish_time desc) as rn
        from weather_ods.source_station_dejson_v
        qualify rn = 1

    ),

    final as (

        select
          *
          , current_datetime() as dbt_loaded_at

        from station
        {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- where date_day >= (select max(date_day) from {{ this }})

        {% endif %}

    )

select *
from final
