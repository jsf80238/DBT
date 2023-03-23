with

    datatype as (

        select
            id,
            name,
            row_number() over (partition by id order by publish_time desc) as rn
        from weather_ods.source_datatype_dejson_v
        qualify rn = 1

    ),

    missing_datatype as (
        -- For some reason TMAX and TMIN (max/min temperature) do not appear
        -- in https://www.ncei.noaa.gov/cdo-web/api/v2/datatypes
        select 'TMAX' as id, 'Maximum Temperature' as name
        union
        distinct
        select 'TMIN' as id, 'Minimum Temperature' as name

    ),

    final as (

        select id, name, current_datetime() as dbt_loaded_at

        from datatype

        union
        distinct

        select id, name, current_datetime() as dbt_loaded_at

        from missing_datatype

    )

select *
from final
