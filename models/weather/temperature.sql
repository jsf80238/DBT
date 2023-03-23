with
    measurement_type_dim as (select * from {{ ref("measurement_type_dim") }}),

    station_dim as (select * from {{ ref("station_dim") }}),

    measurement as (select * from {{ ref("measurement") }}),

    temperature_data as (

        select
            sd.id as station_id,
            sd.name as station_name,
            sd.elevation as station_elevation_meters,
            sd.latitude as station_latitude,
            sd.longitude as station_longitude,
            mtd.id as measurement_type_id,
            mtd.name as measurement_type_name,
            m.measurement_date,
            m.measurement as measurement_celcius
        from measurement as m
        join station_dim as sd on m.station_id = sd.id
        join measurement_type_dim as mtd on m.measurement_type_id = mtd.id
        where true and mtd.id in ('TMIN', 'TMAX')

    ),

    final as (

        select
            station_id,
            station_name,
            station_elevation_meters,
            station_latitude,
            station_longitude,
            measurement_type_id,
            measurement_type_name,
            measurement_date,
            measurement_celcius,
            current_datetime() as dbt_loaded_at

        from temperature_data

    )

select *
from final
