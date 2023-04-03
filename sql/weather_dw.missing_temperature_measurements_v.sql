create or replace view weather_dw.missing_temperature_measurements_v as (
with days_of_interest as (
select day_
from unnest(
  generate_date_array(
    date_sub(current_date(), interval 7 day), current_date(), interval 1 day)
  ) as day_
),
day_station_combinations as (
select
  days_of_interest.day_ as day_
, station_dim.id as station_id
from days_of_interest
cross join weather_dw.station_dim
)
select
  day_
, station_id
from day_station_combinations dsc
where not exists (
  select true
    from weather_dw.measurement m
   where m.measurement_date = dsc.day_
     and m.station_id = dsc.station_id
)
)
;

alter view weather_dw.missing_temperature_measurements_v set options (
  description="Combinations of date/station which are missing from weather_dw.measurement over the last 7 days."
);
