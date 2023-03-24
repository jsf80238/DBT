create view `verdant-bond-262820.weather_ods.source_measurement_dejson_v`
as select
  subscription_name
, message_id
, publish_time
, attributes
, json_value(cast(data as string), '$.attributes') as measurement_attributes
, json_value(cast(data as string), '$.datatype') as datatype
, json_value(cast(data as string), '$.date') as date
, json_value(cast(data as string), '$.station') as station
, json_value(cast(data as string), '$.value') as value
from `weather_ods.source`
where true
and json_value(attributes, '$.record_type') = 'measurement';