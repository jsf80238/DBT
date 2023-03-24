create view `verdant-bond-262820.weather_ods.source_datatype_dejson_v`
as select
  subscription_name
, message_id
, publish_time
, attributes
, json_value(cast(data as string), '$.name') as name
, json_value(cast(data as string), '$.id') as id
from `weather_ods.source`
where true
and json_value(attributes, '$.record_type') = 'datatype';