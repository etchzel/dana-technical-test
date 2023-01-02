{{ config(materialized='table') }}

with weather as (
    {{ 
        dbt_utils.unpivot(
            relation=ref('weather_data'),
            cast_to='bool',
            exclude=['station_id', 'station_name', 'date_observed', 'latitude', 'longitude', 'elevation', 'temp_max', 'temp_min', 'temp_avg'],
            field_name='weather_type',
            value_name='weather_value'
        ) 
    }}
)

select
	station_id,
	station_name,
	trim(initcap((regexp_matches(station_name, '^(.+)INTERNATIONAL'))[1])) as city,
	date_observed,
	weather_type,
    coalesce(weather_value, 'false') as weather_value
from weather