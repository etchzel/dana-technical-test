{{ config(materialized='table') }}

select
	STATION as station_id,
	NAME as station_name,
	LATITUDE as latitude,
	LONGITUDE as longitude,
	ELEVATION as elevation,
	DATE::date as date_observed,
	TMAX::float as temp_max,
	TMIN::float as temp_min,
	TAVG::float as temp_avg,
	WT01 as fog,
	WT02 as heavy_fog,
	WT03 as thunder,
	WT04 as soft_hail,
	WT05 as hail,
	WT06 as rime,
	WT07 as dust,
	WT08 as haze,
	WT09 as blowing_snow,
	WT10 as tornado
from {{ source('staging', 'weather_data') }}
order by date_observed
limit 100