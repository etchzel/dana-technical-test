{{ config(materialized='table') }}

select distinct
	user_id,
	case
		when value = '' then null
		when value = '20' then 2020
		else value::int
	end as year
from {{ source('staging', 'yelp_user_elite_years')}}