{{ config(materialized='table') }}

select
	business_id,
	name,
	address,
	city,
	state,
	postal_code,
	latitude::text,
	longitude::text,
	stars,
	review_count,
	is_open,
	categories
from {{ source('staging', 'yelp_business') }}
limit 100