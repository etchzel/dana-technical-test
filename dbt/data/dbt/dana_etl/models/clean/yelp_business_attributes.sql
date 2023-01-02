{{ config(materialized='table') }}

select
	business_id,
	name as attribute_name,
	case
		when value like 'u''%''' then initcap(replace(regexp_replace(value, 'u''(.*)''', '\1'), 'plus', '+'))
		when value like '''%''' then initcap(replace(value, '''', ''))
		ELSE value
	END AS attribute_value
from {{ source('staging', 'yelp_business_attributes') }}
limit 100