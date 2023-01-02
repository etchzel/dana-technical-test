{{ config(materialized='table') }}

select
	business.business_id,
	name,
	category_name as business_category,
	address,
	city,
	state,
	postal_code,
	stars,
	review_count,
	is_open
from {{ ref('yelp_business') }} business
left join {{ ref('yelp_business_category') }} categories 
on business.business_id = categories.business_id 