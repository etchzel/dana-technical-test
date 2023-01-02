select
	business_id,
	"name",
	address,
	city,
	"state",
	postal_code,
	latitude::text,
	longitude::text,
	stars,
	review_count,
	is_open,
	categories,
	json_build_object(
		'Monday', "hours_Monday",
		'Tuesday', "hours_Tuesday",
        'Wednesday', "hours_Wednesday",
        'Thursday', "hours_Thursday",
        'Friday', "hours_Friday",
        'Saturday', "hours_Saturday",
		'Sunday', "hours_Sunday"
	) as open_hours
from staging.yelp_business limit 10;