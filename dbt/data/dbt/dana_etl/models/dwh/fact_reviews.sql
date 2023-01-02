{{ config(materialized='table') }}

select
    review_id,
    review.business_id,
    user_id,
    city,
    review.stars as review_star,
    text as review_text,
    review_date::date as review_date,
    review_date as review_timestamp
from {{ ref('yelp_review') }} review
left join {{ ref('yelp_business') }} business on review.business_id = business.business_id