{{ config(materialized='table') }}

select
    user_id,
    yelping_since::timestamp as yelping_since,
    name,
    average_stars,
    review_count,
    useful,
    funny,
    cool,
    fans
from {{ source('staging', 'yelp_user') }}
limit 100