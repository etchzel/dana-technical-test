{{ config(materialized='table') }}

select
    user_id,
    yelping_since::timestamp as yelping_since,
    name,
    average_stars,
    review_count
from {{ ref('yelp_user') }}