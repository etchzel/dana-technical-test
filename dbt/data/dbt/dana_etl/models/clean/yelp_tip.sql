{{ config(materialized='table') }}

select
    user_id,
    business_id,
    date::timestamp as review_date,
    compliment_count,
    text
from {{ source('staging', 'yelp_tip') }}
limit 100