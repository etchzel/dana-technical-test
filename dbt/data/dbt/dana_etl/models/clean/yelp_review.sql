{{ config(materialized='table') }}

select
    review_id,
    user_id,
    business_id,
    date::timestamp as review_date,
    stars,
    text,
    useful,
    funny,
    cool
from {{ source('staging', 'yelp_review') }}