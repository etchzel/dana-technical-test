{{ config(materialized='table') }}

select
    business_id,
    value as category_name
from {{ source('staging', 'yelp_business_category') }}