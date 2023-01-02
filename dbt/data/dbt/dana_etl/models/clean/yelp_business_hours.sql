{{ config(materialized='table') }}

select
    business_id,
    name as day,
    value as hours
from {{ source('staging', 'yelp_business_hours') }}