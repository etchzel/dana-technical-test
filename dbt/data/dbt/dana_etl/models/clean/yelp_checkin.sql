{{ config(materialized='table') }}

with checkin_counts as (
    select
        checkin_id,
        count(value) as checkin_count
    from {{ source('staging', 'yelp_checkin_details') }}
    group by 1
)

select
    checkin.checkin_id,
    business_id,
    checkin_count
from {{ source('staging', 'yelp_checkin') }} checkin
join checkin_counts on checkin.checkin_id = checkin_counts.checkin_id
limit 100