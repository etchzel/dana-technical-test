{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['checkin_id', 'value']) }} as checkin_detail_id,
    checkin_id,
    value::timestamp as checkin_timestamp
from {{ source('staging', 'yelp_checkin_details') }}