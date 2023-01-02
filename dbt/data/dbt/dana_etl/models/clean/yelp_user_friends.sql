{{ config(materialized='table') }}

select
	user_id,
	value as friend_id
from {{ source('staging', 'yelp_user_friends')}}