{{ config(materialized="table") }}

select
    pickup_location,
    count(*) as total_pickups
from {{ ref('fact_rides') }}
group by pickup_location
order by total_pickups desc
