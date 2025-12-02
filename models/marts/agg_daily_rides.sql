{{ config(materialized="table") }}

select
    ride_date,
    count(*) as total_rides,
    avg(ride_distance_km) as avg_distance,
    count_if(vehicle_type = 'Auto') as auto_rides,
    count_if(vehicle_type = 'UberXL') as uberxl_rides
from {{ ref('fact_rides') }}
where booking_status = 'Completed'
group by ride_date
order by ride_date
