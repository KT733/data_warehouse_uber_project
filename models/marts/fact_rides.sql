{{ config(materialized="table") }}

with b as (
    select *
    from {{ ref('stg_uber_bookings') }}
),

joined as (
    select
        b.booking_id,
        b.ride_ts,
        b.ride_date,
        b.ride_hour,
        b.day_of_week,
        b.month,
        b.year,
        b.ride_distance_km,
        b.vehicle_type,
        b.booking_status,
        b.pickup_location,
        b.drop_location,

        dl1.location_id as pickup_id,
        dl2.location_id as dropoff_id
    from b
    left join {{ ref('dim_locations') }} dl1
        on b.pickup_location = dl1.location
    left join {{ ref('dim_locations') }} dl2
        on b.drop_location = dl2.location
)

select * from joined
