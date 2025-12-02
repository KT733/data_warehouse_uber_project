{{ config(materialized="view") }}

with src as (
    select *
    from {{ source('google_drive', 'ncr_ride_bookings') }}
),

cleaned as (
    select
        booking_id,

        -- Convert date + time into proper timestamp
        try_to_date(date) as ride_date,
        try_to_time(time) as ride_time,
        try_to_timestamp(date || ' ' || time) as ride_ts,

        ride_distance ::float as ride_distance_km,

        vehicle_type,
        pickup_location,
        drop_location,
        booking_status,

        -- time features for analytics
        extract(hour from try_to_time(time)) as ride_hour,
        extract(dow from try_to_date(date)) as day_of_week,
        extract(month from try_to_date(date)) as month,
        extract(year from try_to_date(date)) as year
    from src
    where booking_status = 'Completed'
    and vehicle_type in ('Auto', 'UberXL')
    and ride_distance is not null
)

select * from cleaned
