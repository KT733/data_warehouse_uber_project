{{ config(materialized="table") }}

with all_loc as (
    select pickup_location as location
    from {{ ref('stg_uber_bookings') }}
    union distinct
    select drop_location as location
    from {{ ref('stg_uber_bookings') }}
),

dedup as (
    select
        md5(location) as location_id,
        location
    from all_loc
    where location is not null
)

select * from dedup
