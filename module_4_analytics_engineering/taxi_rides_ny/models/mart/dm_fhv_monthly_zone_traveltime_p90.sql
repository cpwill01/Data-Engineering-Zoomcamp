{{ config(materialized='table') }}

with traveltimes_p90 as (
    select 
        pickup_year as year, 
        pickup_month as month,
        pickup_locationid,
        pickup_borough,
        pickup_zone,
        dropoff_locationid,
        dropoff_borough,
        dropoff_zone,
        percentile_cont(
            {{ datediff("pickup_datetime", "dropoff_datetime", "second") }}, 
            0.90
            ) over (partition by pickup_year, pickup_month, pickup_locationid, dropoff_locationid)
            as trip_duration_p90
    from {{ ref('fact_fhv_trips') }}
)
select 
    year,
    month,
    pickup_locationid,
    any_value(pickup_borough) as pickup_borough,
    any_value(pickup_zone) as pickup_zone,
    dropoff_locationid,
    any_value(dropoff_borough) as dropoff_borough,
    any_value(dropoff_zone) as dropoff_zone,
    any_value(trip_duration_p90) as trip_duration_p90
from
    traveltimes_p90
group by year, month, pickup_locationid, dropoff_locationid