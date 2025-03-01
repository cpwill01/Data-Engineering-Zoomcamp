{{ config(materialized="table") }}

with
    p_window as (
        select
            service_type,
            pickup_year as year,
            pickup_month as month,
            round(
                percentile_cont(fare_amount, 0.97) over (
                    partition by service_type, pickup_year, pickup_month
                ),
                2
            ) as fare_amount_p97,
            round(
                percentile_cont(fare_amount, 0.95) over (
                    partition by service_type, pickup_year, pickup_month
                ),
                2
            ) as fare_amount_p95,
            round(
                percentile_cont(fare_amount, 0.90) over (
                    partition by service_type, pickup_year, pickup_month
                ),
                2
            ) as fare_amount_p90,
        from {{ ref("fact_trips") }}
        where
            pickup_year >= 2019
            and pickup_year <= 2020
            and fare_amount > 0
            and trip_distance > 0
            and lower(payment_type_description) in ('cash', 'credit card')
    )
select
    service_type,
    year,
    month,
    any_value(fare_amount_p97) as fare_amount_p97,
    any_value(fare_amount_p95) as fare_amount_p95,
    any_value(fare_amount_p90) as fare_amount_p90
from p_window
group by service_type, year, month
order by service_type, year, month
