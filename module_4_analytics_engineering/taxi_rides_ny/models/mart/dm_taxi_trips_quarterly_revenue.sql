{{ config(materialized='table') }}

with quarterly_revenue as (
    select 
        -- Revenue grouping 
        service_type, 
        pickup_year as year,
        pickup_quarter as quarter,

        -- Revenue calculation 
        sum(total_amount) as quarterly_total_amount,
    from {{ ref('fact_trips') }}
    where pickup_year >= 2019 and pickup_year <= 2020
    group by service_type, year, quarter
)
select
    service_type,
    year,
    quarter,
    quarterly_total_amount,
    {{ dbt_utils.safe_divide(
        'quarterly_total_amount - lag(quarterly_total_amount, 1) over (partition by service_type, quarter order by year)',
        'lag(quarterly_total_amount, 1) over (partition by service_type, quarter order by year)'
        ) }} * 100 as yoy_growth
from
    quarterly_revenue