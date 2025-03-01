{{ config(materialized="view") }}

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(["dispatching_base_num", "pulocationid", "pickup_datetime"]) }} as tripid,
    dispatching_base_num,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    affiliated_base_num,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    {{ dbt.safe_cast("SR_flag", api.Column.translate_type("integer")) }} as sr_flag
from {{ source("staging", "fhv_tripdata") }}
where dispatching_base_num is not null

-- dbt build --select <model> --vars '{'is_test_run': false}'
{% if var("is_test_run", default=true) %} 
    limit 10
{% endif %}
