Note: For Questions 5-7, the following datasets were ingested into BigQuery:
* [Green Taxi dataset (2019 and 2020)](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green)
* [Yellow Taxi dataset (2019 and 2020)](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/yellow)
* [For Hire Vehicle dataset (2019)](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv)

using the [Kestra workflow in the module 2 folder](../module_2_workflow_orchestration/taxi_ingestion_to_gcs_gcp_trigger_flow.yaml)

## Question 1: Understanding dbt model resolution

Provided you've got the following sources.yaml
```yaml
version: 2

sources:
  - name: raw_nyc_tripdata
    database: "{{ env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') }}"
    schema:   "{{ env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') }}"
    tables:
      - name: ext_green_taxi
      - name: ext_yellow_taxi
```

with the following env variables setup where dbt runs:
```sh
export DBT_BIGQUERY_PROJECT=myproject
export DBT_BIGQUERY_DATASET=my_nyc_tripdata
```

What does this .sql model compile to?
```sql
select * 
from {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}
```

Answer: `` select * from `myproject`.`raw_nyc_tripdata`.`ext_green_taxi` ``

On `dbt core`, this can be checked by creating the `schema.yaml` and `question1.sql` files under `models/tmp`, 
running the `export` commands, then running:
```sh
dbt compile --select "question1"
```
On `dbt cloud`, we can also create the same files, but the environment variables need to be set in 
`Deploy > Environments > Environment variables`. After that, click `Compile` and check the `Compiled code`
tab.


## Question 2: dbt Variables & Dynamic Models

Say you have to modify the following dbt_model (`fct_recent_taxi_trips.sql`) to enable Analytics Engineers to dynamically control the date range.

 In development, you want to process only **the last 7 days of trips**
- In production, you need to process **the last 30 days** for analytics

```sql
select *
from {{ ref('fact_taxi_trips') }}
where pickup_datetime >= CURRENT_DATE - INTERVAL '30' DAY
```

What would you change to accomplish that in a such way that command line arguments takes precedence over ENV_VARs, which takes precedence over DEFAULT value?

Answer: Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`

From the [dbt documentation](https://docs.getdbt.com/reference/dbt-jinja-functions/var), "The `var()` function takes an optional second argument, default. If this argument is provided, then it will be the default value for the variable if one is not explicitly defined."
The syntax is similar for `env_var`. Hence, `'{{ var("days_back", env_var("DAYS_BACK", "30")) }}'` will first check for the command line argument `"days_back"`, then check for the environment variable if no such argument was specified, and finally default to `"30"` if both are not defined.

## Question 3: dbt Data Lineage and Execution

Considering the data lineage below **and** that taxi_zone_lookup is the **only** materialization build (from a .csv seed file):

![image](./homework_q2.png)

Select the option that does **NOT** apply for materializing `fct_taxi_monthly_zone_revenue`:

- `dbt run`
- `dbt run --select +models/core/dim_taxi_trips.sql+ --target prod`
- `dbt run --select +models/core/fct_taxi_monthly_zone_revenue.sql`
- `dbt run --select +models/core/`
- `dbt run --select models/staging/+`

Answer: `dbt run --select models/staging/+`. This command does not run the `taxi_zone_lookup` and `dim_zone_lookup` models as they are not downstream from any of the staging models.

## Question 4: dbt Macros and Jinja

Consider you're dealing with sensitive data (e.g.: [PII](https://en.wikipedia.org/wiki/Personal_data)), that is **only available to your team and very selected few individuals**, in the `raw layer` of your DWH (e.g: a specific BigQuery dataset or PostgreSQL schema), 

 - Among other things, you decide to obfuscate/masquerade that data through your staging models, and make it available in a different schema (a `staging layer`) for other Data/Analytics Engineers to explore

- And **optionally**, yet  another layer (`service layer`), where you'll build your dimension (`dim_`) and fact (`fct_`) tables (assuming the [Star Schema dimensional modeling](https://www.databricks.com/glossary/star-schema)) for Dashboarding and for Tech Product Owners/Managers

You decide to make a macro to wrap a logic around it:

```sql
{% macro resolve_schema_for(model_type) -%}

    {%- set target_env_var = 'DBT_BIGQUERY_TARGET_DATASET'  -%}
    {%- set stging_env_var = 'DBT_BIGQUERY_STAGING_DATASET' -%}

    {%- if model_type == 'core' -%} {{- env_var(target_env_var) -}}
    {%- else -%}                    {{- env_var(stging_env_var, env_var(target_env_var)) -}}
    {%- endif -%}

{%- endmacro %}
```

And use on your staging, dim_ and fact_ models as:
```sql
{{ config(
    schema=resolve_schema_for('core'), 
) }}
```

That all being said, regarding macro above, **select all statements that are true to the models using it**:
- Setting a value for  `DBT_BIGQUERY_TARGET_DATASET` env var is mandatory, or it'll fail to compile
- Setting a value for `DBT_BIGQUERY_STAGING_DATASET` env var is mandatory, or it'll fail to compile
- When using `core`, it materializes in the dataset defined in `DBT_BIGQUERY_TARGET_DATASET`
- When using `stg`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`
- When using `staging`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`

Answer: Statements 1, 3, 4 and 5 are true. Statement 2 is false because `{{- env_var(stging_env_var, env_var(target_env_var)) -}}` compiles successfully even when `DBT_BIGQUERY_STAGING_DATASET` is not set, as long as `DBT_BIGQUERY_TARGET_DATASET` is set.

## Question 5: Taxi Quarterly Revenue Growth

1. Create a new model `dm_taxi_trips_quarterly_revenue.sql`
2. Compute the Quarterly Revenues for each year for based on `total_amount`
3. Compute the Quarterly YoY (Year-over-Year) revenue growth 
  * e.g.: In 2020/Q1, Green Taxi had -12.34% revenue growth compared to 2019/Q1
  * e.g.: In 2020/Q4, Yellow Taxi had +34.56% revenue growth compared to 2019/Q4

***Important Note: The Year-over-Year (YoY) growth percentages provided in the examples are purely illustrative. You will not be able to reproduce these exact values using the datasets provided for this homework.***

Considering the YoY Growth in 2020, which were the yearly quarters with the best (or less worse) and worst results for green, and yellow?

Answer: `green: {best: 2020/Q1, worst: 2020/Q2}` at -56% and -92% respectively, `yellow: {best: 2020/Q1, worst: 2020/Q2}` at -19% and -92% respectively. 

Note: The model I wrote to obtain the answer is available [here](./taxi_rides_ny/models/mart/dm_taxi_trips_quarterly_revenue.sql). 
After building this, we can just look at the data preview since the table is small enough.

## Question 6: P97/P95/P90 Taxi Monthly Fare

1. Create a new model `dm_taxi_trips_monthly_fare_p95.sql`
2. Filter out invalid entries (`fare_amount > 0`, `trip_distance > 0`, and `payment_type_description in ('Cash', 'Credit card')`)
3. Compute the **continous percentile** of `fare_amount` partitioning by service_type, year and and month

Now, what are the values of `p97`, `p95`, `p90` for Green Taxi and Yellow Taxi, in April 2020?

Answer: `green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}`

Note: The model I wrote to obtain the answer is available [here](./taxi_rides_ny/models/mart/dm_taxi_trips_monthly_fare_p95.sql)
After building this, we can just look at the data preview since the table is small enough.

## Question 7: Top #Nth longest P90 travel time Location for FHV

Prerequisites:
- [x] Create a staging model for FHV Data (2019), and **DO NOT** add a deduplication step, just filter out the entries where `where dispatching_base_num is not null`
- [x] Create a core model for FHV Data (`fact_fhv_trips.sql`) joining with `dim_zones`. Similar to what has been done [here](./taxi_rides_ny/models/core/fact_trips.sql)
- [x] Add some new dimensions `year` (e.g.: 2019) and `month` (e.g.: 1, 2, ..., 12), based on `pickup_datetime`, to the core model to facilitate filtering for your queries

Now...
1. Create a new model `dm_fhv_monthly_zone_traveltime_p90.sql`
2. For each record in `fact_fhv_trips.sql`, compute the [timestamp_diff](https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff) in seconds between dropoff_datetime and pickup_datetime - we'll call it `trip_duration` for this exercise
3. Compute the **continous** `p90` of `trip_duration` partitioning by year, month, pickup_location_id, and dropoff_location_id

For the Trips that **respectively** started from `Newark Airport`, `SoHo`, and `Yorkville East`, in November 2019, what are **dropoff_zones** with the 2nd longest p90 trip_duration?

Answer: `LaGuardia Airport, Chinatown, Garment District` respectively.

Note: The model I wrote to obtain the answer is available [here](./taxi_rides_ny/models/mart/dm_fhv_monthly_zone_traveltime_p90.sql), with the prerequisite `fact_fhv_trips` model in the `models/core` folder.
After building these, I ran the following query on BigQuery:
```sql 
WITH ranks AS (
  SELECT 
    pickup_zone,
    dropoff_zone,
    DENSE_RANK() OVER (PARTITION BY pickup_zone ORDER BY trip_duration_p90 DESC) as p90_rank
  FROM `<BigQuery_project_name>.<BigQuery_dataset_name>.dm_fhv_monthly_zone_traveltime_p90`
  WHERE
    year = 2019
    AND month = 11
    AND lower(pickup_zone) in ('newark airport', 'soho', 'yorkville east')
)
SELECT
  pickup_zone,
  dropoff_zone
FROM ranks
WHERE p90_rank = 2; 
```