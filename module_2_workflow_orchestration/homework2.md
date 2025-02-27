The data required for the homework include the yellow taxi trip data and greed taxi trip data from year 2020 to 2021. To ingest the data, we can use Kestra's backfill functionality on the [taxi_ingestion_with_trigger_flow](./taxi_ingestion_with_trigger_flow.yaml) for the time period `2020-01-01` to `2021-07-31`

## Question 1
Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the extract task)?

Answer: `128.3 MB`. The answer can be obtained by selecting the file under the `Outputs` tab in Kestra.

## Question 2
What is the rendered value of the variable file when the inputs taxi is set to `green`, year is set to `2020`, and month is set to `04` during execution?


Answer: `green_tripdata_2020-04.csv`. This can be checked in the `Debug Outputs` functionality under the `Outputs` tab.

## Questions 3 - 5
How many rows are there for the `Yellow` Taxi data for all CSV files in the year `2020`?

How many rows are there for the `Green` Taxi data for all CSV files in the year `2020`?

How many rows are there for the `Yellow` Taxi data for the `March 2021` CSV file?


Answer: `24,648,499`, `1,734,051` and `1,925,152` respectively. This can be checked from the `Outputs` tab for the respective runs, or by querying from the data warehouse.

## Questions 6
How would you configure the timezone to `New York` in a `Schedule` trigger?


Answer: Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration, e.g.

```yaml
triggers:
  - id: green_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *"
    inputs:
      taxiType: "green"
    timezone: "America/New_York"
```

Note:  Based on the [Schedule Trigger documentation](https://kestra.io/plugins/core/trigger/io.kestra.plugin.core.trigger.schedule), the format for the timezone is the `time zone identifier` of the IANA time zone database (second column in this [Wikipedia table](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List))
