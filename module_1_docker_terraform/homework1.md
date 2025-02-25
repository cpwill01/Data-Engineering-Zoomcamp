## Question 1: Understanding docker first run
Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint bash. What's the version of pip in the image?

Answer:
To run the image, we can use
```bash
docker run -it --entrypoint=bash python:3.12.8 
```
To check the version of pip, run
```bash
pip --version
```
## Question 2: Understanding Docker networking and docker-compose

Given the following docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```
Answer: Since pgadmin is running inside the same network as the postgres database, it will use port `5432` instead of `5433`. The hostname can be the container name or the name of the service, so the possible combinations are:
- `db:5432`
- `postgres:5432`

## Prepare Postgres
For Questions 3-6, the green taxi trips data and the zone lookup data were put into Postgres as `trips` and `zones` respectively.

```
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz

wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
``` 

## Question 3: Trip Segmentation Count 
During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:
- Up to 1 mile
- In between 1 (exclusive) and 3 miles (inclusive),
- In between 3 (exclusive) and 7 miles (inclusive),
- In between 7 (exclusive) and 10 miles (inclusive),
- Over 10 miles

Answer: `104,802, 198,924, 109,603, 27,678, 35,189` respectively, obtained from
```postgresql
SELECT 
    CASE 
        WHEN trip_distance <= 1 THEN '[0,1]'
        WHEN trip_distance > 1 AND trip_distance <= 3 THEN '(1,3]'
        WHEN trip_distance > 3 AND trip_distance <= 7 THEN '(3,7]'
        WHEN trip_distance > 7 AND trip_distance <= 10 THEN '(7,10]'
        WHEN trip_distance > 10 THEN '[10,inf)'
    END AS distance_bucket,
    COUNT(*) AS num_trips
FROM
    trips
WHERE
    lpep_pickup_datetime >= '2019-10-01'
    AND lpep_pickup_datetime < '2019-11-01' 
    AND lpep_dropoff_datetime >= '2019-10-01' 
    AND lpep_dropoff_datetime < '2019-11-01'
GROUP BY
    distance_bucket
```

## Question 4: Day with longest trip
Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

Answer: `2019-10-31`, obtained from
```postgresql
SELECT
    lpep_pickup_datetime::date
FROM
    trips
ORDER BY
    trip_distance DESC
LIMIT 
    1
```
## Question 5: Three biggest pickup zones
Which were the top 3 pickup locations by `total_amount` (across all trips) for `2019-10-18`? Consider only `lpep_pickup_datetime` when filtering by date.

Answer: `East Harlem North, East Harlem South, Morningside Heights`, obtained from
```postgresql
SELECT
    z.zone
FROM 
    trips t
    INNER JOIN zones z
    ON t.PULocationID = z.location_id
WHERE
    t.lpep_pickup_datetime = '2019-10-18'
GROUP BY
    z.zone
ORDER BY
    SUM(t.total_amount)
LIMIT 
    3
```
## Question 6: Zone with largest tip
For the passengers picked up in the zone named "East Harlem North", which was the drop off zone that had the largest tip?

Answer: `JFK Airport`, obtained from
```postgresql
SELECT
    zdo.zone as dropoff_zone,
FROM 
    trips t
    INNER JOIN zones zpu
    ON t.PULocationID = zpu.location_id
    INNER JOIN zones zdo
    ON t.DOLocationID = zdo.location_id
WHERE
    zpu.zone = 'East Harlem North'
ORDER BY
    t.tip_amount desc
LIMIT 
    1
```
## Question 7: Terraform workflow
Which commands are used to perform the following?

- Downloading the provider plugins and setting up backend,
- Generating proposed changes and auto-executing the plan
- Remove all resources managed by terraform

Answer: `terraform init`,  `terraform apply -auto-approve`, and `terraform destroy` respectively.

