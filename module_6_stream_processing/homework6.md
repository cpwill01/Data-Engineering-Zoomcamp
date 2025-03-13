

## Question 1: Redpanda version

Now let's find out the version of redpandas.

For that, check the output of the command `rpk help` inside the container. The name of the container is redpanda-1.

Find out what you need to execute based on the help output.

What's the version, based on the output of the command you executed? (copy the entire version)

Answer: `rpk version v24.2.18 (rev f9a22d4430)`. We can run `rpk help` inside the container using
```sh
docker compose exec redpanda-1 rpk help
```
Based on the output, we can see that we should run `rpk --version` inside the container.

## Question 2. Creating a topic

Before we can send data to the redpanda server, we
need to create a topic. We do it also with the `rpk`
command we used previously for figuring out the version of 
redpandas.

Read the output of `help` and based on it, create a topic with name `green-trips` 

What's the output of the command for creating a topic? Include the entire output in your answer.

Answer: The output is
```sh
TOPIC        STATUS
green-trips  OK 
```
Note: To create the topic, run `rpk topic create green-trips` inside the container.

## Question 3. Connecting to the Kafka server

We need to make sure we can connect to the server, so
later we can send some data to its topics

First, let's install the kafka connector (up to you if you
want to have a separate virtual environment for that)

```bash
pip install kafka-python
```

You can start a jupyter notebook in your solution folder or
create a script

Let's try to connect to our server:

```python
import json

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()
```

Provided that you can connect to the server, what's the output
of the last command?

Answer: `True`. 

Note: the function `KafkaProducer::bootstrap_connected()` is used to check
if the producer is connected to the bootstrap server(s).

## Question 4: Sending the Trip Data

Now we need to send the data to the `green-trips` topic

Read the data, and keep only these columns:

* `'lpep_pickup_datetime',`
* `'lpep_dropoff_datetime',`
* `'PULocationID',`
* `'DOLocationID',`
* `'passenger_count',`
* `'trip_distance',`
* `'tip_amount'`

Now send all the data using this code:

```python
producer.send(topic_name, value=message)
```

For each row (`message`) in the dataset. In this case, `message`
is a dictionary.

After sending all the messages, flush the data:

```python
producer.flush()
```

Use `from time import time` to see the total time 

```python
from time import time

t0 = time()

# ... your code

t1 = time()
took = t1 - t0
```

How much time did it take to send the entire dataset and flush? 

Answer: `950.9719321727753 seconds`

Note: The code is in `hw6_producer.py` in the same folder as this file.

## Question 5: Build a Sessionization Window (2 points)

Now we have the data in the Kafka stream. It's time to process it.

* Copy `aggregation_job.py` and rename it to `session_job.py`
* Have it read from `green-trips` fixing the schema
* Use a [session window](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/windows/) with a gap of 5 minutes
* Use `lpep_dropoff_datetime` time as your watermark with a 5 second tolerance
* Which pickup and drop off locations have the longest unbroken streak of taxi trips?

Answer: `PULocationID = 75, DOLocationID = 74`

Note: The [`session_job.py` file](./flink-training-main/src/job/session_job.py) is in `flink-training-main/src/job`. After Flink has populated Postgres, we can query the database using
```
SELECT 
	event_window_end - event_window_start AS session_window_length,
	pulocationid,
	dolocationid
FROM 
	processed_events_aggregated 
ORDER BY 
	1 DESC
```
to get the pickup and dropoff locations in order of session window length. Since a session window closes only when it doesn’t receive elements for a certain period of time (5 minutes in this case), the first record returned by this query represents the pickup and drop off locations with the longest unbroken streak of taxi trips.
