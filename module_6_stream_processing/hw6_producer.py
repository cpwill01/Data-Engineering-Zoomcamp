import json
import time

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

print("producer.bootstrap_connected() returns:", producer.bootstrap_connected())

t0 = time.time()

topic_name = 'green-trips'
KEEP_COLS = {'lpep_pickup_datetime', 'lpep_dropoff_datetime','PULocationID',
             'DOLocationID','passenger_count','trip_distance','tip_amount'}
    
with open('./flink-training-main/data/green_tripdata_2019-10.csv', 'r') as f:
    header = next(f).split(',')
    idx_and_col = [(i, col_name) for i, col_name in enumerate(header) if col_name in KEEP_COLS]
    for line in f:
        items = line.split(',')
        message = {col_name : items[i] for i, col_name in idx_and_col}
        producer.send(topic_name, value=message)
        print(f"Sent: {message}")

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0)} seconds')