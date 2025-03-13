from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

def create_events_aggregated_sink(t_env):
    table_name = 'processed_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            event_window_start TIMESTAMP(3),
            event_window_end TIMESTAMP(3),
            PULocationID INT,
            DOLocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (event_window_start, PULocationID, DOLocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime TIMESTAMP(3),
            lpep_dropoff_datetime TIMESTAMP(3),
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count VARCHAR,
            trip_distance VARCHAR,
            tip_amount VARCHAR,
            WATERMARK for lpep_pickup_datetime as lpep_pickup_datetime - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_events_aggregated_sink(t_env)

        t_env.execute_sql(f"""
        INSERT INTO {aggregated_table}
        SELECT
            SESSION_START(lpep_pickup_datetime, INTERVAL '5' MINUTE) AS event_window_start, 
            SESSION_ROWTIME(lpep_pickup_datetime, INTERVAL '5' MINUTE) AS event_window_end, 
            PULocationID,
            DOLocationID,
            COUNT(*) AS num_trips
        FROM {source_table}
        GROUP BY SESSION(lpep_pickup_datetime, INTERVAL '5' MINUTE), PULocationID, DOLocationID;
        
        """).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()
