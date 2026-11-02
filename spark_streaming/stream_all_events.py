# Run the script using the following command
# spark-submit \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
# stream_all_events.py

import os
from streaming_functions import *
from schema import schema

# Kafka Topics
LISTEN_EVENTS_TOPIC = "listen_events"
PAGE_VIEW_EVENTS_TOPIC = "page_view_events"
AUTH_EVENTS_TOPIC = "auth_events"

KAFKA_PORT = "9092"

KAFKA_ADDRESS = os.getenv("KAFKA_ADDRESS", 'localhost')
GCP_GCS_BUCKET = os.getenv("GCP_GCS_BUCKET", 'streamify')
# STORAGE_PATH=/some/local/dir to write locally instead of GCS
GCS_STORAGE_PATH = os.getenv("STORAGE_PATH", f'gs://{GCP_GCS_BUCKET}')
# TRIGGER_INTERVAL: how often to flush to storage (default 2 minutes)
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "120 seconds")
# SOURCE=file + INPUT_PATH=<dir> reads JSON lines instead of Kafka (local testing)
SOURCE = os.getenv("SOURCE", "kafka")
INPUT_PATH = os.getenv("INPUT_PATH", "./sample_input")

def read_stream(spark, topic):
    if SOURCE == "file":
        return create_file_read_stream(spark, INPUT_PATH, topic)
    return create_kafka_read_stream(spark, KAFKA_ADDRESS, KAFKA_PORT, topic)


# initialize a spark session
spark = create_or_get_spark_session('Eventsim Stream')
spark.streams.resetTerminated()
# listen events stream
listen_events = read_stream(spark, LISTEN_EVENTS_TOPIC)
listen_events = process_stream(
    listen_events, schema[LISTEN_EVENTS_TOPIC], LISTEN_EVENTS_TOPIC)

# page view stream
page_view_events = read_stream(spark, PAGE_VIEW_EVENTS_TOPIC)
page_view_events = process_stream(
    page_view_events, schema[PAGE_VIEW_EVENTS_TOPIC], PAGE_VIEW_EVENTS_TOPIC)

# auth stream
auth_events = read_stream(spark, AUTH_EVENTS_TOPIC)
auth_events = process_stream(
    auth_events, schema[AUTH_EVENTS_TOPIC], AUTH_EVENTS_TOPIC)

# write a file to storage every 2 minutes in parquet format
listen_events_writer = create_file_write_stream(listen_events,
                                                f"{GCS_STORAGE_PATH}/{LISTEN_EVENTS_TOPIC}",
                                                f"{GCS_STORAGE_PATH}/checkpoint/{LISTEN_EVENTS_TOPIC}",
                                                trigger=TRIGGER_INTERVAL
                                                )

page_view_events_writer = create_file_write_stream(page_view_events,
                                                   f"{GCS_STORAGE_PATH}/{PAGE_VIEW_EVENTS_TOPIC}",
                                                   f"{GCS_STORAGE_PATH}/checkpoint/{PAGE_VIEW_EVENTS_TOPIC}",
                                                   trigger=TRIGGER_INTERVAL
                                                   )

auth_events_writer = create_file_write_stream(auth_events,
                                              f"{GCS_STORAGE_PATH}/{AUTH_EVENTS_TOPIC}",
                                              f"{GCS_STORAGE_PATH}/checkpoint/{AUTH_EVENTS_TOPIC}",
                                              trigger=TRIGGER_INTERVAL
                                              )


listen_events_writer.start()
auth_events_writer.start()
page_view_events_writer.start()

# RUN_SECONDS=N stops the job after N seconds (handy for local runs / `make all`)
RUN_SECONDS = os.getenv("RUN_SECONDS")
if RUN_SECONDS:
    spark.streams.awaitAnyTermination(int(RUN_SECONDS))
    for q in spark.streams.active:
        q.stop()
else:
    spark.streams.awaitAnyTermination()
