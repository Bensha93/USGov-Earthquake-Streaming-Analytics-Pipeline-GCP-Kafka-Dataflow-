import json
import time
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringDeserializer
from google.cloud import bigquery

# --- Kafka Configuration ---
KAFKA_CONF = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'C5HS5POGL2M5URBY',
    'sasl.password': 'cflt2cVULwS8cyE1Z//PszlKQcqbNA4pdl3AdhVzGQR6ZYitS3bEAmNVtM3sWg9Q',
    'group.id': 'earthquake-bq-consumer',
    'auto.offset.reset': 'earliest'
}

SCHEMA_REGISTRY_CONF = {
    'url': 'https://psrc-zgxmd0p.us-east1.gcp.confluent.cloud',
    'basic.auth.user.info': 'A25RAHQW374TDEX4:cfltwQUucJypC0wKQbBN0seokmoDr6ZmB1Hw3A8HZalW+m9gdyk+BGs4xfTJC92w'
}

TOPIC = "earthquake-avro-stream-v2"

# --- BigQuery Setup ---
bq_client = bigquery.Client()
table_id = "earthquake-475820.earthquake_data.usgs_events"  # project.dataset.table

# --- Define Avro Schema for Deserialization ---
value_schema_str = """
{
  "type": "record",
  "name": "EarthquakeEvent",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "time", "type": "long"},
    {"name": "mag", "type": ["null", "float"], "default": null},
    {"name": "place", "type": ["null", "string"], "default": null},
    {"name": "url", "type": ["null", "string"], "default": null},
    {"name": "lon", "type": ["null", "double"], "default": null},
    {"name": "lat", "type": ["null", "double"], "default": null},
    {"name": "depth_km", "type": ["null", "double"], "default": null}
  ]
}
"""

# --- Initialize Schema Registry & Deserializer ---
schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONF)
avro_deserializer = AvroDeserializer(
    schema_registry_client,
    value_schema_str
)
string_deserializer = StringDeserializer('utf_8')

consumer = DeserializingConsumer({
    **KAFKA_CONF,
    'key.deserializer': string_deserializer,
    'value.deserializer': avro_deserializer
})

consumer.subscribe([TOPIC])

print(f" Listening to topic '{TOPIC}' and writing to BigQuery table: {table_id}")

# --- Consume and Write Loop ---
while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f" Consumer error: {msg.error()}")
            continue

        record = msg.value()
        if not record:
            continue

        # Prepare record for BigQuery
        row = {
            "id": record.get("id"),
            "time": record.get("time"),
            "mag": record.get("mag"),
            "place": record.get("place"),
            "url": record.get("url"),
            "lon": record.get("lon"),
            "lat": record.get("lat"),
            "depth_km": record.get("depth_km")
        }

        # Insert into BigQuery
        errors = bq_client.insert_rows_json(table_id, [row])
        if errors:
            print(f" BigQuery insert error: {errors}")
        else:
            print(f" Inserted record {row['id']} with magnitude {row['mag']}")

    except KeyboardInterrupt:
        break
    except Exception as e:
        print(" Error:", e)
        time.sleep(5)

consumer.close()
