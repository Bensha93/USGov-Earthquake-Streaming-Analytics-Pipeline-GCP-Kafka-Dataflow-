import time
import requests
from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro

# --- Config ---
USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

KAFKA_CONF = {
    "bootstrap.servers": "localhost:9092",
    "schema.registry.url": "http://localhost:8081"  # adjust for your Schema Registry endpoint
}

TOPIC = "earthquake-avro-stream"

# Define Avro Schema
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

value_schema = avro.loads(value_schema_str)

# --- Create Avro Producer ---
producer = AvroProducer(KAFKA_CONF, default_value_schema=value_schema)

seen_ids = set()

def fetch_and_send():
    resp = requests.get(USGS_URL, timeout=20)
    data = resp.json()
    for feat in data.get("features", []):
        eid = feat.get("id")
        if eid in seen_ids:
            continue
        seen_ids.add(eid)

        props = feat.get("properties", {})
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates") or [None, None, None]

        msg = {
            "id": eid,
            "time": int(props.get("time") or 0),
            "mag": props.get("mag"),
            "place": props.get("place"),
            "url": props.get("url"),
            "lon": coords[0],
            "lat": coords[1],
            "depth_km": coords[2]
        }

        producer.produce(topic=TOPIC, value=msg)

    producer.flush()

if __name__ == "__main__":
    while True:
        try:
            fetch_and_send()
        except Exception as e:
            print("Error:", e)
        time.sleep(60)
