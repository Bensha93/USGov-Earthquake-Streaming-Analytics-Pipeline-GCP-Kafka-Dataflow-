import time
import requests
import socket
from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro

# --- Config ---
USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

KAFKA_CONF = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'C5HS5POGL2M5URBY',
    'sasl.password': 'cflt2cVULwS8cyE1Z//PszlKQcqbNA4pdl3AdhVzGQR6ZYitS3bEAmNVtM3sWg9Q',
    'client.id': socket.gethostname(),
    'schema.registry.url': 'https://psrc-zgxmd0p.us-east1.gcp.confluent.cloud',
    'basic.auth.credentials.source': 'USER_INFO',
    'schema.registry.basic.auth.user.info': '<SR_API_KEY>:<SR_API_SECRET>'
}

TOPIC = "earthquake-avro-stream"

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
producer = AvroProducer(KAFKA_CONF, default_value_schema=value_schema)
seen_ids = set()

def delivery_report(err, msg):
    if err is not None:
        print(f" Delivery failed: {err}")
    else:
        print(f" Message delivered to {msg.topic()} [{msg.partition()}]")

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

        producer.produce(topic=TOPIC, value=msg, callback=delivery_report)
        producer.poll(0)

    producer.flush()

if __name__ == "__main__":
    while True:
        try:
            print(" Fetching latest earthquake data...")
            fetch_and_send()
            print(" Batch sent. Waiting 60s...")
        except Exception as e:
            print(" Error:", e)
        time.sleep(60)
