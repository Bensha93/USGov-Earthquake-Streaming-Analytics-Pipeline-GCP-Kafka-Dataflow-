import time
import socket
import requests
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# --- Config ---
USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
TOPIC = "earthquake-avro-stream-v2"

# Schema definition
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
    {"name": "depth_km", "type": ["null", "double"], "default": null},
    {"name": "felt", "type": ["null", "int"], "default": null},
    {"name": "cdi", "type": ["null", "float"], "default": null},
    {"name": "mmi", "type": ["null", "float"], "default": null},
    {"name": "alert", "type": ["null", "string"], "default": null},
    {"name": "tsunami", "type": ["null", "int"], "default": null},
    {"name": "type", "type": ["null", "string"], "default": null}
  ]
}
"""

# ✅ Schema Registry config
schema_registry_conf = {
    'url': 'https://psrc-zgxmd0p.us-east1.gcp.confluent.cloud',
    'basic.auth.user.info': 'A25RAHQW374TDEX4:cfltwQUucJypC0wKQbBN0seokmoDr6ZmB1Hw3A8HZalW+m9gdyk+BGs4xfTJC92w'
}


schema_registry_client = SchemaRegistryClient(schema_registry_conf)
value_serializer = AvroSerializer(schema_registry_client, value_schema_str)

# ✅ Kafka producer config
producer_conf = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'C5HS5POGL2M5URBY',
    'sasl.password': 'cflt2cVULwS8cyE1Z//PszlKQcqbNA4pdl3AdhVzGQR6ZYitS3bEAmNVtM3sWg9Q',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': value_serializer,
    'client.id': socket.gethostname(),
}

producer = SerializingProducer(producer_conf)

seen_ids = set()

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

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
            "depth_km": coords[2],
            "felt": props.get("felt"),
            "cdi": props.get("cdi"),
            "mmi": props.get("mmi"),
            "alert": props.get("alert"),
            "tsunami": props.get("tsunami"),
            "type": props.get("type")
        }

        producer.produce(topic=TOPIC, key=eid, value=msg, on_delivery=delivery_report)
        producer.poll(0)

    producer.flush()


if __name__ == "__main__":
    print("🌍 Fetching and sending one batch...")
    fetch_and_send()
    print("✅ Completed one batch successfully!")
