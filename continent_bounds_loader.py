import requests
import pandas as pd
from google.cloud import bigquery

#  SPARQL endpoint and query 
sparql_url = "https://query.wikidata.org/sparql"
query = """
SELECT 
  ?continentLabel
  (MIN(?lat) AS ?minLat)
  (MAX(?lat) AS ?maxLat)
  (MIN(?lon) AS ?minLon)
  (MAX(?lon) AS ?maxLon)
WHERE {
  ?country wdt:P31 wd:Q6256;      # is a country
           wdt:P30 ?continent;    # located in continent
           wdt:P625 ?coord.       # has coordinate location
  BIND(geof:latitude(?coord) AS ?lat)
  BIND(geof:longitude(?coord) AS ?lon)
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
GROUP BY ?continentLabel
ORDER BY ?continentLabel
"""

# Fetch data from Wikidata 
headers = {"Accept": "application/sparql-results+json"}
response = requests.get(sparql_url, params={"query": query}, headers=headers)
response.raise_for_status()

# Parse JSON → DataFrame
results = response.json()["results"]["bindings"]
data = [
    {
        "continentLabel": r["continentLabel"]["value"],
        "minLat": float(r["minLat"]["value"]),
        "maxLat": float(r["maxLat"]["value"]),
        "minLon": float(r["minLon"]["value"]),
        "maxLon": float(r["maxLon"]["value"]),
    }
    for r in results
]
df = pd.DataFrame(data)

# Load into BigQuery
client = bigquery.Client()
table_id = "earthquake-475820.earthquake_data.continent_bounds"

job = client.load_table_from_dataframe(
    df,
    table_id,
    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
)
job.result()

print(f" Loaded {len(df)} rows into {table_id}")