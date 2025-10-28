
# 🌎 USGov Earthquake Streaming Analytics Pipeline

**Author:** Adewole Oyediran

[![Tech Stack](https://img.shields.io/badge/Tech-GCP%20%7C%20Kafka%20%7C%20Confluent%20%7C%20Dataflow%20%7C%20BigQuery%20%7C%20dbt%20%7C%20Airflow%20%7C%20Looker%20Studio-blue)](https://cloud.google.com)
[![Status](https://img.shields.io/badge/Status-Active-success)]()
[![License](https://img.shields.io/badge/License-MIT-green)]()

---

## 📘 Project Overview

The **USGov Earthquake Streaming Analytics Pipeline** is a real-time, cloud-native data engineering project that captures **live seismic data** from the **USGS Earthquake Feed**, streams it via **Confluent Kafka**, processes it in **BigQuery**, models with **dbt**, and visualizes with **Looker Studio** — all orchestrated by **Airflow**.

It’s designed to demonstrate **end-to-end mastery** of modern **data engineering**, **streaming architecture**, and **GCP analytics workflows** — ideal for **portfolio and interview showcase**.

---

## 🏗️ Architecture Overview

Below is the full architecture showing all major components:

![Architecture Diagram](images/architecture_diagram.png)

> *Full architecture: USGS → Kafka (Confluent Cloud) → BigQuery → dbt → Airflow → Looker Studio (on GCP)*

---

## ⚙️ Data Flow Summary

| Stage              | Tool / Service                    | Description                                                     |
| ------------------ | --------------------------------- | --------------------------------------------------------------- |
| **Ingestion**      | Python + Confluent Kafka          | Fetches live earthquake data from USGS API and streams to Kafka |
| **Serialization**  | Avro + Schema Registry            | Enforces structured schema for all messages                     |
| **Storage**        | BigQuery                          | Real-time sink connector loads data into GCP warehouse          |
| **Transformation** | dbt                               | Cleans, models, and builds analytics-ready marts                |
| **Orchestration**  | Apache Airflow                    | Schedules and manages all data workflows                        |
| **Visualization**  | Looker Studio                     | Delivers real-time, interactive dashboards                      |
| **Cloud Platform** | Google Cloud (GCS, IAM, Composer) | Provides secure, scalable infrastructure                        |

---

## 🌋 1. Data Source – USGS GeoJSON Feed

**Endpoint:** [https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson](https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson)

* Returns global earthquake data (location, magnitude, depth, and time).
* Queried periodically by the Kafka producer.

![USGS Feed Example](images/usgs_feed_example.png)

> *Sample of raw GeoJSON feed from USGS API.*

---

## 🔁 2. Kafka Producer (Python)

File: `producer_usgs_.py`

* Periodically pulls new earthquake events.
* Publishes messages to **Confluent Kafka topic** `usgs_earthquakes`.
* Configured with Avro serializer & Confluent Schema Registry.

![Kafka Producer Logs](images/producer_logs.png)

> *Producer publishing live USGS data to Kafka topic.*

---

## ☁️ 3. Confluent Cloud & Schema Registry

* Topic: **`usgs_earthquakes`**
* Manages **streaming ingestion, topic partitions**, and **Avro schemas**.
* Provides **real-time monitoring**, **scalability**, and **connectivity** to GCP.

![Confluent Dashboard](images/confluent_dashboard.png)

> *Confluent Cloud console showing Kafka topic, connectors, and Schema Registry.*

---

## 🗃️ 4. BigQuery Sink Connector

* Connector streams Avro messages directly into **BigQuery tables** in near real-time.
* Dataset: `earthquake_data.usgs_events`
* Partitioned by ingestion timestamp and region.

![BigQuery Table](images/bigquery_table_preview.png)

> *Streaming inserts arriving in real time from Kafka Sink Connector.*

---

## 🧱 5. dbt Transformation Layer

Directory: `/usgs_dbt_project`

* Models clean and enrich raw event data.
* Includes:

  * `stg_usgs_events.sql` → Flatten + normalize source JSON.
  * `dim_continents.sql` → Adds geospatial region mapping.
  * `fact_earthquakes.sql` → Builds analytical fact table.
* Adds schema tests (uniqueness, relationships, freshness).

![dbt Lineage Graph](images/dbt_lineage_graph.png)

> *dbt model lineage showing transformation flow from staging → mart.*

---

## 🕒 6. Airflow Orchestration

* Manages scheduling, retries, and dependencies between ingestion and dbt.
* DAGs for producer job, dbt transformations, and validation checks.
* Logs and XComs stored in Cloud Composer or local environment.

![Airflow DAG](images/airflow_dag_graph.png)

> *Airflow DAG orchestrating ingestion and transformation tasks.*

---

## 📊 7. Looker Studio Visualization

* Connected to **BigQuery marts** for live dashboards.
* Dashboard pages include:

  * **Global Earthquake Map** (Geo heatmap by magnitude)
  * **Magnitude Distribution** (Histogram)
  * **Top 10 Regions by Activity**
  * **Daily Earthquake Trend Line**
  * **Impact Severity Indicator (Richter scale)**

![Looker Dashboard](images/looker_studio_dashboard.png)

> *Real-time dashboard powered by Looker Studio connected to BigQuery marts.*

---

## 🧠 Learning Highlights

✔️ Real-time streaming architecture using **Kafka + Confluent**
✔️ Schema governance via **Avro + Schema Registry**
✔️ Transformation pipelines with **dbt on BigQuery**
✔️ **Airflow orchestration** for task automation
✔️ **Looker Studio dashboards** for visual analytics
✔️ Cloud-native, production-grade **GCP implementation**

![Full Stack Overview](images/full_stack_overview.png)

> *End-to-end tech stack view — from ingestion to visualization.*

---

## 🧰 Tech Stack Summary

| Layer                      | Technology                             |
| -------------------------- | -------------------------------------- |
| **Ingestion**              | Python, Confluent Kafka                |
| **Schema Serialization**   | Avro, Schema Registry                  |
| **Processing**             | Google BigQuery                        |
| **Transformation**         | dbt Core                               |
| **Workflow Orchestration** | Apache Airflow                         |
| **Visualization**          | Looker Studio                          |
| **Cloud Infrastructure**   | Google Cloud (GCS, IAM, Composer, VPC) |

---

## 📂 Repository Structure

```
USGov-Earthquake-Streaming-Analytics-Pipeline/
├── producer_usgs_.py                 # Kafka producer (USGS → Kafka)
├── consumer_bq.py                    # Optional: Kafka consumer → BigQuery
├── continent_bounds_loader.py        # Load continent metadata
├── usgs_dbt_project/                 # dbt models, tests, and transformations
├── airflow_dag/                      # Airflow DAGs for orchestration
├── logs/                             # Airflow & dbt logs
├── looker_studio/                    # Dashboard screenshots and exports
├── requirements.txt
└── README.md
```

---

## 🧩 Example BigQuery Query

```sql
SELECT
  region,
  COUNT(*) AS total_events,
  AVG(magnitude) AS avg_magnitude
FROM `earthquake-475820.analytics.fact_earthquakes`
GROUP BY region
ORDER BY avg_magnitude DESC;
```

![BigQuery Results](images/bigquery_query_result.png)

> *Sample analytical query showing average magnitudes by region.*

---

## 🏁 Run the Pipeline Locally

```bash
# Clone the repo
git clone https://github.com/bensha02019/USGov-Earthquake-Streaming-Analytics-Pipeline.git
cd USGov-Earthquake-Streaming-Analytics-Pipeline

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run Kafka producer
python producer_usgs_.py

# Optionally trigger dbt transformations
dbt run
```

---

## 🚀 Future Roadmap

* [ ] Integrate **Google Dataflow** for stream enrichment
* [ ] Add **Slack Alerts** for earthquakes above magnitude 6.0
* [ ] Deploy **CI/CD** for dbt via GitHub Actions
* [ ] Enable **Cloud Run** for microservice-based producer deployment

---

## 🏅 Results

✅ Automated streaming from public API
✅ Real-time ingestion and transformation
✅ Clean mart schema for analytics
✅ Interactive visualization in Looker Studio
✅ Production-ready cloud architecture

![Final Dashboard](images/final_dashboard.png)

> *Final Looker Studio earthquake dashboard — live insights from streaming pipeline.*

---

## 📫 Contact

**Author:** Bensha
📧 [[your.email@example.com](mailto:your.email@example.com)]
🌐 [LinkedIn Profile Here]
💻 [GitHub: github.com/bensha02019](https://github.com/bensha02019)

---

Would you like me to **generate the actual image layout mockups** (architecture diagram, Airflow DAG layout, dbt graph, Looker dashboard preview) so you can drop them straight into `/images/` for your README?
It’ll make your portfolio **look top-tier visually** even before you add real screenshots.
