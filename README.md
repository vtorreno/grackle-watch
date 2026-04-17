# Grackle Watch: Multi-Source Medallion Data Pipeline

End-to-end data engineering pipeline for analyzing Great-tailed Grackle (*Quiscalus mexicanus*) sightings and behavioral experiments across the United States. Integrates three independent data sources through a Bronze → Silver → Gold architecture using industry-standard tools.

**DataTalks.Club Data Engineering Capstone 2026**
Developed by [Victoria Torreno](https://github.com/vtorreno)

---

## Table of Contents
- [Problem Description](#problem-description)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Data Sources](#data-sources)
- [Pipeline Overview](#pipeline-overview)
- [Data Warehouse](#data-warehouse)

---

## Dashboard

[View Live Dashboard](https://datastudio.google.com/s/spKRTuhL728)

<p align="center">
  <img src="images/learning_curves.png" width="45%">
  <img src="images/monthly_sightings.png" width="45%">
</p>

**Tile 1 (Categorical):** Subject Learning Performance by Experiment Type
Cumulative success rates per bird across the Aesop's Fable Task and Color Reversal Task.

**Tile 1 (Temporal):** Monthly Grackle Sightings by Source
GBIF provides full 2025 annual coverage. eBird reflects a rolling 30-day window due to API limitations.

---

## Problem Description

The Great-tailed Grackle (*Quiscalus mexicanus*) is one of North America's fastest-expanding bird species, yet its behavioral flexibility remains understudied at scale. This project builds an end-to-end data pipeline integrating three independent data sources to analyze both the geographic spread of grackle sightings and the cognitive performance of individual subjects in controlled behavioral experiments.

This pipeline answers:
- Where and when are Great-tailed Grackles most active across the United States?
- How do sighting patterns shift seasonally across GBIF and eBird sources?
- Do individual grackles show measurable learning curves across tool use and color discrimination tasks?
- Do grackles exhibit consistent spatial bias in approach direction during foraging trials?

---

## Architecture

┌─────────────────────────────────────────────────────────────────────┐
│                         Grackle Watch Pipeline                       │
└─────────────────────────────────────────────────────────────────────┘

  SOURCES              BRONZE              SILVER              GOLD
  ───────              ──────              ──────              ────
  GBIF API  ──┐
              │
  eBird API ──┼── dlt ──── GCS ──── Spark ──── GCS ──── dbt ──── BigQuery
              │
  KNB CSVs  ──┘

                    ┌─────────────────────────────────┐
                    │  Kestra (Orchestration)          │
                    │  Terraform (Infrastructure)      │
                    │  Docker (Containerization)       │
                    └─────────────────────────────────┘

                                                            │
                                                       Data Studio

---

## Tech Stack

| Layer               | Tool                      | Description                                                    |
| :-------------------| :-------------------------| :------------------------------------------------------------- |
| **Cloud**           | **Google Cloud Platform** | Primary cloud infrastructure and resource hosting.             |
| **Infrastructure**  | **Terraform**             | IaC for reproducible GCS buckets and BigQuery datasets.        |
| **Containerization**| **Docker**                | Reproducible environments for Kestra and Spark jobs.           |
| **Orchestration**   | **Kestra**                | Pipeline scheduling and workflow management via Docker Compose.|
| **Environment**     | **uv**                    | Python package management for high-performance ingestion.      |
| **Ingestion**       | **dlt (Data Load Tool)**  | Paginated API ingestion with automated schema inference.       |
| **Data Lake**       | **Google Cloud Storage**  | Scalable storage for Raw (Bronze) and Processed (Silver) data. |
| **Processing**      | **Apache Spark**          | Bronze to Silver transformations and schema standardization.   |
| **Transformation**  | **dbt**                   | Behavioral modeling and data quality testing (Silver/Gold).    |
| **Warehouse**       | **BigQuery**              | Serverless storage for final analytical models.                |
| **Visualization**   | **Data Studio**           | Interactive behavioral insights and performance dashboards.    |

---

## Data Sources

| Source     | Description                       | Volume                 |
|------------|-----------------------------------|------------------------|
| [GBIF API](https://www.gbif.org/) | Occurrence records for *Quiscalus mexicanus* | ~100k records (2025) |
| [eBird API 2.0](https://documenter.getpostman.com/view/664302/S1ENwy59) | Recent sightings across 5 US states | Rolling 30-day window |
| [KNB (Logan 2015)](https://knb.ecoinformatics.org/view/doi:10.5063/F13B5XBC) | Behavioral experiment data for 8 subjects | 1,899 rows across 3 files |
                                                                        
---

```markdown
## Prerequisites

- Google Cloud account with billing enabled
- Docker and Docker Compose
- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager
- Terraform
- eBird API key ([request here](https://ebird.org/api/keygen))

---

### Step 1. Clone the Repository

```bash
git clone https://github.com/vtorreno/grackle-watch.git
cd grackle-watch
```

### Step 2. Set Up Environment Variables

**Copy and configure** the environment file:

```bash
cp .env.example .env
```

```dotenv
GCS_BUCKET=your-bucket-name
GCP_PROJECT_ID=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
EBIRD_API_KEY=your-ebird-api-key
DESTINATION__FILESYSTEM__BUCKET_URL=gs://your-bucket-name
GCS_CONNECTOR_JAR=/path/to/jars/gcs-connector.jar
```

### Step 3. Download GCS Connector JAR

```bash
mkdir -p jars
curl -L https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.5/gcs-connector-hadoop3-2.2.5-shaded.jar \
     -o jars/gcs-connector.jar
```

### Step 4. Provision Infrastructure

**Create** a `terraform.tfvars` file in the `terraform/` directory:

```hcl
project                    = "your-gcp-project-id"
region                     = "us-central1"
location                   = "US"
gcs_bucket_name            = "your-bucket-name"
kestra_storage_bucket_name = "your-kestra-bucket-name"
bq_dataset_gold            = "grackle_watch_gold"
bq_dataset_external        = "grackle_watch_external"
```

**Initialize and apply** Terraform:

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

### Step 5. Install Dependencies

```bash
uv sync
```

### Step 6. Start Kestra

**Start** the orchestration server:

```bash
docker compose up -d
```

Open http://localhost:8080.

### Step 7. Run the Pipeline

**Option A — Via Kestra UI (recommended)**
1. Upload namespace files (ingestion scripts, Spark scripts, dbt files, KNB CSVs, GCS connector JAR)
2. Configure KV store with required secrets
3. Execute `grackle_watch_pipeline` flow

**Option B — Run locally**

```bash
# ingestion
uv run python ingestion/gbif/01_gbif_ingest.py --year 2025
uv run python ingestion/ebird/01_ebird_ingest.py --back 30
uv run python ingestion/knb/01_knb_ingest.py

# spark transforms
uv run python spark/gbif/01_gbif_transform.py
uv run python spark/ebird/01_ebird_transform.py
uv run python spark/knb/01_knb_transform.py

# dbt
cd grackle_watch
uv run dbt deps
uv run dbt run
uv run dbt test
```

---

## Data Modeling

Before running dbt, create external tables in BigQuery pointing to GCS Silver Parquet files.

<details>
<summary>Create BigQuery External Tables</summary>

```sql
-- GBIF (partitioned by year/month)
CREATE OR REPLACE EXTERNAL TABLE `<project>.grackle_watch_external.gbif`
WITH PARTITION COLUMNS
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://<bucket>/silver_gbif/*'],
  hive_partition_uri_prefix = 'gs://<bucket>/silver_gbif/'
);

-- eBird (partitioned by year/month)
CREATE OR REPLACE EXTERNAL TABLE `<project>.grackle_watch_external.ebird`
WITH PARTITION COLUMNS
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://<bucket>/silver_ebird/*'],
  hive_partition_uri_prefix = 'gs://<bucket>/silver_ebird/'
);

-- KNB (not partitioned)
CREATE OR REPLACE EXTERNAL TABLE `<project>.grackle_watch_external.knb_water_tube`
OPTIONS (format = 'PARQUET', uris = ['gs://<bucket>/silver_knb/water_tube/*']);

CREATE OR REPLACE EXTERNAL TABLE `<project>.grackle_watch_external.knb_color_test`
OPTIONS (format = 'PARQUET', uris = ['gs://<bucket>/silver_knb/color_test/*']);

CREATE OR REPLACE EXTERNAL TABLE `<project>.grackle_watch_external.knb_interaction`
OPTIONS (format = 'PARQUET', uris = ['gs://<bucket>/silver_knb/interaction/*']);
```

</details>
```

---

## Pipeline Overview

This project uses a **batch pipeline** with the following schedule:

| Source | Schedule |
|---|---|
| GBIF | Annually on January 1st |
| eBird | Daily at midnight |
| KNB | One-time upload |
| Spark + dbt | After ingestion completes |

Data flows through three layers:
1. Bronze (raw)    GCS
2. Silver (clean)  GCS
3. Gold (model)    BigQuery

---

## Data Warehouse

Silver layer data in GCS is partitioned by `year` and `month` using Hive-style partitioning for efficient downstream querying. Gold layer models are materialized as tables in BigQuery.

| Model | Description |
|---|---|
| `fct_observations` | Unified GBIF and eBird sightings fact table |
| `mart_monthly_sighting_trends` | Monthly sighting counts and flock size metrics |
| `mart_subject_learning_curves` | Per-trial success rate and cumulative learning curve per subject |
| `mart_spatial_preference` | Approach direction frequency and spatial bias per subject |

---

## Project Structure

```
.
├── terraform/           # IaC: GCS buckets and BigQuery datasets
├── ingestion/           # Bronze: dlt ingestion scripts for GBIF, eBird, and KNB
├── spark/               # Silver: PySpark cleaning and schema standardization
├── grackle_watch/       # Gold: dbt models for BigQuery analytics
├── kestra/              # Orchestration: end-to-end pipeline flows
├── notebooks/           # Exploration and transformation prototypes
├── Dockerfile           # Spark runtime image
├── Dockerfile.ingest    # Ingestion runtime image
├── docker-compose.yml   # Local Kestra and Postgres stack
└── pyproject.toml       # uv-managed Python dependencies
```

---

[MIT License](LICENSE) · Developed by Victoria Torreno · DataTalks.Club 2026