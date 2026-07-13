# NYC Yellow Taxi Data Pipeline

## Project Overview

This project implements an end-to-end batch data pipeline for the NYC Yellow Taxi dataset using the Medallion Architecture (Landing → Bronze → Silver → Gold).

The pipeline includes:

- Data ingestion using Apache Spark
- Data quality validation
- Bronze, Silver and Gold transformations
- Workflow orchestration using Apache Airflow
- Dockerized deployment

---

## Technology Stack

- Apache Spark 3.5.6
- Apache Airflow 2.10
- Python 3.11
- Docker & Docker Compose
- PostgreSQL
- Parquet

---

## Project Structure

```
project/
├── dags/                  # Airflow DAGs
├── jobs/                  # Spark jobs
│   ├── ingestion.py
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
├── storage/
│   ├── landing/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docker-compose.yml
├── Dockerfile
└── README.md
```

---

# Prerequisites

- Docker Desktop
- Docker Compose
- At least 8 GB RAM
- NYC Yellow Taxi parquet files placed under:

```
storage/landing/trip/
```

---

# Run the Project

### Step 1. Clone the repository

```bash
git clone <repository-url>
cd <repository-name>
```

---

### Step 2. Start all services

```bash
docker compose up --build -d
```

This command starts:

- PostgreSQL
- Spark Master
- Spark Worker
- Airflow Scheduler
- Airflow Webserver

---

### Step 3. Open Airflow

Open your browser:

```
http://localhost:8080
```

Login:

```
Username: airflow
Password: airflow
```

---

### Step 4. Trigger the pipeline

Run the DAG:

```
yellow_tripdata_pipeline
```

The DAG executes:

```
Landing
    ↓
Bronze
    ↓
Silver
    ↓
Gold
```

---

### Step 5. Verify outputs

Processed data will be generated under:

```
storage/

├── bronze/
├── silver/
└── gold/
```

---

## Expected Outputs

- Bronze dataset
- Silver cleaned dataset
- Gold analytical dataset
- Quarantine records for:
  - Schema violations
  - Duplicate records

---

## Notes

- The pipeline automatically processes all parquet files found in `storage/landing/trip/`.
- Bronze data is partitioned by `year/month`.
- Invalid records are stored separately in the quarantine directory.
- Spark jobs are executed through Airflow using a standalone Spark cluster.