# Microservice Architecture based Batch-Processing Architecture Implementation 

---

## 1. Executive Summary & System Overview

This repository contains the complete, reproducible source code for an end-to-end **Batch-Processing Data Architecture** designed to support scheduled quarterly feature engineering for a downstream Machine Learning (ML) model training application.

The pipeline ingests, validates, stores, transforms, and delivers features derived from an open dataset containing **over 1,000,000 time-referenced records** (*NYC Yellow Taxi Trip Dataset*). The system is fully containerized using **Docker** microservices, orchestrated via **Docker Compose**, and configured for zero-cloud local execution.

---

## 2. Architecture & Microservices Layout

<img width="2826" height="888" alt="Batch Processing pipeline" src="https://github.com/user-attachments/assets/579e8e18-f1fe-4e71-96fa-2bc78311d783" />

```
                  +----------------------------------------------+
                  |  Kaggle Timestamped Dataset (>=1M Records)   |
                  +----------------------------------------------+
                                         |
                                         v
                  +----------------------------------------------+
                  |   Data Ingestion Microservice (Python ETL)   |
                  |     - Schema Validation & Timestamp Check    |
                  +----------------------------------------------+
                               /                   \
                              /                     \
                             v                       v
      +-------------------------------+   +-------------------------------+
      | MinIO Object Storage (S3 API) |   | PostgreSQL Relational DB      |
      |   - Raw Snappy Parquet Files  |   |   - Audit Ingestion Logs      |
      |   - Partitioned by Year/Month |   |   - Relational Metadata       |
      +-------------------------------+   +-------------------------------+
                             \                       /
                              \                     /
                               v                   v
                  +----------------------------------------------+
                  | Apache Spark Engine (PySpark Processing)     |
                  |   - Batch Transformations & Cleaning        |
                  |   - Quarterly Feature Aggregations           |
                  +----------------------------------------------+
                                         |
                                         v
                  +----------------------------------------------+
                  | PostgreSQL Feature Store                     |
                  |   - Table: quarterly_feature_aggregations    |
                  +----------------------------------------------+
                                         |
                                         v
                  +----------------------------------------------+
                  | Feature Delivery REST API (FastAPI)          |
                  |   - GET /api/v1/features                     |
                  |   - Served to Downstream ML Application      |
                  +----------------------------------------------+
```

### Microservice Roles & Components:
1. **Ingestion Service (`de_ingestion`):** Fetches/generates 1,000,000+ time-referenced records, validates schema integrity, converts raw data into Snappy-compressed Parquet files, uploads them to local MinIO storage via the S3 API, and logs execution metadata to PostgreSQL.
2. **Object Store (`de_minio`):** Local, S3-compatible MinIO container housing raw partitioned Parquet datasets (`raw-data` bucket).
3. **Database & Feature Store (`de_postgres`):** PostgreSQL container acting as both the audit logger and the final feature store for aggregated metrics (`quarterly_feature_aggregations`).
4. **Processing Engine (`de_spark_processor`):** Apache Spark (PySpark) container executing quarterly feature engineering, group-by aggregations (average distance, fare, tip, revenue), and automated loading to PostgreSQL.
5. **Feature Delivery API (`de_fastapi`):** FastAPI REST application exposing low-latency HTTP endpoints (`/api/v1/features`) for downstream ML training applications.

---

## 3. Local Installation & Quick Start Guide

### Prerequisites
* **Docker Desktop** (v20.10+) & **Docker Compose** (v2.0+)
* Git & Python 3.10+ (optional, for local client testing)

### Execution Steps
```bash
# 1. Clone the repository
git clone <your-repository-url>
cd dlmdsede02-batch-pipeline

# 2. Configure environment variables
cp .env.example .env

# 3. Build and launch all containerized microservices
docker-compose up --build -d

# 4. Verify microservice container status
docker-compose ps
```

### Accessing Microservice Interfaces
* **FastAPI Interactive Swagger Docs:** `http://localhost:8000/docs`
* **MinIO Storage Console:** `http://localhost:9001` *(User: `minio_admin` | Pass: `minio_password`)*
* **PostgreSQL Database:** `localhost:5432` *(User: `db_user` | Pass: `db_password` | DB: `feature_store`)*

---

## 4. API Documentation & Sample Usage

### `GET /health`
Returns the status of the API and database connectivity.
```json
{
  "status": "HEALTHY",
  "database": "CONNECTED"
}
```

### `GET /api/v1/features?quarter=2023-Q1&vendor_id=1`
Retrieves quarterly aggregated feature vectors for ML model training.
```json
[
  {
    "vendor_id": 1,
    "year_quarter": "2023-Q1",
    "total_trips": 100234,
    "avg_trip_distance_miles": 3.42,
    "avg_fare_amount": 17.85,
    "avg_tip_amount": 2.50,
    "total_revenue": 2039702.30
  }
]
```

---

## 5. Architecture Scaling & Performance Optimization

*(Addressing Professor Max Pumperla's feedback on handling volume growth and system scalability)*

To ensure this batch-processing architecture transitions seamlessly from local execution to production-grade data volumes (100M+ records), the following scaling strategies are designed into the pipeline:

### 1. Storage Layer Scaling (MinIO Object Store)
* **Parquet Columnar Format & Compression:** Raw CSV inputs are immediately converted into Snappy-compressed Parquet files, reducing disk footprint by ~75% and accelerating columnar reads during Spark queries.
* **Partition Pruning:** Parquet object keys are partitioned by year and month (`/raw-data/year=YYYY/month=MM/`), enabling Spark to prune irrelevant partitions during query execution rather than scanning full datasets.

### 2. Processing & Compute Scaling (Apache Spark)
* **Horizontal Cluster Scale-Out:** The PySpark engine can scale from local standalone mode (`local[*]`) to a multi-node Spark executor cluster managed by Kubernetes or YARN, eliminating single-node driver OOM bottlenecks.
* **Shuffle Partition Tuning:** `spark.sql.shuffle.partitions` and executor memory limits are dynamically set to balance cluster parallelism during heavy quarterly group-by operations.

### 3. Database & Feature Store Scaling (PostgreSQL)
* **Declarative Range Partitioning:** The `quarterly_feature_aggregations` table is indexed on `(vendor_id, year_quarter)` and can be partitioned by range (`year`) in PostgreSQL to maintain sub-millisecond query performance as table size grows.
* **In-Memory Caching Tier (Redis):** A Redis caching layer can be introduced between PostgreSQL and FastAPI to cache frequent quarterly feature requests, protecting the relational database under high concurrent load.

### 4. API & Orchestration Scaling (FastAPI & Docker)
* **Asynchronous Workers & Load Balancing:** FastAPI instances run with multiple Uvicorn worker threads behind an NGINX reverse proxy, while Docker Compose defines explicit CPU and memory reservations (`deploy.resources`) to prevent resource contention.

---

## 6. Repository File Index

```
.
├── docker-compose.yml           # Microservice orchestration manifest
├── .env.example                 # Sample environment configuration
├── .gitignore                   # Git exclusion rules
├── README.md                    # Technical documentation & scaling guide
├── sql/
│   └── schema.sql               # PostgreSQL DDL tables & indexes
└── services/
    ├── ingestion/               # Python ETL microservice (1M+ rows -> Parquet -> MinIO)
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── ingest.py
    ├── spark_processor/         # PySpark batch aggregation microservice
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── spark_process.py
    └── api/                     # FastAPI feature delivery REST microservice
        ├── Dockerfile
        ├── requirements.txt
        └── main.py
```

---

## 7. License & Academic Integrity
Developed as part of the studies at **IU International University of Applied Sciences**.
