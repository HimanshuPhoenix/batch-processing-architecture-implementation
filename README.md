# Microservice Architecture based Batch-Processing Architecture Implementation 

---

## 1. Executive Summary & System Overview

This repository contains the complete, reproducible source code for an end-to-end **Batch-Processing Data Architecture** designed to support scheduled quarterly feature engineering for a downstream Machine Learning (ML) model training application.

The ingestion service generates a reproducible synthetic dataset modeled on **NYC Yellow Taxi trip attributes**, containing **over 1,000,000 timestamped records** to enable deterministic, reproducible testing of large-scale batch processing. The system is fully containerized using **Docker** microservices, orchestrated via **Docker Compose**, and configured for zero-cloud local execution.

---

## 2. Architecture & Microservices Layout

<img width="2826" height="888" alt="Batch Processing pipeline" src="https://github.com/user-attachments/assets/579e8e18-f1fe-4e71-96fa-2bc78311d783" />

```
                  +----------------------------------------------+
                  |  Reproducible Synthetic Dataset (>=1M Rows)  |
                  |     - NYC Yellow Taxi Schema & Timestamps    |
                  +----------------------------------------------+
                                         |
                                         v
                  +----------------------------------------------+
                  |   Data Ingestion Microservice (Python ETL)   |
                  |     - Schema & Timestamp Validation Checks   |
                  |     - Exponential Backoff Retries            |
                  +----------------------------------------------+
                               /                   \
                              /                     \
                             v                       v
      +-------------------------------+   +-------------------------------+
      | MinIO Object Storage (S3 API) |   | PostgreSQL Relational DB      |
      |   - Snappy Parquet Files      |   |   - Audit Ingestion Logs      |
      |   - Hive Partitioned (Y/M)    |   |   - Relational Metadata       |
      +-------------------------------+   +-------------------------------+
                             \                       /
                              \                     /
                               v                   v
                  +----------------------------------------------+
                  | Apache Spark Engine (PySpark Native S3A)     |
                  |   - Direct Parallel Parquet S3A Reading     |
                  |   - Quarterly Feature Engineering Aggs       |
                  |   - Idempotent PostgreSQL UPSERT             |
                  +----------------------------------------------+
                                         |
                                         v
                  +----------------------------------------------+
                  | PostgreSQL Feature Store                     |
                  |   - Table: quarterly_feature_aggregations    |
                  |   - Indexes & Unique Constraints Preserved   |
                  +----------------------------------------------+
                                         |
                                         v
                  +----------------------------------------------+
                  | Feature Delivery REST API (FastAPI)          |
                  |   - Connection Pooling & Query Pagination    |
                  |   - GET /api/v1/features                     |
                  |   - Served to Downstream ML Application      |
                  +----------------------------------------------+
```

### Microservice Roles & Components:
1. **Ingestion Service (`de_ingestion`):** Generates 1,000,000+ time-referenced records, executes explicit schema integrity & timestamp validation checks, converts data into Hive-partitioned Snappy-compressed Parquet files (`yellow_taxi_2023/year=YYYY/month=MM/`), uploads them to local MinIO storage via S3 API with exponential backoff retries, and logs execution audit metadata to PostgreSQL.
2. **Object Store (`de_minio`):** Local, S3-compatible MinIO container housing raw partitioned Parquet datasets (`raw-data` bucket), pinned to a tested release version (`minio/minio:RELEASE.2023-09-20T22-40-07Z`).
3. **Database & Feature Store (`de_postgres`):** PostgreSQL container acting as both audit logger and final feature store (`quarterly_feature_aggregations`), utilizing primary keys, unique constraints, and indexes.
4. **Processing Engine (`de_spark_processor`):** Apache Spark (PySpark) container reading Parquet files directly from MinIO using native Hadoop S3A connector, executing quarterly feature engineering, group-by aggregations, and performing an idempotent UPSERT (`ON CONFLICT DO UPDATE`) to PostgreSQL.
5. **Feature Delivery API (`de_fastapi`):** FastAPI REST application with connection pooling, query pagination, and secure exception handling exposing low-latency HTTP endpoints (`/api/v1/features`) for downstream ML training applications.

---

## 3. Local Installation & Quick Start Guide

### Prerequisites
* **Docker Desktop** (v20.10+) & **Docker Compose** (v2.0+)
* Git & Python 3.10+ (optional, for local client testing)

### Execution Steps
```bash
# 1. Clone the repository
git clone https://github.com/HimanshuPhoenix/batch-processing-architecture-implementation.git
cd batch-processing-architecture-implementation

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

### `GET /api/v1/features?quarter=2023-Q1&vendor_id=1&limit=10&offset=0`
Retrieves quarterly aggregated feature vectors for ML model training with pagination support.
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

## 5. Architectural Improvements & Reliability Enhancements

1. **Explicit Data & Schema Validation:** `ingest.py` executes schema column presence checks, null checks, non-negative range checks, and datetime sanity checks (`pickup_datetime <= dropoff_datetime`) before formatting data for storage.
2. **Hive-Partitioned Parquet Storage:** Data is uploaded to MinIO using Hive partition keys (`yellow_taxi_2023/year=YYYY/month=MM/`), enabling Spark partition pruning and optimized parallel reads.
3. **Native PySpark S3A Processing:** Spark reads raw Parquet objects directly from MinIO using the Hadoop S3A connector without loading data into Python driver memory.
4. **Idempotent PostgreSQL UPSERT:** Aggregations write to PostgreSQL using `ON CONFLICT (vendor_id, year_quarter) DO UPDATE`, preserving SQL schema constraints, primary keys, and indexes.
5. **Fault Tolerance & Retries:** Ingestion microservice incorporates exponential backoff retries (`execute_with_retry`) for MinIO S3 streaming and PostgreSQL logging.
6. **API Performance & Security:** FastAPI uses `ThreadedConnectionPool` for database access, query pagination (`limit`/`offset`), and masks internal SQL exceptions from external clients.
7. **Container Security & Resource Governance:** Docker containers execute under dedicated non-root users (`USER appuser`), pin MinIO release tags, and specify explicit CPU and memory resource limits in `docker-compose.yml`.

---

## 6. Repository File Index

```
.
├── docker-compose.yml           # Microservice orchestration manifest with resource limits
├── .env.example                 # Sample environment configuration
├── .gitignore                   # Git exclusion rules
├── README.md                    # Technical documentation & architecture guide
├── sql/
│   └── schema.sql               # PostgreSQL DDL tables, constraints & indexes
└── services/
    ├── ingestion/               # Python ETL microservice (Validation, Hive Parquet, Retries)
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── ingest.py
    ├── spark_processor/         # PySpark batch aggregation microservice (Native S3A, UPSERT)
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── spark_process.py
    └── api/                     # FastAPI feature delivery microservice (Pooling, Pagination, Security)
        ├── Dockerfile
        ├── requirements.txt
        └── main.py
```

---

## 7. License & Academic Integrity
Developed as part of the studies at **IU International University of Applied Sciences**.
