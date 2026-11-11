### Dual-Layer Lambda Architecture: Containerized Batch & Real-Time Streaming Data Pipeline

---

#### 1. Executive Summary & System Overview
This repository contains the complete, reproducible source code for an end-to-end **Lambda Architecture** data pipeline designed to support both scheduled quarterly feature engineering for machine learning (ML) model training and low-latency feature delivery for real-time inference.

The architecture combines a **robust batch layer** processing over **1,000,000 timestamped records** (modeled on NYC Yellow Taxi trip attributes) with a **real-time streaming speed layer** ingesting continuous trip events via **Apache Kafka** and **Spark Structured Streaming**. The system is completely containerized using **Docker microservices**, orchestrated via **Docker Compose**, and engineered for zero-cloud, reproducible local execution with production-grade resource limits and non-root security.

---

#### 2. Architecture & Microservices Layout
![Batch Processing Pipeline](Batch_Processing_pipeline.png)

```
========================================================================================================================
                                             DUAL-LAYER LAMBDA ARCHITECTURE
========================================================================================================================

  [ BATCH LAYER ]
  +----------------------------------------------+
  |  Kaggle Dataset / Synthetic (>=1M Rows)      |
  |    - NYC Yellow Taxi Schema & Timestamps     |
  +----------------------------------------------+
                         |
                         v
  +----------------------------------------------+
  |  Data Ingestion Microservice (Python ETL)    |
  |    - Defensive Schema & Temporal Checks      |
  |    - Bounded Exponential Backoff Retries     |
  +----------------------------------------------+
                /                   \
               / (Parquet Objects)   \ (Audit Logs)
              v                       v
  +-------------------------------+   |
  | MinIO Object Storage (S3 API) |   |
  |   - Hive Partitioned (Y/M)    |   |
  |   - Snappy Columnar Parquet   |   |
  +-------------------------------+   |
              |                       |
              v (Direct S3A Read)     |
  +-------------------------------+   |
  | Multi-Node Spark Cluster      |   |
  |   - spark-master (Port: 7077) |   |
  |   - spark-worker (Distributed)|   |
  |   - de_spark_processor (Batch)|   |
  |   - Quarterly Aggregations    |   |
  +-------------------------------+   |
              |                       |
              | (Idempotent UPSERT)   |
              v                       v
  +-------------------------------------------------------------+
  | PostgreSQL Dual Feature Store (de_postgres)                 |
  |   - Table: ingestion_logs (Audit trail)                     |
  |   - Table: quarterly_feature_aggregations (Batch Features)  |
  |   - Table: realtime_feature_aggregations (Windowed Features)|
  |   - Unique Constraints & B-Tree Composite Indexes           |
  +-------------------------------------------------------------+
              ^                                       |
              | (Windowed JDBC Sink)                  v
  +-------------------------------+   +----------------------------------------------+
  | Spark Structured Streaming    |   | Feature Delivery REST API (FastAPI)          |
  |   - 10-Min Watermarking       |   |   - ThreadedConnectionPool (1-10 conns)      |
  |   - 15-Min Sliding Windows    |   |   - GET /api/v1/features (Batch Vectors)     |
  |   - Micro-batch (10s trigger) |   |   - GET /api/v1/features/realtime (Stream)   |
  +-------------------------------+   |   - Query Pagination & Security Masking      |
              ^                       +----------------------------------------------+
              | (Subscribe Topic)                            |
  +-------------------------------+                          v
  | Apache Kafka Broker (KRaft)   |   +----------------------------------------------+
  |   - Topic: taxi-trips-stream  |   | Downstream Machine Learning Applications     |
  |   - ZooKeeper-less Streaming  |   |   - Quarterly Offline Model Retraining       |
  +-------------------------------+   |   - Real-Time Feature Drift Monitoring       |
              ^                       |   - Low-Latency Online Prediction Serving    |
              | (Publish Events)      +----------------------------------------------+
  +-------------------------------+
  | Stream Event Producer         |
  |   - stream_producer.py        |
  |   - Real-time Trip Simulator  |
  +-------------------------------+
  [ SPEED LAYER ]

========================================================================================================================
```

##### Microservice Roles & Components:
1. **Data Ingestion Service (`de_ingestion`):** Generates and validates 1,000,000+ timestamped records, converts them into Hive-partitioned Snappy Parquet files (`yellow_taxi_2023/year=YYYY/month=MM/`), streams them into MinIO with exponential backoff retries, and logs execution audit metadata to PostgreSQL.
2. **Stream Event Producer (`stream_producer.py`):** Simulates high-velocity, real-time taxi trip events and publishes JSON payloads to the Kafka broker with rate throttling and automatic connection recovery.
3. **Object Store (`de_minio`):** S3-compatible MinIO data lake housing partitioned Parquet datasets (`raw-data` bucket), pinned to a verified release (`RELEASE.2023-09-20T22-40-07Z`).
4. **Apache Kafka Broker (`de_kafka`):** Distributed message broker operating in ZooKeeper-less KRaft mode, hosting the `taxi-trips-stream` topic with internal JVM memory bounds (`-Xmx512m`).
5. **Spark Cluster Master (`de_spark_master`):** Central cluster coordinator managing distributed compute scheduling via RPC (`spark://spark-master:7077`) and exposing the cluster Web UI on port `8085`.
6. **Spark Cluster Worker (`de_spark_worker_1`):** Distributed executor container registered with the master, allocating 2 CPU cores and 1.5 GB executor memory for parallel computation.
7. **Batch Processing Engine (`de_spark_processor`):** PySpark batch client reading Parquet partitions directly from MinIO using native Hadoop S3A connectors, computing quarterly feature metrics, and executing idempotent SQL UPSERTs into PostgreSQL.
8. **Spark Streaming Speed Layer (`de_spark_streaming`):** PySpark Structured Streaming application consuming Kafka events, enforcing 10-minute watermarks, calculating 15-minute sliding windows (sliding every 5 minutes), and flushing micro-batches to PostgreSQL.
9. **Relational Database & Feature Store (`de_postgres`):** PostgreSQL 15 database storing audit trails, batch feature vectors, and real-time streaming aggregations with primary keys, unique constraints, and B-tree indexes.
10. **Feature Delivery REST API (`de_fastapi`):** FastAPI application with connection pooling, query pagination, and exception masking, delivering both batch features and real-time streaming metrics to downstream ML models.

--------------------------------------------------------------------------------

#### 3. Local Installation & Quick Start Guide
##### Prerequisites
* **Docker Desktop** (v20.10+) & **Docker Compose** (v2.0+)
* Host Resources: Minimum 8 GB RAM and 4 CPU cores recommended.

##### Execution Steps
```bash
# 1. Clone the repository
git clone https://github.com/HimanshuPhoenix/batch-processing-architecture-implementation.git
cd batch-processing-architecture-implementation

# 2. Configure environment variables from template
cp .env.example .env

# 3. Build and launch all containerized microservices in background mode
docker-compose up --build -d

# 4. Verify microservice container health and execution status
docker-compose ps
```

##### Starting the Real-Time Event Stream (Optional)
To continuously feed real-time trip events into the Kafka speed layer:
```bash
# Run stream producer inside the running ingestion container
docker-compose exec -d ingestion python stream_producer.py

# Check streaming worker logs
docker-compose logs -f spark-streaming
```

##### Accessing Microservice Interfaces
* **FastAPI Interactive Swagger Docs:** http://localhost:8000/docs
* **Spark Master Web UI:** http://localhost:8085 *(Cluster status, running workers, and active drivers)*
* **MinIO Storage Console:** http://localhost:9001 *(Credentials defined in .env)*
* **PostgreSQL Database:** localhost:5432 *(Database: feature_store)*
* **Apache Kafka Broker:** localhost:9092

--------------------------------------------------------------------------------

#### 4. API Documentation & Sample Usage
##### GET /health
Verifies service uptime and database connection pooling health.
```json
{
  "status": "HEALTHY",
  "database": "CONNECTED"
}
```

##### GET /api/v1/features?quarter=2023-Q1&vendor_id=1&limit=10&offset=0
Retrieves historical quarterly feature vectors computed by the distributed batch layer.
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

##### GET /api/v1/features/realtime?vendor_id=1&limit=5
Retrieves real-time 15-minute sliding window aggregations generated by the Spark Structured Streaming speed layer.
```json
[
  {
    "vendor_id": 1,
    "window_start": "2026-11-11T12:00:00",
    "window_end": "2026-11-11T12:15:00",
    "live_trips": 48,
    "live_avg_fare": 19.35,
    "live_avg_distance": 4.12
  }
]
```

##### GET /api/v1/ingestion-logs?limit=5
Returns metadata audit records from batch ingestion executions.
```json
[
  {
    "log_id": 1,
    "batch_id": "batch_20261111_120000",
    "records_ingested": 1000000,
    "s3_path": "s3://raw-data/yellow_taxi_2023/",
    "status": "SUCCESS",
    "ingested_at": "2026-11-11T12:05:22.123456+00:00"
  }
]
```

--------------------------------------------------------------------------------

#### 5. Architectural Improvements & Reliability Enhancements
1. **Defensive Schema & Data Validation:** `ingest.py` performs explicit column existence verification, null checks, positive boundary validation, and datetime sanity checks (`pickup_datetime <= dropoff_datetime`) before converting data into Parquet.
2. **Hive-Partitioned Parquet Storage:** Data in MinIO follows the Hive standard (`yellow_taxi_2023/year=YYYY/month=MM/`), allowing PySpark to leverage partition pruning and eliminate scanning unneeded files.
3. **Decoupled Multi-Node Spark Cluster:** Replaces single-node `local[*]` execution with a dedicated `spark-master` and scalable `spark-worker` architecture. Both batch processing and streaming workloads schedule tasks across executor nodes, resolving driver-side memory bottlenecks.
4. **Real-Time Speed Layer (Lambda Architecture):** Kafka and Spark Structured Streaming calculate sliding window metrics with 10-minute watermarking to handle late-arriving events, persisting near-real-time features alongside historical batch metrics.
5. **Idempotent PostgreSQL UPSERT:** Batch feature writes utilize `ON CONFLICT (vendor_id, year_quarter) DO UPDATE`, preventing primary key collisions, duplicate rows, and schema degradation across repeated batch runs.
6. **Threaded Connection Pooling & Query Pagination:** FastAPI uses `ThreadedConnectionPool` (1–10 pooled connections) to handle concurrent ML query traffic efficiently while supporting `limit` and `offset` pagination.
7. **Container Security & Resource Governance:** All containers run under dedicated non-root users (`USER appuser`), isolate environment secrets via `.env`, and enforce strict CPU and memory bounds (`deploy.resources.limits`) to ensure host stability.

--------------------------------------------------------------------------------

#### 6. Repository File Index
```
.
├── docker-compose.yml                     # Multi-container orchestration (9 services with resource limits)
├── .env.example                           # Configuration template for credentials and cluster endpoints
├── .gitignore                             # Git exclusion patterns
├── README.md                              # Comprehensive architecture guide & documentation
├── Batch_Processing_pipeline.png          # Visual Lambda Architecture blueprint
├── sql/
│   └── schema.sql                         # PostgreSQL schema (Audit, Batch features, Streaming features)
└── services/
    ├── ingestion/                         # Batch & streaming ingestion microservice
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   ├── ingest.py                      # 1M+ record generator, validator & Hive Parquet writer
    │   └── stream_producer.py             # Real-time synthetic event producer to Kafka
    ├── spark_processor/                   # Distributed PySpark batch aggregation microservice
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── spark_process.py               # Native S3A direct reader & idempotent SQL UPSERT
    ├── spark_streaminglayer/              # PySpark Structured Streaming speed layer microservice
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── stream_process.py              # Kafka consumer, watermarking & windowed micro-batch sink
    └── api/                               # FastAPI feature delivery microservice
        ├── Dockerfile
        ├── requirements.txt
        └── main.py                        # REST endpoints for batch and real-time features
```

--------------------------------------------------------------------------------

#### 7. License & Academic Integrity
Developed as part of the studies at **IU International University of Applied Sciences**.
