# Microservice Architecture based Batch-Processing Architecture Implementation 

## Scenario & System Objective:

Design a reliable, scalable, and maintainable batch-processing backend infrastructure that ingests, stores, preprocesses, and delivers aggregated features for a downstream machine learning (ML) model training application running on a quarterly schedule.

Visual Architecture Flowchart:

<img width="2826" height="888" alt="Batch Processing pipeline" src="https://github.com/user-attachments/assets/579e8e18-f1fe-4e71-96fa-2bc78311d783" />


## Microservice Architecture & Data Flow:

### Data Ingestion: 

A Python/Airflow batch fetcher container executes monthly ingestion from a Kaggle open dataset containing $\ge 1,000,000$ timestamped records (e.g., NYC Taxi Trip Records), validating schemas and data types upon landing.

### Data Storage: 

A decoupled dual-storage layer utilizes a MinIO object store (S3 API) for immutable, raw Parquet files (partitioned by year/month) and a PostgreSQL database for relational metadata, job logs, and audit trails.

### Pre-Processing & Aggregation: 

An Apache Spark (PySpark) container runs scheduled quarterly batch jobs to execute data cleaning, feature engineering, and quarterly aggregations.

### Data Delivery: 

A FastAPI REST service provides low-latency endpoints serving aggregated feature tables to the downstream ML training application.

### Operational Qualities, Security & Governance:

The system is containerized via Docker, orchestrated locally with Docker Compose, and version-controlled on GitHub. Reliability and maintainability are enforced through container health checks, modular service isolation, and automated startup scripts. Security and governance are implemented via internal Docker network isolation, non-root container users, environment variable secret management, and immutable raw storage.

## Architecture Scaling &amp; Performance Optimization
