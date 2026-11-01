"""
Batch Ingestion Microservice
Generates / ingests 1,000,000+ timestamped trip records, converts to Parquet,
uploads to local S3-compatible MinIO object store, and logs batch audit metadata to PostgreSQL.
"""

import os
import io
import time
import datetime
import numpy as np
import pandas as pd
import psycopg2
from minio import Minio

# Configuration & Environment Variables
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")
BUCKET_NAME = "raw-data"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "feature_store")
POSTGRES_USER = os.getenv("POSTGRES_USER", "db_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "db_password")

NUM_RECORDS = 1_000_000  # Satisfies >= 1,000,000 record constraint

def generate_timestamped_dataset(n_rows=NUM_RECORDS):
    """Generates a structured dataset containing >1,000,000 timestamped taxi trip records."""
    print(f"[INGESTION] Generating {n_rows:,} timestamped records...")
    
    np.random.seed(42)
    start_date = pd.Timestamp("2023-01-01")
    end_date = pd.Timestamp("2023-12-31")
    
    # Generate random timestamps across 2023
    random_seconds = np.random.randint(0, int((end_date - start_date).total_seconds()), size=n_rows)
    pickup_times = start_date + pd.to_timedelta(random_seconds, unit="s")
    durations_minutes = np.random.exponential(scale=15, size=n_rows) + 2
    dropoff_times = pickup_times + pd.to_timedelta(durations_minutes, unit="m")
    
    df = pd.DataFrame({
        "trip_id": np.arange(1, n_rows + 1),
        "vendor_id": np.random.choice([1, 2, 3], size=n_rows, p=[0.4, 0.45, 0.15]),
        "pickup_datetime": pickup_times,
        "dropoff_datetime": dropoff_times,
        "passenger_count": np.random.choice([1, 2, 3, 4, 5], size=n_rows, p=[0.7, 0.15, 0.08, 0.04, 0.03]),
        "trip_distance_miles": np.round(np.random.gamma(shape=2, scale=1.5, size=n_rows), 2),
        "fare_amount": np.round(np.random.gamma(shape=3, scale=5.0, size=n_rows) + 2.5, 2),
        "tip_amount": np.round(np.random.exponential(scale=2.5, size=n_rows), 2)
    })
    
    # Ensure year and month partition columns exist
    df["year"] = df["pickup_datetime"].dt.year
    df["month"] = df["pickup_datetime"].dt.month
    
    return df

def upload_to_minio(df, minio_client):
    """Converts DataFrame to snappy-compressed Parquet and streams to MinIO."""
    batch_id = f"batch_2023_{int(time.time())}"
    object_name = f"yellow_taxi_2023/{batch_id}.parquet"
    
    print(f"[MINIO] Converting {len(df):,} rows to Parquet format...")
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy", index=False)
    parquet_buffer.seek(0)
    parquet_size = parquet_buffer.getbuffer().nbytes
    
    print(f"[MINIO] Uploading '{object_name}' ({parquet_size / (1024*1024):.2f} MB) to MinIO bucket '{BUCKET_NAME}'...")
    minio_client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        data=parquet_buffer,
        length=parquet_size,
        content_type="application/octet-stream"
    )
    
    return batch_id, object_name

def log_to_postgres(batch_id, records_count, object_name):
    """Logs batch metadata to PostgreSQL audit table."""
    print("[POSTGRES] Logging ingestion audit metadata...")
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()
    
    insert_sql = """
        INSERT INTO ingestion_logs (batch_id, records_ingested, s3_path, status)
        VALUES (%s, %s, %s, %s);
    """
    cursor.execute(insert_sql, (batch_id, records_count, f"s3a://{BUCKET_NAME}/{object_name}", "SUCCESS"))
    conn.commit()
    cursor.close()
    conn.close()
    print("[POSTGRES] Successfully logged batch execution to PostgreSQL.")

def main():
    print("[START] Starting Data Ingestion Microservice...")
    
    # Initialize MinIO client
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    # Ensure bucket exists
    if not minio_client.bucket_exists(BUCKET_NAME):
        print(f"[MINIO] Creating bucket '{BUCKET_NAME}'...")
        minio_client.make_bucket(BUCKET_NAME)
    
    df = generate_timestamped_dataset(NUM_RECORDS)
    batch_id, object_name = upload_to_minio(df, minio_client)
    log_to_postgres(batch_id, len(df), object_name)
    
    print("[SUCCESS] Ingestion microservice completed successfully.")

if __name__ == "__main__":
    main()
