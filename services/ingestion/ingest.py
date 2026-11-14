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
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

def execute_with_retry(action_fn, action_name):
    """Utility function to execute network operations with exponential backoff retries."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return action_fn()
        except Exception as e:
            print(f"[RETRY] {action_name} failed (Attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt == MAX_RETRIES:
                raise e
            time.sleep(RETRY_DELAY * attempt)

def generate_timestamped_dataset(n_rows=NUM_RECORDS):
    """Generates a structured dataset containing >1,000,000 timestamped taxi trip records."""
    print(f"[INGESTION] Generating {n_rows:,} timestamped records...")
    
    np.random.seed(42)
    start_date = pd.Timestamp("2023-01-01")
    end_date = pd.Timestamp("2023-12-31")
    
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
    
    df["year"] = df["pickup_datetime"].dt.year
    df["month"] = df["pickup_datetime"].dt.month
    
    return df

def validate_schema_and_timestamps(df):
    """
    Executes explicit quality checks on incoming dataset:
    1. Mandatory column structure verification
    2. Null value checks
    3. Timestamp ordering sanity check (pickup <= dropoff)
    4. Non-negative numeric bounds check
    """
    print("[VALIDATION] Running schema integrity & timestamp validation checks...")
    
    required_columns = {
        "trip_id", "vendor_id", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance_miles", "fare_amount", "tip_amount",
        "year", "month"
    }
    missing_cols = required_columns - set(df.columns)
    if missing_cols:
        raise ValueError(f"[VALIDATION FAILED] Missing mandatory columns: {missing_cols}")
        
    null_counts = df[list(required_columns)].isnull().sum().to_dict()
    total_nulls = sum(null_counts.values())
    if total_nulls > 0:
        raise ValueError(f"[VALIDATION FAILED] Detected null values in dataset: {null_counts}")
        
    invalid_timestamps = (df["pickup_datetime"] > df["dropoff_datetime"]).sum()
    if invalid_timestamps > 0:
        raise ValueError(f"[VALIDATION FAILED] Found {invalid_timestamps} records where pickup > dropoff!")
        
    if (df["trip_distance_miles"] < 0).any() or (df["fare_amount"] < 0).any() or (df["tip_amount"] < 0).any():
        raise ValueError("[VALIDATION FAILED] Negative values detected in distance or monetary amounts!")
        
    print(f"[VALIDATION PASSED] 100% of {len(df):,} records validated successfully.")

def upload_partitioned_to_minio(df, minio_client):
    """
    Converts DataFrame into Hive-partitioned Snappy Parquet files (year=YYYY/month=MM)
    and uploads to MinIO.
    """
    batch_id = f"batch_2023_{int(time.time())}"
    print(f"[MINIO] Converting {len(df):,} rows to Hive-partitioned Parquet format...")
    
    partition_paths = []
    
    # Group by year and month to generate hive-partitioned S3 storage paths
    grouped = df.groupby(["year", "month"])
    for (yr, mo), group_df in grouped:
        partition_prefix = f"year={yr}/month={mo:02d}"
        object_name = f"yellow_taxi_2023/{partition_prefix}/{batch_id}.parquet"
        
        parquet_buffer = io.BytesIO()
        group_df.to_parquet(
            parquet_buffer,
            engine="pyarrow",
            compression="snappy",
            index=False,
            coerce_timestamps="us",
            allow_truncated_timestamps=True
        )
        parquet_buffer.seek(0)
        parquet_size = parquet_buffer.getbuffer().nbytes
        
        def do_upload():
            minio_client.put_object(
                bucket_name=BUCKET_NAME,
                object_name=object_name,
                data=parquet_buffer,
                length=parquet_size,
                content_type="application/octet-stream"
            )
            
        execute_with_retry(do_upload, f"Upload to '{object_name}'")
        partition_paths.append(object_name)

    print(f"[MINIO] Uploaded batch across {len(partition_paths)} year/month partitions.")
    #return batch_id, f"yellow_taxi_2023/({len(partition_paths)} partitions)/{batch_id}.parquet"
    return batch_id, "yellow_taxi_2023/"

def log_to_postgres(batch_id, records_count, s3_path):
    """Logs batch execution metadata to PostgreSQL audit table with retries."""
    print("[POSTGRES] Logging ingestion audit metadata...")
    
    def do_log():
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
        cursor.execute(insert_sql, (batch_id, records_count, s3_path, "SUCCESS"))
        conn.commit()
        cursor.close()
        conn.close()

    execute_with_retry(do_log, "PostgreSQL Audit Logging")
    print("[POSTGRES] Successfully logged batch execution to PostgreSQL.")

def main():
    print("[START] Starting Data Ingestion Microservice...")
    
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    def check_make_bucket():
        if not minio_client.bucket_exists(BUCKET_NAME):
            print(f"[MINIO] Creating bucket '{BUCKET_NAME}'...")
            minio_client.make_bucket(BUCKET_NAME)

    execute_with_retry(check_make_bucket, "MinIO Bucket Initialization")
    
    df = generate_timestamped_dataset(NUM_RECORDS)
    validate_schema_and_timestamps(df)
    
    batch_id, s3_path = upload_partitioned_to_minio(df, minio_client)
    log_to_postgres(batch_id, len(df), f"s3a://{BUCKET_NAME}/{s3_path}")
    
    print("[SUCCESS] Ingestion microservice completed successfully.")

if __name__ == "__main__":
    main()
