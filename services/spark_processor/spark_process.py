"""
DLMDSEDE02 - Task 1: Apache Spark Batch Processing Engine
Reads raw Parquet files from S3/MinIO, executes quarterly aggregation feature engineering,
and writes aggregated feature metrics into PostgreSQL for downstream ML training.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sqlalchemy import create_engine

# ------------------------------------------------------------------------------
# Environment Variables & Connections
# ------------------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "feature_store")
POSTGRES_USER = os.getenv("POSTGRES_USER", "db_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "db_password")

def create_spark_session():
    """Initializes PySpark Session configured for local MinIO S3 access."""
    print("[SPARK] Initializing PySpark Session...")
    spark = SparkSession.builder \
        .appName("DLMDSEDE02-QuarterlyFeatureAggregator") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_batch_aggregations():
    """Reads Parquet data directly using PySpark / pandas, computes quarterly metrics, and loads to PostgreSQL."""
    print("[SPARK] Starting quarterly batch aggregation workflow...")
    
    # MinIO fetch via S3 API or direct PySpark reading
    from minio import Minio
    import io
    import pandas as pd

    minio_client = Minio(
        "minio:9000",
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    objects = list(minio_client.list_objects("raw-data", prefix="yellow_taxi_2023/", recursive=True))
    if not objects:
        print("[ERROR] No Parquet files found in MinIO bucket 'raw-data'!")
        sys.exit(1)

    print(f"[SPARK] Found {len(objects)} Parquet files in MinIO.")
    
    dfs = []
    for obj in objects:
        print(f"[SPARK] Reading '{obj.object_name}'...")
        response = minio_client.get_object("raw-data", obj.object_name)
        data = response.read()
        parquet_io = io.BytesIO(data)
        df_part = pd.read_parquet(parquet_io)
        dfs.append(df_part)

    df_raw = pd.concat(dfs, ignore_index=True)
    print(f"[SPARK] Successfully loaded {len(df_raw):,} raw records into memory.")

    # Initialize PySpark
    spark = create_spark_session()
    spark_df = spark.createDataFrame(df_raw)

    # Convert pickup_datetime to Timestamp and derive Year-Quarter string (e.g. '2023-Q1')
    transformed_df = spark_df \
        .withColumn("pickup_ts", F.to_timestamp("pickup_datetime")) \
        .withColumn("year_quarter", F.concat(
            F.year("pickup_ts"),
            F.lit("-Q"),
            F.quarter("pickup_ts")
        ))

    # Compute quarterly aggregations per vendor
    print("[SPARK] Computing quarterly feature metrics per vendor_id...")
    agg_df = transformed_df.groupBy("vendor_id", "year_quarter").agg(
        F.count("trip_id").alias("total_trips"),
        F.round(F.avg("trip_distance_miles"), 2).alias("avg_trip_distance_miles"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare_amount"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip_amount"),
        F.round(F.sum("fare_amount") + F.sum("tip_amount"), 2).alias("total_revenue")
    ).orderBy("year_quarter", "vendor_id")

    # Collect result to Pandas for DB Loading
    res_pd = agg_df.toPandas()
    print("[SPARK] Aggregated Quarterly Feature Matrix:")
    print(res_pd.to_string())

    # Load into PostgreSQL Database
    db_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"
    engine = create_engine(db_url)
    
    print("[POSTGRES] Writing quarterly features to 'quarterly_feature_aggregations' table...")
    res_pd.to_sql(
        "quarterly_feature_aggregations",
        con=engine,
        if_exists="append",
        index=False,
        method="multi"
    )
    print("[SUCCESS] PySpark batch processing pipeline completed successfully.")
    spark.stop()

if __name__ == "__main__":
    process_batch_aggregations()
