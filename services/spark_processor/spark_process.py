"""
Apache Spark Batch Processing Engine
Reads raw Parquet files directly from S3/MinIO partitioned paths via native Hadoop S3A connector,
executes quarterly aggregation feature engineering in PySpark, and performs an idempotent
UPSERT into PostgreSQL preserving schema constraints, primary keys, and indexes.
Supports distributed execution on a multi-node Spark Standalone cluster via SPARK_MASTER_URL.
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sqlalchemy import create_engine, text

# Environment Variables & Connections
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "feature_store")
POSTGRES_USER = os.getenv("POSTGRES_USER", "db_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "db_password")

# Multi-node distributed Spark master fallback
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")


def create_spark_session():
    """Initializes PySpark Session configured for native MinIO S3A direct reading & cluster execution."""
    print(f"[SPARK] Initializing PySpark Session targeting Spark Master at '{SPARK_MASTER_URL}' with S3A support...")
    
    s3a_endpoint = MINIO_ENDPOINT
    if not s3a_endpoint.startswith("http://") and not s3a_endpoint.startswith("https://"):
        s3a_endpoint = f"http://{s3a_endpoint}"

    builder = (
        SparkSession.builder
        .appName("QuarterlyFeatureAggregator")
        .master(SPARK_MASTER_URL)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.hadoop.fs.s3a.endpoint", s3a_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )
    
    if os.path.exists("/app/jars/hadoop-aws-3.3.4.jar") and os.path.exists("/app/jars/aws-java-sdk-bundle-1.12.262.jar"):
        builder = builder.config("spark.jars", "/app/jars/hadoop-aws-3.3.4.jar,/app/jars/aws-java-sdk-bundle-1.12.262.jar")
    else:
        builder = builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def save_features_to_postgres(res_pd, engine):
    """
    Inserts or updates (UPSERT) aggregated features into PostgreSQL while preserving 
    table constraints, indexes, and primary keys created by schema.sql.
    """
    print("[POSTGRES] Executing PostgreSQL UPSERT to preserve table schema & constraints...")
    
    upsert_sql = text("""
        INSERT INTO quarterly_feature_aggregations (
            vendor_id,
            year_quarter,
            total_trips,
            avg_trip_distance_miles,
            avg_fare_amount,
            avg_tip_amount,
            total_revenue,
            computed_at
        )
        VALUES (
            :vendor_id,
            :year_quarter,
            :total_trips,
            :avg_trip_distance_miles,
            :avg_fare_amount,
            :avg_tip_amount,
            :total_revenue,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (vendor_id, year_quarter)
        DO UPDATE SET
            total_trips = EXCLUDED.total_trips,
            avg_trip_distance_miles = EXCLUDED.avg_trip_distance_miles,
            avg_fare_amount = EXCLUDED.avg_fare_amount,
            avg_tip_amount = EXCLUDED.avg_tip_amount,
            total_revenue = EXCLUDED.total_revenue,
            computed_at = CURRENT_TIMESTAMP;
    """)

    records = res_pd.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(upsert_sql, records)
        
    print(f"[SUCCESS] Upserted {len(records)} feature aggregation records into PostgreSQL.")


def process_batch_aggregations():
    """Reads Parquet data directly from MinIO partitioned paths using PySpark S3A reader, computes quarterly metrics, and loads to PostgreSQL."""
    print("[SPARK] Starting quarterly batch aggregation workflow...")
    
    spark = create_spark_session()
    
    # Read recursively from Hive-partitioned paths or fallback to wildcard
    s3_path = "s3a://raw-data/yellow_taxi_2023/"
    print(f"[SPARK] Reading partitioned Parquet data directly from '{s3_path}' via native S3A connector...")
    
    try:
        spark_df = spark.read.option("recursiveFileLookup", "true").parquet(s3_path)
    except Exception as e:
        print(f"[ERROR] Failed to read Parquet files from '{s3_path}': {e}")
        sys.exit(1)

    record_count = spark_df.count()
    print(f"[SPARK] Successfully loaded {record_count:,} raw records directly into PySpark DataFrame.")

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

    res_pd = agg_df.toPandas()
    print("[SPARK] Aggregated Quarterly Feature Matrix:")
    print(res_pd.to_string())

    db_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"
    engine = create_engine(db_url)
    
    save_features_to_postgres(res_pd, engine)

    print("[SUCCESS] PySpark batch processing pipeline completed successfully.")
    spark.stop()


if __name__ == "__main__":
    process_batch_aggregations()
