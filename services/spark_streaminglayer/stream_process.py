"""
Speed Layer: Spark Structured Streaming
Consumes real-time taxi trip events from Kafka, computes 15-minute sliding window
aggregations with watermarking, and writes streaming metrics into PostgreSQL.
"""
import os
import sys
import logging
import psycopg2 
from psycopg2.extras import execute_batch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SparkStreamingSpeedLayer")

# Environment configuration with production defaults
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "taxi-trips-stream")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "feature_store")
POSTGRES_USER = os.getenv("POSTGRES_USER", "db_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "db_password")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark_checkpoints/streaming_layer")


def create_spark_streaming_session():
    """Initializes Spark Session configured with Kafka & PostgreSQL JDBC connectors and driver network bindings."""
    logger.info("Initializing Spark Structured Streaming Session...")
    spark = (
        SparkSession.builder
        .appName("SpeedLayer-Streaming")
        .master(SPARK_MASTER_URL)
        .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "spark-streaming"))
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", "7088")
        .config("spark.blockManager.port", "7089")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

UPSERT_REALTIME_SQL = """
    INSERT INTO realtime_feature_aggregations (
        vendor_id,
        window_start,
        window_end,
        live_trips,
        live_avg_fare,
        live_avg_distance,
        created_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (vendor_id, window_start, window_end)
    DO UPDATE SET
        live_trips = EXCLUDED.live_trips,
        live_avg_fare = EXCLUDED.live_avg_fare,
        live_avg_distance = EXCLUDED.live_avg_distance,
        created_at = CURRENT_TIMESTAMP;
"""

def write_streaming_batch_to_postgres(batch_df, batch_id):
    """
    ForeachBatch micro-batch sink writing aggregated window features into PostgreSQL
    using an idempotent ON CONFLICT (vendor_id, window_start, window_end) DO UPDATE (UPSERT).
    """
    record_count = batch_df.count()
    if record_count > 0:
        logger.info(f"Processing micro-batch {batch_id}: writing {record_count} windowed aggregations to PostgreSQL (idempotent UPSERT)...")
        flat_df = batch_df.select(
            F.col("vendor_id"),
            F.col("window.start").cast("timestamp").alias("window_start"),
            F.col("window.end").cast("timestamp").alias("window_end"),
            F.col("live_trips"),
            F.col("live_avg_fare"),
            F.col("live_avg_distance")
        )

        rows = flat_df.collect()
        params = [
            (
                row["vendor_id"],
                row["window_start"],
                row["window_end"],
                row["live_trips"],
                float(row["live_avg_fare"]) if row["live_avg_fare"] is not None else None,
                float(row["live_avg_distance"]) if row["live_avg_distance"] is not None else None
            )
            for row in rows
        ]

        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        try:
            with conn.cursor() as cur:
                execute_batch(cur, UPSERT_REALTIME_SQL, params)
            conn.commit()
            logger.info(f"Micro-batch {batch_id} successfully upserted {len(params)} records into PostgreSQL.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error persisting micro-batch {batch_id} to PostgreSQL: {e}")
            raise e
        finally:
            conn.close()


def main():
    spark = create_spark_streaming_session()

    # Schema definition for taxi event JSON payload
    event_schema = StructType([
        StructField("trip_id", StringType(), False),
        StructField("vendor_id", IntegerType(), False),
        StructField("pickup_datetime", StringType(), False),
        StructField("dropoff_datetime", StringType(), False),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance_miles", DoubleType(), False),
        StructField("fare_amount", DoubleType(), False),
        StructField("tip_amount", DoubleType(), True),
        StructField("payment_type", IntegerType(), True),
    ])

    logger.info(f"Subscribing to Kafka topic '{KAFKA_TOPIC}' at '{KAFKA_BOOTSTRAP}'...")
    raw_kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON string values and cast timestamp
    parsed_stream = (
        raw_kafka_stream
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(F.from_json(F.col("json_payload"), event_schema).alias("data"))
        .select("data.*")
        .withColumn("pickup_timestamp", F.to_timestamp(F.col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))
    )

    # Sliding window aggregation with 10-minute watermark
    windowed_features = (
        parsed_stream
        .withWatermark("pickup_timestamp", "10 minutes")
        .groupBy(
            F.window(F.col("pickup_timestamp"), "15 minutes", "5 minutes"),
            F.col("vendor_id")
        )
        .agg(
            F.count("*").alias("live_trips"),
            F.round(F.avg("fare_amount"), 2).alias("live_avg_fare"),
            F.round(F.avg("trip_distance_miles"), 2).alias("live_avg_distance")
        )
    )

    logger.info("Starting streaming sink with 10-second processing trigger (checkpoint: {CHECKPOINT_DIR})...")
    query = (
        windowed_features.writeStream
        .foreachBatch(write_streaming_batch_to_postgres)
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
