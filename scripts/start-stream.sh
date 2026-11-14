
#!/bin/sh

set -eu

echo "========================================"
echo " Starting Streaming Infrastructure"
echo "========================================"

SPARK_IMAGE="${SPARK_IMAGE:-apache/spark:3.5.0}"

# Discover Spark runtime UID and GID
echo "[1/3] Discovering Spark runtime identity..."

SPARK_UID=$(docker run --rm --entrypoint id "$SPARK_IMAGE" -u)
SPARK_GID=$(docker run --rm --entrypoint id "$SPARK_IMAGE" -g)

export SPARK_UID SPARK_GID

echo "Spark UID: $SPARK_UID"
echo "Spark GID: $SPARK_GID"

# Initialize persistent checkpoint volume
echo "[2/3] Initializing checkpoint volume..."

docker compose run --rm --no-deps init-checkpoint-volume

# Start streaming infrastructure
echo "[3/3] Starting Kafka and Spark Streaming..."

docker compose --profile streaming up -d \
    kafka spark-master spark-worker spark-streaming

echo "========================================"
echo " Streaming infrastructure started."
echo " Start the producer separately when needed, using: [docker compose --profile streaming up -d stream-producer]"
echo "========================================"
