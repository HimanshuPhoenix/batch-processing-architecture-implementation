
#!/bin/sh

set -eu

echo "Stopping Streaming Infrastructure..."

# Stop producer first, then streaming processor
docker compose --profile streaming stop \
    stream-producer spark-streaming

# Stop Kafka and Spark cluster
docker compose --profile streaming stop \
    kafka spark-worker spark-master

echo "Streaming infrastructure stopped."
echo "Persistent checkpoint and Kafka volumes preserved."
