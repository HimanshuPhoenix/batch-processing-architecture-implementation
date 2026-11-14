
#!/bin/sh

set -eu

# Use the same Spark image configured in Docker Compose
SPARK_IMAGE="${SPARK_IMAGE:-apache/spark:3.5.0}"

echo "Discovering Spark runtime UID and GID..."

# Discover the default runtime UID/GID from the actual image
SPARK_UID=$(docker run --rm --entrypoint id "$SPARK_IMAGE" -u)
SPARK_GID=$(docker run --rm --entrypoint id "$SPARK_IMAGE" -g)

if [ -z "$SPARK_UID" ] || [ -z "$SPARK_GID" ]; then
    echo "ERROR: Unable to determine Spark UID/GID."
    exit 1
fi

case "$SPARK_UID:$SPARK_GID" in
    *[!0-9:]*)
        echo "ERROR: Invalid Spark UID/GID: $SPARK_UID:$SPARK_GID"
        exit 1
        ;;
esac

echo "Spark image: $SPARK_IMAGE"
echo "Detected UID: $SPARK_UID"
echo "Detected GID: $SPARK_GID"

# Export values for Docker Compose interpolation
export SPARK_UID SPARK_GID

echo "Initializing persistent checkpoint volume..."

docker compose run --rm --no-deps init-checkpoint-volume

echo "Checkpoint volume initialization completed."