#!/bin/bash

# Health check script for Mini Data Cloud services

set -e

SERVICE=${1:-control-plane}
HOST=${2:-localhost}

case $SERVICE in
    "control-plane")
        PORT=8080
        ENDPOINT="/actuator/health"
        ;;
    "worker")
        PORT=8081
        ENDPOINT="/health"
        ;;
    "postgres")
        # Use pg_isready for PostgreSQL
        pg_isready -h $HOST -p 5432 -U minicloud -d minicloud_catalog
        exit $?
        ;;
    "minio")
        # Use curl for MinIO
        curl -f http://$HOST:9000/minio/health/live
        exit $?
        ;;
    *)
        echo "Unknown service: $SERVICE"
        exit 1
        ;;
esac

# HTTP health check
curl -f http://$HOST:$PORT$ENDPOINT > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "$SERVICE is healthy"
    exit 0
else
    echo "$SERVICE is not healthy"
    exit 1
fi