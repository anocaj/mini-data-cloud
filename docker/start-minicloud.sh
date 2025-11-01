#!/bin/bash

# Mini Data Cloud Docker Startup Script with Iceberg Integration
# This script ensures proper startup order and initialization

set -e

echo "Starting Mini Data Cloud with Iceberg Integration..."

# Load environment variables
if [ -f .env.docker ]; then
    echo "Loading Docker environment variables..."
    export $(cat .env.docker | grep -v '^#' | xargs)
fi

# Function to wait for service health
wait_for_service() {
    local service_name=$1
    local max_attempts=30
    local attempt=1
    
    echo "Waiting for $service_name to be healthy..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker compose ps $service_name | grep -q "healthy"; then
            echo "$service_name is healthy!"
            return 0
        fi
        
        echo "Attempt $attempt/$max_attempts: $service_name not ready yet..."
        sleep 10
        attempt=$((attempt + 1))
    done
    
    echo "ERROR: $service_name failed to become healthy within timeout"
    return 1
}

# Function to check if service is running
check_service() {
    local service_name=$1
    if docker compose ps $service_name | grep -q "Up"; then
        echo "$service_name is running"
        return 0
    else
        echo "ERROR: $service_name is not running"
        return 1
    fi
}

# Clean up any existing containers
echo "Cleaning up existing containers..."
docker compose down -v --remove-orphans

# Build images
echo "Building Docker images..."
docker compose build

# Start infrastructure services first
echo "Starting infrastructure services..."
docker compose up -d metadata-db minio

# Wait for infrastructure to be healthy
wait_for_service metadata-db
wait_for_service minio

# Initialize MinIO buckets
echo "Initializing MinIO buckets..."
docker compose up minio-setup
docker compose wait minio-setup

# Start monitoring services
echo "Starting monitoring services..."
docker compose up -d prometheus grafana

# Start application services
echo "Starting application services..."
docker compose up -d control-plane

# Wait for control plane to be ready
echo "Waiting for control plane to be ready..."
sleep 30

# Check control plane health
max_attempts=30
attempt=1
while [ $attempt -le $max_attempts ]; do
    if curl -f http://localhost:8080/actuator/health > /dev/null 2>&1; then
        echo "Control plane is healthy!"
        break
    fi
    
    echo "Attempt $attempt/$max_attempts: Control plane not ready yet..."
    sleep 10
    attempt=$((attempt + 1))
    
    if [ $attempt -gt $max_attempts ]; then
        echo "ERROR: Control plane failed to start within timeout"
        exit 1
    fi
done

# Start worker nodes
echo "Starting worker nodes..."
docker compose up -d worker-1 worker-2

# Wait for workers to register
echo "Waiting for workers to register..."
sleep 20

# Verify all services are running
echo "Verifying service status..."
check_service metadata-db
check_service minio
check_service control-plane
check_service worker-1
check_service worker-2
check_service prometheus
check_service grafana

echo ""
echo "🎉 Mini Data Cloud with Iceberg Integration is now running!"
echo ""
echo "Available services:"
echo "  - Control Plane API: http://localhost:8080"
echo "  - Control Plane Health: http://localhost:8080/actuator/health"
echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)"
echo "  - Grafana Dashboard: http://localhost:3000 (admin/admin)"
echo "  - Prometheus: http://localhost:9091"
echo "  - PostgreSQL: localhost:5432 (minicloud/minicloud123)"
echo ""
echo "To view logs: docker compose logs -f [service-name]"
echo "To stop: docker compose down"
echo "To stop and remove volumes: docker compose down -v"
echo ""

# Show container status
echo "Container Status:"
docker compose ps