# Docker Infrastructure for Mini Data Cloud with Iceberg Integration

This directory contains the Docker infrastructure configuration for running Mini Data Cloud with Apache Iceberg integration.

## Architecture Overview

The Docker setup includes the following services:

### Core Services
- **control-plane**: Main application service with REST API and gRPC endpoints
- **worker-1, worker-2**: Query execution workers
- **metadata-db**: PostgreSQL database for Iceberg catalog metadata
- **minio**: S3-compatible object storage for data files

### Supporting Services
- **minio-setup**: Initializes MinIO buckets and policies
- **prometheus**: Metrics collection
- **grafana**: Monitoring dashboards

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- At least 4GB RAM available for containers
- Ports 3000, 5432, 8080-8083, 9000-9001, 9090-9091 available

### Starting the System

1. **Using the startup script (recommended):**
   ```bash
   ./docker/start-minicloud.sh
   ```

2. **Manual startup:**
   ```bash
   # Load environment variables
   export $(cat .env.docker | grep -v '^#' | xargs)
   
   # Start infrastructure
   docker compose up -d metadata-db minio
   
   # Initialize buckets
   docker compose up minio-setup
   
   # Start application
   docker compose up -d
   ```

### Stopping the System

```bash
# Stop services
docker compose down

# Stop and remove volumes (WARNING: This deletes all data)
docker compose down -v
```

## Service Configuration

### PostgreSQL (metadata-db)
- **Port**: 5432
- **Database**: minicloud_catalog
- **Username**: minicloud
- **Password**: minicloud123
- **Initialization**: Automatic schema creation via `init-iceberg-catalog.sql`

### MinIO (minio)
- **API Port**: 9000
- **Console Port**: 9001
- **Username**: minioadmin
- **Password**: minioadmin123
- **Buckets**: 
  - `iceberg-data`: Table data files
  - `iceberg-metadata`: Table metadata and manifests
  - `iceberg-stats`: Statistics and optimization data
  - `iceberg-logs`: Transaction logs and audit trails

### Control Plane
- **REST API**: http://localhost:8080
- **gRPC**: localhost:9090
- **Health Check**: http://localhost:8080/actuator/health
- **Profile**: docker (uses PostgreSQL and MinIO)

### Workers
- **Worker 1**: HTTP 8081, Arrow Flight 8082
- **Worker 2**: HTTP 8083, Arrow Flight 8085
- **Memory**: 2GB per worker

## Environment Variables

Key environment variables are defined in `.env.docker`:

```bash
# Database
POSTGRES_URL=jdbc:postgresql://metadata-db:5432/minicloud_catalog
POSTGRES_USER=minicloud
POSTGRES_PASSWORD=minicloud123

# Storage
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123

# Iceberg
ICEBERG_ENABLED=true
ICEBERG_CATALOG_TYPE=jdbc
ICEBERG_WAREHOUSE=s3a://iceberg-data/
```

## Data Persistence

The following Docker volumes are used for data persistence:

- `metadata_data`: PostgreSQL database files
- `minio_data`: MinIO object storage
- `minio_cache`: MinIO cache directory
- `grafana_data`: Grafana dashboards and settings

## Monitoring

### Prometheus
- **URL**: http://localhost:9091
- **Configuration**: `../tools/monitoring/prometheus.yml`
- **Targets**: Control plane and worker metrics

### Grafana
- **URL**: http://localhost:3000
- **Username**: admin
- **Password**: admin
- **Dashboards**: Pre-configured for Mini Data Cloud metrics

## Troubleshooting

### Common Issues

1. **Port conflicts**: Ensure ports 3000, 5432, 8080-8083, 9000-9001, 9090-9091 are available
2. **Memory issues**: Increase Docker memory limit to at least 4GB
3. **Startup timeout**: Services may take 1-2 minutes to fully initialize

### Checking Service Health

```bash
# Check all services
docker compose ps

# Check specific service logs
docker compose logs -f control-plane
docker compose logs -f metadata-db
docker compose logs -f minio

# Check service health
curl http://localhost:8080/actuator/health
curl http://localhost:9000/minio/health/live
```

### Database Access

```bash
# Connect to PostgreSQL
docker compose exec metadata-db psql -U minicloud -d minicloud_catalog

# Check Iceberg catalog tables
docker compose exec metadata-db psql -U minicloud -d minicloud_catalog -c "\dt iceberg_catalog.*"
```

### MinIO Access

```bash
# Access MinIO console
open http://localhost:9001

# List buckets using mc client
docker compose exec minio-setup mc ls myminio/
```

## Development

### Building Images

```bash
# Build all images
docker compose build

# Build specific service
docker compose build control-plane
```

### Debugging

```bash
# Run with debug logging
SPRING_PROFILES_ACTIVE=docker,debug docker compose up

# Access container shell
docker compose exec control-plane bash
docker compose exec metadata-db bash
```

### Configuration Changes

After modifying configuration files:

1. Rebuild affected images: `docker compose build [service]`
2. Restart services: `docker compose restart [service]`
3. For database schema changes: `docker compose down -v && docker compose up`

## File Structure

```
docker/
├── README.md                    # This file
├── start-minicloud.sh          # Startup script
├── healthcheck.sh              # Health check utilities
├── postgres/
│   └── init-iceberg-catalog.sql # Database initialization
├── minio/
│   ├── init-buckets.sh         # Bucket initialization
│   └── minio.env               # MinIO configuration
└── .env.docker                 # Environment variables
```

## Security Notes

⚠️ **Development Only**: This configuration is for development purposes only. For production:

- Change default passwords
- Use proper SSL/TLS certificates
- Implement proper access controls
- Use secrets management
- Configure network security
- Enable audit logging