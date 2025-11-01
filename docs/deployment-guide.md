# Mini Data Cloud Deployment Guide

## Overview

This guide covers deployment options for Mini Data Cloud with Apache Iceberg integration, from development environments to production-ready deployments.

## Table of Contents

1. [Quick Start (Development)](#quick-start-development)
2. [Production Deployment](#production-deployment)
3. [Configuration Reference](#configuration-reference)
4. [Scaling and Performance](#scaling-and-performance)
5. [Security Configuration](#security-configuration)
6. [Monitoring Setup](#monitoring-setup)
7. [Backup and Recovery](#backup-and-recovery)
8. [Troubleshooting](#troubleshooting)

## Quick Start (Development)

### Prerequisites

- Docker 20.10+ and Docker Compose 2.0+
- At least 8GB RAM available
- 20GB free disk space
- Ports 8080, 8081-8085, 5432, 9000-9001, 3000, 9091 available

### 1. Clone and Start

```bash
# Clone the repository
git clone <repository-url>
cd mini-data-cloud

# Start all services
docker compose up -d

# Wait for services to be ready (2-3 minutes)
./verify-setup.sh
```

### 2. Verify Installation

```bash
# Check system health
curl http://localhost:8080/health

# Verify Iceberg catalog
curl http://localhost:8080/api/iceberg/tables

# Load sample data
curl -X POST -F "file=@sample-data/bank_transactions.csv" \
     -F "tableName=bank_transactions" \
     -F "format=iceberg" \
     http://localhost:8080/api/data/upload

# Run test query
curl -X POST -H "Content-Type: application/json" \
     -d '{"sql": "SELECT COUNT(*) FROM bank_transactions"}' \
     http://localhost:8080/api/query/execute
```

### 3. Access Services

- **Control Plane API**: http://localhost:8080
- **Grafana Dashboards**: http://localhost:3000 (admin/admin)
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin123)
- **Prometheus**: http://localhost:9091

## Production Deployment

### Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Load Balancer │    │   Control Plane │    │   Worker Nodes  │
│   (nginx/ALB)   │────│   (Multiple)    │────│   (Auto-scale)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │              ┌─────────────────┐              │
         │              │   Metadata DB   │              │
         │              │  (PostgreSQL)   │              │
         │              └─────────────────┘              │
         │                       │                       │
         │              ┌─────────────────┐              │
         └──────────────│  Object Storage │──────────────┘
                        │  (S3/MinIO)     │
                        └─────────────────┘
```

### 1. Infrastructure Requirements

#### Minimum Production Setup
- **Control Plane**: 4 vCPU, 8GB RAM, 100GB SSD
- **Workers**: 2 vCPU, 4GB RAM each (2-10 workers)
- **PostgreSQL**: 2 vCPU, 4GB RAM, 500GB SSD
- **Object Storage**: S3-compatible with 1TB+ capacity
- **Load Balancer**: nginx or cloud load balancer

#### Recommended Production Setup
- **Control Plane**: 8 vCPU, 16GB RAM, 200GB SSD (2+ instances)
- **Workers**: 4 vCPU, 8GB RAM each (5-50 workers)
- **PostgreSQL**: 4 vCPU, 8GB RAM, 1TB SSD (with replicas)
- **Object Storage**: S3 with multi-region replication
- **Monitoring**: Dedicated Prometheus/Grafana cluster

### 2. Production Docker Compose

Create `docker-compose.prod.yml`:

```yaml
version: '3.8'

services:
  # Load Balancer
  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./ssl:/etc/nginx/ssl:ro
    depends_on:
      - control-plane-1
      - control-plane-2
    networks:
      - minicloud-network

  # Control Plane Cluster
  control-plane-1:
    build:
      context: .
      dockerfile: minicloud-control-plane/Dockerfile
      target: production
    environment:
      - SPRING_PROFILES_ACTIVE=production
      - SERVER_PORT=8080
      - POSTGRES_URL=jdbc:postgresql://postgres-primary:5432/minicloud_catalog
      - POSTGRES_USER=${POSTGRES_USER}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - MINIO_ENDPOINT=https://s3.amazonaws.com
      - MINIO_ACCESS_KEY=${AWS_ACCESS_KEY_ID}
      - MINIO_SECRET_KEY=${AWS_SECRET_ACCESS_KEY}
      - ICEBERG_WAREHOUSE=s3a://your-iceberg-bucket/
      - CLUSTER_NODE_ID=control-plane-1
    volumes:
      - control-plane-1-data:/data
    networks:
      - minicloud-network
    deploy:
      resources:
        limits:
          memory: 8G
          cpus: '4'
        reservations:
          memory: 4G
          cpus: '2'

  control-plane-2:
    build:
      context: .
      dockerfile: minicloud-control-plane/Dockerfile
      target: production
    environment:
      - SPRING_PROFILES_ACTIVE=production
      - SERVER_PORT=8080
      - POSTGRES_URL=jdbc:postgresql://postgres-primary:5432/minicloud_catalog
      - POSTGRES_USER=${POSTGRES_USER}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - MINIO_ENDPOINT=https://s3.amazonaws.com
      - MINIO_ACCESS_KEY=${AWS_ACCESS_KEY_ID}
      - MINIO_SECRET_KEY=${AWS_SECRET_ACCESS_KEY}
      - ICEBERG_WAREHOUSE=s3a://your-iceberg-bucket/
      - CLUSTER_NODE_ID=control-plane-2
    volumes:
      - control-plane-2-data:/data
    networks:
      - minicloud-network
    deploy:
      resources:
        limits:
          memory: 8G
          cpus: '4'
        reservations:
          memory: 4G
          cpus: '2'

  # PostgreSQL Primary
  postgres-primary:
    image: postgres:15
    environment:
      - POSTGRES_DB=minicloud_catalog
      - POSTGRES_USER=${POSTGRES_USER}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - POSTGRES_REPLICATION_MODE=master
      - POSTGRES_REPLICATION_USER=${POSTGRES_REPLICATION_USER}
      - POSTGRES_REPLICATION_PASSWORD=${POSTGRES_REPLICATION_PASSWORD}
    volumes:
      - postgres-primary-data:/var/lib/postgresql/data
      - ./docker/postgres/init-iceberg-catalog.sql:/docker-entrypoint-initdb.d/01-init.sql:ro
      - ./docker/postgres/postgresql.conf:/etc/postgresql/postgresql.conf:ro
    networks:
      - minicloud-network
    deploy:
      resources:
        limits:
          memory: 8G
          cpus: '4'
        reservations:
          memory: 4G
          cpus: '2'

  # PostgreSQL Replica
  postgres-replica:
    image: postgres:15
    environment:
      - POSTGRES_MASTER_SERVICE=postgres-primary
      - POSTGRES_REPLICATION_MODE=slave
      - POSTGRES_REPLICATION_USER=${POSTGRES_REPLICATION_USER}
      - POSTGRES_REPLICATION_PASSWORD=${POSTGRES_REPLICATION_PASSWORD}
    volumes:
      - postgres-replica-data:/var/lib/postgresql/data
    networks:
      - minicloud-network
    depends_on:
      - postgres-primary

  # Worker Auto-scaling Group
  worker-manager:
    build:
      context: .
      dockerfile: minicloud-worker/Dockerfile
      target: production
    environment:
      - WORKER_MANAGER_MODE=true
      - MIN_WORKERS=2
      - MAX_WORKERS=20
      - SCALE_UP_THRESHOLD=0.8
      - SCALE_DOWN_THRESHOLD=0.3
      - CONTROL_PLANE_ENDPOINTS=control-plane-1:9090,control-plane-2:9090
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    networks:
      - minicloud-network

  # Monitoring Stack
  prometheus:
    image: prom/prometheus:latest
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--storage.tsdb.retention.time=30d'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'
      - '--web.enable-lifecycle'
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus-data:/prometheus
    networks:
      - minicloud-network
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: '2'

  grafana:
    image: grafana/grafana:latest
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD}
      - GF_INSTALL_PLUGINS=grafana-piechart-panel
    volumes:
      - grafana-data:/var/lib/grafana
      - ./monitoring/grafana/dashboards:/etc/grafana/provisioning/dashboards:ro
      - ./monitoring/grafana/datasources:/etc/grafana/provisioning/datasources:ro
    networks:
      - minicloud-network

volumes:
  control-plane-1-data:
  control-plane-2-data:
  postgres-primary-data:
  postgres-replica-data:
  prometheus-data:
  grafana-data:

networks:
  minicloud-network:
    driver: bridge
```

### 3. Environment Configuration

Create `.env.prod`:

```bash
# Database Configuration
POSTGRES_USER=minicloud_prod
POSTGRES_PASSWORD=your_secure_password_here
POSTGRES_REPLICATION_USER=replicator
POSTGRES_REPLICATION_PASSWORD=replication_password_here

# AWS/S3 Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=us-east-1

# Security
GRAFANA_ADMIN_PASSWORD=your_grafana_password
JWT_SECRET=your_jwt_secret_key_here

# Application Configuration
MINICLOUD_ENV=production
LOG_LEVEL=INFO
METRICS_ENABLED=true
```

### 4. SSL/TLS Configuration

Create `nginx.conf`:

```nginx
upstream control_plane {
    server control-plane-1:8080;
    server control-plane-2:8080;
}

server {
    listen 80;
    server_name your-domain.com;
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name your-domain.com;

    ssl_certificate /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;

    location / {
        proxy_pass http://control_plane;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /health {
        access_log off;
        proxy_pass http://control_plane;
    }
}
```

### 5. Deploy to Production

```bash
# Set environment
export COMPOSE_FILE=docker-compose.prod.yml

# Deploy
docker compose --env-file .env.prod up -d

# Verify deployment
./verify-production-setup.sh

# Run health checks
curl https://your-domain.com/health
```

## Configuration Reference

### Control Plane Configuration

`application-production.yml`:

```yaml
server:
  port: 8080
  compression:
    enabled: true
  http2:
    enabled: true

spring:
  datasource:
    url: ${POSTGRES_URL}
    username: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
      connection-timeout: 30000
      idle-timeout: 600000
      max-lifetime: 1800000

minicloud:
  # Iceberg Configuration
  iceberg:
    enabled: true
    catalog:
      type: jdbc
      uri: ${POSTGRES_URL}
      warehouse: ${ICEBERG_WAREHOUSE}
    cache:
      l1-size: 10000
      l1-ttl: 300s
      l2-size: 100000
      l2-ttl: 3600s
      l3-enabled: true
    
  # Storage Configuration
  storage:
    s3:
      endpoint: ${MINIO_ENDPOINT}
      access-key: ${MINIO_ACCESS_KEY}
      secret-key: ${MINIO_SECRET_KEY}
      region: ${AWS_REGION}
    
  # Query Engine Configuration
  query:
    max-concurrent-queries: 100
    query-timeout: 3600s
    result-cache-ttl: 1800s
    
  # Worker Configuration
  workers:
    auto-scale: true
    min-workers: 2
    max-workers: 50
    scale-up-threshold: 0.8
    scale-down-threshold: 0.3
    health-check-interval: 30s
    
  # Security Configuration
  security:
    jwt:
      secret: ${JWT_SECRET}
      expiration: 86400
    cors:
      allowed-origins: "https://your-domain.com"
      
  # Monitoring Configuration
  monitoring:
    metrics:
      enabled: true
      export-interval: 30s
    logging:
      level: ${LOG_LEVEL:INFO}
      structured: true
```

### Worker Configuration

`application-production.yml` for workers:

```yaml
server:
  port: 8081

minicloud:
  worker:
    id: ${WORKER_ID}
    control-plane-endpoints: ${CONTROL_PLANE_ENDPOINTS}
    max-memory: ${MAX_MEMORY_MB:4096}
    max-cpu-cores: ${MAX_CPU_CORES:4}
    heartbeat-interval: 30s
    
  storage:
    s3:
      endpoint: ${MINIO_ENDPOINT}
      access-key: ${MINIO_ACCESS_KEY}
      secret-key: ${MINIO_SECRET_KEY}
      region: ${AWS_REGION}
      
  monitoring:
    metrics:
      enabled: true
      port: 8082
```

## Scaling and Performance

### Horizontal Scaling

#### Control Plane Scaling
```bash
# Scale control plane instances
docker compose --env-file .env.prod up -d --scale control-plane=3

# Update load balancer configuration
# Add new instances to nginx upstream block
```

#### Worker Auto-scaling
```bash
# Configure auto-scaling parameters
export MIN_WORKERS=5
export MAX_WORKERS=100
export SCALE_UP_THRESHOLD=0.7
export SCALE_DOWN_THRESHOLD=0.2

# Deploy with auto-scaling
docker compose --env-file .env.prod up -d
```

### Vertical Scaling

#### Resource Limits
```yaml
# In docker-compose.prod.yml
deploy:
  resources:
    limits:
      memory: 16G
      cpus: '8'
    reservations:
      memory: 8G
      cpus: '4'
```

#### JVM Tuning
```bash
# Control Plane JVM options
JAVA_OPTS="-Xms4g -Xmx8g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# Worker JVM options
JAVA_OPTS="-Xms2g -Xmx4g -XX:+UseG1GC -XX:+UseStringDeduplication"
```

### Performance Optimization

#### Database Optimization
```sql
-- PostgreSQL configuration for production
-- In postgresql.conf:
shared_buffers = 2GB
effective_cache_size = 6GB
maintenance_work_mem = 512MB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 100
random_page_cost = 1.1
effective_io_concurrency = 200
```

#### Storage Optimization
```yaml
# S3/MinIO optimization
minicloud:
  storage:
    s3:
      multipart-threshold: 64MB
      multipart-part-size: 16MB
      max-connections: 50
      connection-timeout: 60s
      socket-timeout: 60s
```

## Security Configuration

### Authentication and Authorization

#### JWT Configuration
```yaml
minicloud:
  security:
    jwt:
      secret: ${JWT_SECRET}
      expiration: 86400  # 24 hours
      refresh-expiration: 604800  # 7 days
    
    oauth2:
      enabled: true
      providers:
        google:
          client-id: ${GOOGLE_CLIENT_ID}
          client-secret: ${GOOGLE_CLIENT_SECRET}
```

#### API Security
```yaml
minicloud:
  security:
    api:
      rate-limiting:
        enabled: true
        requests-per-minute: 1000
      cors:
        allowed-origins: 
          - "https://your-domain.com"
          - "https://app.your-domain.com"
        allowed-methods: ["GET", "POST", "PUT", "DELETE"]
        allowed-headers: ["*"]
```

### Network Security

#### Firewall Rules
```bash
# Allow only necessary ports
ufw allow 22/tcp    # SSH
ufw allow 80/tcp    # HTTP
ufw allow 443/tcp   # HTTPS
ufw deny 8080/tcp   # Block direct access to control plane
ufw deny 5432/tcp   # Block direct access to database
```

#### Docker Network Security
```yaml
networks:
  minicloud-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16
    driver_opts:
      com.docker.network.bridge.enable_icc: "false"
```

### Data Encryption

#### At Rest
```yaml
minicloud:
  storage:
    encryption:
      enabled: true
      algorithm: AES-256-GCM
      key-management: aws-kms  # or vault
      
  database:
    encryption:
      enabled: true
      transparent-data-encryption: true
```

#### In Transit
```yaml
minicloud:
  security:
    tls:
      enabled: true
      min-version: TLSv1.2
      cipher-suites:
        - TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
        - TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
```

## Monitoring Setup

### Prometheus Configuration

`prometheus.yml`:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "rules/*.yml"

scrape_configs:
  - job_name: 'minicloud-control-plane'
    static_configs:
      - targets: ['control-plane-1:8080', 'control-plane-2:8080']
    metrics_path: '/actuator/prometheus'
    
  - job_name: 'minicloud-workers'
    consul_sd_configs:
      - server: 'consul:8500'
        services: ['minicloud-worker']
    
  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres-primary:9187']
      
  - job_name: 'node-exporter'
    static_configs:
      - targets: ['node-exporter:9100']

alerting:
  alertmanagers:
    - static_configs:
        - targets: ['alertmanager:9093']
```

### Alerting Rules

`rules/minicloud.yml`:

```yaml
groups:
  - name: minicloud
    rules:
      - alert: ControlPlaneDown
        expr: up{job="minicloud-control-plane"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Control plane instance is down"
          
      - alert: HighQueryLatency
        expr: histogram_quantile(0.95, query_execution_duration_seconds) > 30
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High query latency detected"
          
      - alert: WorkerUnhealthy
        expr: minicloud_worker_health_status == 0
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Worker node is unhealthy"
```

### Grafana Dashboards

Import production dashboards:

```bash
# Import dashboards
curl -X POST \
  http://admin:${GRAFANA_ADMIN_PASSWORD}@localhost:3000/api/dashboards/db \
  -H 'Content-Type: application/json' \
  -d @monitoring/dashboards/production-overview.json
```

## Backup and Recovery

### Database Backup

```bash
#!/bin/bash
# backup-database.sh

BACKUP_DIR="/backups/postgres"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="minicloud_catalog_${DATE}.sql"

# Create backup
docker exec postgres-primary pg_dump \
  -U ${POSTGRES_USER} \
  -d minicloud_catalog \
  --clean --if-exists \
  > "${BACKUP_DIR}/${BACKUP_FILE}"

# Compress backup
gzip "${BACKUP_DIR}/${BACKUP_FILE}"

# Upload to S3
aws s3 cp "${BACKUP_DIR}/${BACKUP_FILE}.gz" \
  s3://your-backup-bucket/database/

# Cleanup old backups (keep 30 days)
find ${BACKUP_DIR} -name "*.gz" -mtime +30 -delete
```

### Iceberg Data Backup

```bash
#!/bin/bash
# backup-iceberg.sh

# Iceberg tables are stored in S3, so we backup metadata
BACKUP_DIR="/backups/iceberg"
DATE=$(date +%Y%m%d_%H%M%S)

# Backup table metadata
curl -s http://localhost:8080/api/iceberg/tables | \
  jq '.' > "${BACKUP_DIR}/tables_${DATE}.json"

# Backup snapshots for each table
for table in $(curl -s http://localhost:8080/api/iceberg/tables | jq -r '.[].name'); do
  curl -s "http://localhost:8080/api/iceberg/tables/${table}/snapshots" | \
    jq '.' > "${BACKUP_DIR}/snapshots_${table}_${DATE}.json"
done

# Upload to S3
aws s3 sync ${BACKUP_DIR} s3://your-backup-bucket/iceberg/
```

### Disaster Recovery

```bash
#!/bin/bash
# disaster-recovery.sh

# 1. Restore database
gunzip -c /backups/postgres/minicloud_catalog_latest.sql.gz | \
  docker exec -i postgres-primary psql -U ${POSTGRES_USER} -d minicloud_catalog

# 2. Restore Iceberg metadata
# Tables and data are preserved in S3, just need to refresh catalog
curl -X POST http://localhost:8080/api/iceberg/catalog/refresh

# 3. Verify system health
./verify-production-setup.sh
```

## Troubleshooting

### Common Issues

#### Control Plane Won't Start
```bash
# Check logs
docker logs control-plane-1

# Common causes:
# 1. Database connection issues
# 2. S3 credentials invalid
# 3. Port conflicts
# 4. Insufficient memory

# Solutions:
# Check database connectivity
docker exec control-plane-1 pg_isready -h postgres-primary -p 5432

# Verify S3 access
docker exec control-plane-1 aws s3 ls s3://your-iceberg-bucket/

# Check resource usage
docker stats
```

#### Workers Not Connecting
```bash
# Check worker logs
docker logs worker-1

# Verify control plane connectivity
docker exec worker-1 nc -zv control-plane-1 9090

# Check worker registration
curl http://localhost:8080/api/workers
```

#### Query Performance Issues
```bash
# Check query metrics
curl http://localhost:8080/api/iceberg/metrics/queries

# Analyze slow queries
curl http://localhost:8080/api/query/history?slow=true

# Update table statistics
curl -X POST http://localhost:8080/api/iceberg/tables/your_table/analyze
```

#### Database Connection Pool Exhausted
```bash
# Check active connections
docker exec postgres-primary psql -U ${POSTGRES_USER} -d minicloud_catalog \
  -c "SELECT count(*) FROM pg_stat_activity;"

# Increase pool size in application.yml
spring:
  datasource:
    hikari:
      maximum-pool-size: 50
```

### Health Checks

```bash
#!/bin/bash
# health-check.sh

echo "Checking system health..."

# Control Plane
if curl -f -s http://localhost:8080/health > /dev/null; then
  echo "✓ Control Plane healthy"
else
  echo "✗ Control Plane unhealthy"
fi

# Database
if docker exec postgres-primary pg_isready -U ${POSTGRES_USER} > /dev/null; then
  echo "✓ Database healthy"
else
  echo "✗ Database unhealthy"
fi

# Workers
WORKER_COUNT=$(curl -s http://localhost:8080/api/workers | jq length)
echo "✓ ${WORKER_COUNT} workers active"

# Storage
if curl -f -s http://localhost:9000/minio/health/live > /dev/null; then
  echo "✓ Storage healthy"
else
  echo "✗ Storage unhealthy"
fi
```

### Log Analysis

```bash
# Centralized logging with ELK stack
docker run -d \
  --name elasticsearch \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  elasticsearch:7.14.0

# Configure log shipping
# Add to docker-compose.prod.yml:
logging:
  driver: "json-file"
  options:
    max-size: "10m"
    max-file: "3"
```

## Maintenance

### Regular Maintenance Tasks

```bash
#!/bin/bash
# maintenance.sh

# 1. Update table statistics (weekly)
for table in $(curl -s http://localhost:8080/api/iceberg/tables | jq -r '.[].name'); do
  curl -X POST "http://localhost:8080/api/iceberg/tables/${table}/analyze"
done

# 2. Compact small files (daily)
for table in $(curl -s http://localhost:8080/api/iceberg/tables | jq -r '.[].name'); do
  curl -X POST "http://localhost:8080/api/iceberg/tables/${table}/compact"
done

# 3. Expire old snapshots (weekly)
for table in $(curl -s http://localhost:8080/api/iceberg/tables | jq -r '.[].name'); do
  curl -X POST -H "Content-Type: application/json" \
    -d '{"retentionDays": 30}' \
    "http://localhost:8080/api/iceberg/tables/${table}/snapshots/expire"
done

# 4. Vacuum deleted files (monthly)
for table in $(curl -s http://localhost:8080/api/iceberg/tables | jq -r '.[].name'); do
  curl -X POST "http://localhost:8080/api/iceberg/tables/${table}/vacuum"
done

# 5. Database maintenance
docker exec postgres-primary psql -U ${POSTGRES_USER} -d minicloud_catalog \
  -c "VACUUM ANALYZE;"
```

### Upgrade Procedures

```bash
#!/bin/bash
# upgrade.sh

# 1. Backup current state
./backup-database.sh
./backup-iceberg.sh

# 2. Pull new images
docker compose --env-file .env.prod pull

# 3. Rolling upgrade
docker compose --env-file .env.prod up -d --no-deps control-plane-1
# Wait for health check
sleep 30
docker compose --env-file .env.prod up -d --no-deps control-plane-2

# 4. Upgrade workers
docker compose --env-file .env.prod up -d --no-deps worker-manager

# 5. Verify upgrade
./verify-production-setup.sh
```

This comprehensive deployment guide covers all aspects of deploying Mini Data Cloud with Iceberg integration from development to production environments.