# Configuration Reference

## Overview

This document provides a comprehensive reference for all configuration options available in Mini Data Cloud with Apache Iceberg integration.

## Table of Contents

1. [Application Configuration](#application-configuration)
2. [Iceberg Configuration](#iceberg-configuration)
3. [Storage Configuration](#storage-configuration)
4. [Query Engine Configuration](#query-engine-configuration)
5. [Worker Configuration](#worker-configuration)
6. [Security Configuration](#security-configuration)
7. [Monitoring Configuration](#monitoring-configuration)
8. [NYC Datasets Configuration](#nyc-datasets-configuration)
9. [Environment Variables](#environment-variables)
10. [Docker Configuration](#docker-configuration)

## Application Configuration

### Control Plane Configuration

`application.yml` for the control plane service:

```yaml
server:
  port: 8080                    # HTTP port for REST API
  compression:
    enabled: true               # Enable response compression
  http2:
    enabled: true               # Enable HTTP/2 support

spring:
  application:
    name: minicloud-control-plane
  
  # Database Configuration
  datasource:
    url: ${POSTGRES_URL:jdbc:postgresql://localhost:5432/minicloud_catalog}
    username: ${POSTGRES_USER:minicloud}
    password: ${POSTGRES_PASSWORD:minicloud123}
    driver-class-name: org.postgresql.Driver
    hikari:
      maximum-pool-size: 20     # Maximum database connections
      minimum-idle: 5           # Minimum idle connections
      connection-timeout: 30000 # Connection timeout (ms)
      idle-timeout: 600000      # Idle timeout (ms)
      max-lifetime: 1800000     # Maximum connection lifetime (ms)
      leak-detection-threshold: 60000  # Connection leak detection (ms)
  
  # JPA Configuration
  jpa:
    hibernate:
      ddl-auto: update          # Database schema management
    show-sql: false             # Show SQL queries in logs
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
        format_sql: true
        use_sql_comments: true
  
  # Jackson Configuration
  jackson:
    serialization:
      write-dates-as-timestamps: false
    deserialization:
      fail-on-unknown-properties: false

# Logging Configuration
logging:
  level:
    com.minicloud: ${LOG_LEVEL:INFO}
    org.apache.iceberg: INFO
    org.apache.calcite: WARN
    org.springframework: WARN
  pattern:
    console: "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n"
    file: "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n"
  file:
    name: logs/minicloud-control-plane.log
    max-size: 100MB
    max-history: 30

# Management Endpoints
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  endpoint:
    health:
      show-details: always
  metrics:
    export:
      prometheus:
        enabled: true
```

### Profile-Specific Configuration

#### Development Profile (`application-development.yml`)

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:minicloud;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE
    driver-class-name: org.h2.Driver
    username: sa
    password: ""
  h2:
    console:
      enabled: true
      path: /h2-console

minicloud:
  iceberg:
    enabled: false              # Disable Iceberg for simple development
  storage:
    type: local                 # Use local file storage
    local:
      base-path: ./data
  workers:
    auto-scale: false           # Disable auto-scaling in development

logging:
  level:
    com.minicloud: DEBUG
    org.springframework.web: DEBUG
```

#### Test Profile (`application-test.yml`)

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE
    driver-class-name: org.h2.Driver
  jpa:
    hibernate:
      ddl-auto: create-drop

minicloud:
  iceberg:
    enabled: true
    catalog:
      type: memory              # Use in-memory catalog for tests
  storage:
    type: memory                # Use in-memory storage for tests
  workers:
    auto-scale: false
    health-check-interval: 5s   # Faster health checks for tests

logging:
  level:
    com.minicloud: DEBUG
    org.apache.iceberg: DEBUG
```

#### Production Profile (`application-production.yml`)

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 50
      minimum-idle: 10
      connection-timeout: 60000
      idle-timeout: 300000
      max-lifetime: 1800000

minicloud:
  iceberg:
    enabled: true
    catalog:
      type: jdbc
      cache:
        l1-size: 50000          # Larger cache for production
        l1-ttl: 600s
        l2-size: 500000
        l2-ttl: 7200s
        l3-enabled: true
  
  storage:
    s3:
      multipart-threshold: 64MB
      multipart-part-size: 16MB
      max-connections: 100
  
  workers:
    auto-scale: true
    min-workers: 5
    max-workers: 100
    scale-up-threshold: 0.7
    scale-down-threshold: 0.2

logging:
  level:
    com.minicloud: INFO
    root: WARN
  file:
    max-size: 500MB
    max-history: 90
```

## Iceberg Configuration

### Core Iceberg Settings

```yaml
minicloud:
  iceberg:
    enabled: true                           # Enable/disable Iceberg integration
    
    # Catalog Configuration
    catalog:
      type: jdbc                            # Catalog type: jdbc, hive, hadoop, memory
      uri: ${POSTGRES_URL}                  # JDBC catalog URI
      warehouse: ${ICEBERG_WAREHOUSE:s3a://iceberg-data/}  # Warehouse location
      
      # Catalog Properties
      properties:
        catalog-impl: org.apache.iceberg.jdbc.JdbcCatalog
        uri: ${POSTGRES_URL}
        warehouse: ${ICEBERG_WAREHOUSE}
        io-impl: org.apache.iceberg.aws.s3.S3FileIO
        s3.endpoint: ${AWS_S3_ENDPOINT:}
        s3.access-key-id: ${AWS_ACCESS_KEY_ID}
        s3.secret-access-key: ${AWS_SECRET_ACCESS_KEY}
        s3.region: ${AWS_REGION:us-east-1}
        s3.path-style-access: ${S3_PATH_STYLE_ACCESS:true}
    
    # Caching Configuration
    cache:
      l1-size: 10000                        # L1 cache size (in-memory)
      l1-ttl: 300s                          # L1 cache TTL
      l2-size: 100000                       # L2 cache size (local disk)
      l2-ttl: 3600s                         # L2 cache TTL
      l3-enabled: true                      # Enable L3 distributed cache
      l3-ttl: 86400s                        # L3 cache TTL
      
    # Table Configuration
    table:
      format: PARQUET                       # Default file format
      compression: ZSTD                     # Compression codec
      row-group-size: 268435456             # Row group size (256MB)
      page-size: 1048576                    # Page size (1MB)
      target-file-size: 536870912           # Target file size (512MB)
      
    # Partitioning Configuration
    partitioning:
      auto-partition: true                  # Enable automatic partitioning
      partition-size-target: 1GB            # Target partition size
      clustering-enabled: true              # Enable data clustering
      
    # Transaction Configuration
    transaction:
      timeout: 300s                         # Transaction timeout
      isolation-level: READ_COMMITTED       # Transaction isolation level
      retry-attempts: 3                     # Retry attempts for failed transactions
      
    # Schema Evolution Configuration
    schema-evolution:
      enabled: true                         # Enable schema evolution
      compatibility-check: true             # Check compatibility before changes
      allow-column-drops: false             # Allow dropping columns
      allow-type-changes: true              # Allow compatible type changes
      
    # Time Travel Configuration
    time-travel:
      enabled: true                         # Enable time travel queries
      max-history-days: 365                 # Maximum history retention
      snapshot-retention-days: 30           # Snapshot retention period
      
    # Maintenance Configuration
    maintenance:
      auto-compact: true                    # Enable automatic compaction
      compact-schedule: "0 2 * * *"         # Compaction schedule (cron)
      auto-expire-snapshots: true           # Enable automatic snapshot expiration
      expire-schedule: "0 3 * * 0"          # Expiration schedule (weekly)
      vacuum-schedule: "0 4 1 * *"          # Vacuum schedule (monthly)
```

### Advanced Iceberg Settings

```yaml
minicloud:
  iceberg:
    # Performance Tuning
    performance:
      vectorized-reader: true               # Enable vectorized reading
      predicate-pushdown: true              # Enable predicate pushdown
      projection-pushdown: true             # Enable projection pushdown
      split-size: 134217728                 # Split size for parallel processing (128MB)
      
    # Statistics Configuration
    statistics:
      enabled: true                         # Enable statistics collection
      auto-collect: true                    # Automatically collect statistics
      collect-schedule: "0 1 * * *"         # Statistics collection schedule
      histogram-enabled: true               # Enable histogram statistics
      
    # Metadata Configuration
    metadata:
      cache-expiry: 3600s                   # Metadata cache expiry
      refresh-interval: 300s                # Metadata refresh interval
      max-manifest-size: 8388608            # Maximum manifest size (8MB)
      
    # Write Configuration
    write:
      batch-size: 1000                      # Write batch size
      buffer-size: 67108864                 # Write buffer size (64MB)
      sort-order: true                      # Enable sort order optimization
      
    # Read Configuration
    read:
      batch-size: 4096                      # Read batch size
      prefetch-enabled: true                # Enable prefetching
      parallel-enabled: true                # Enable parallel reading
```

## Storage Configuration

### S3/MinIO Configuration

```yaml
minicloud:
  storage:
    type: s3                                # Storage type: s3, local, memory
    
    s3:
      endpoint: ${MINIO_ENDPOINT:http://localhost:9000}
      access-key: ${MINIO_ACCESS_KEY:minioadmin}
      secret-key: ${MINIO_SECRET_KEY:minioadmin123}
      region: ${AWS_REGION:us-east-1}
      path-style-access: ${S3_PATH_STYLE_ACCESS:true}
      
      # Bucket Configuration
      buckets:
        data: iceberg-data                  # Data bucket
        metadata: iceberg-metadata          # Metadata bucket
        statistics: iceberg-stats           # Statistics bucket
        logs: iceberg-logs                  # Logs bucket
        
      # Performance Configuration
      multipart-threshold: 64MB             # Multipart upload threshold
      multipart-part-size: 16MB             # Multipart part size
      max-connections: 50                   # Maximum connections
      connection-timeout: 60s               # Connection timeout
      socket-timeout: 60s                   # Socket timeout
      retry-attempts: 3                     # Retry attempts
      
      # Security Configuration
      encryption:
        enabled: false                      # Enable server-side encryption
        algorithm: AES256                   # Encryption algorithm
        kms-key-id: ""                      # KMS key ID (if using KMS)
```

### Local Storage Configuration

```yaml
minicloud:
  storage:
    type: local
    
    local:
      base-path: ${MINICLOUD_DATA_PATH:./data}
      temp-path: ${MINICLOUD_TEMP_PATH:./temp}
      
      # File System Configuration
      permissions:
        file-mode: 644                      # File permissions
        directory-mode: 755                 # Directory permissions
        
      # Cleanup Configuration
      cleanup:
        enabled: true                       # Enable automatic cleanup
        temp-file-ttl: 3600s               # Temporary file TTL
        cleanup-schedule: "0 */6 * * *"     # Cleanup schedule
```

## Query Engine Configuration

```yaml
minicloud:
  query:
    # Execution Configuration
    execution:
      max-concurrent-queries: 100           # Maximum concurrent queries
      query-timeout: 3600s                  # Query timeout
      max-memory-per-query: 2GB             # Maximum memory per query
      spill-enabled: true                   # Enable spilling to disk
      spill-path: ./spill                   # Spill directory
      
    # Optimization Configuration
    optimization:
      cost-based: true                      # Enable cost-based optimization
      statistics-enabled: true              # Use statistics for optimization
      adaptive-execution: true              # Enable adaptive query execution
      vectorized-execution: true            # Enable vectorized execution
      
    # Caching Configuration
    cache:
      result-cache-enabled: true            # Enable result caching
      result-cache-size: 1000               # Result cache size
      result-cache-ttl: 1800s               # Result cache TTL
      plan-cache-enabled: true              # Enable plan caching
      plan-cache-size: 500                  # Plan cache size
      plan-cache-ttl: 3600s                 # Plan cache TTL
      
    # Distributed Execution Configuration
    distributed:
      enabled: true                         # Enable distributed execution
      shuffle-partitions: 200               # Number of shuffle partitions
      broadcast-threshold: 10MB             # Broadcast join threshold
      max-broadcast-size: 8GB               # Maximum broadcast size
```

## Worker Configuration

```yaml
minicloud:
  worker:
    # Basic Configuration
    id: ${WORKER_ID:worker-1}               # Worker ID
    control-plane-endpoints: ${CONTROL_PLANE_ENDPOINTS:localhost:9090}
    
    # Resource Configuration
    resources:
      max-memory: ${MAX_MEMORY_MB:4096}     # Maximum memory (MB)
      max-cpu-cores: ${MAX_CPU_CORES:4}     # Maximum CPU cores
      disk-space: ${MAX_DISK_GB:100}        # Maximum disk space (GB)
      
    # Health Configuration
    health:
      heartbeat-interval: 30s               # Heartbeat interval
      health-check-timeout: 10s             # Health check timeout
      max-missed-heartbeats: 3              # Maximum missed heartbeats
      
    # Task Configuration
    task:
      max-concurrent-tasks: 10              # Maximum concurrent tasks
      task-timeout: 1800s                   # Task timeout
      retry-attempts: 3                     # Task retry attempts
      
    # Auto-scaling Configuration
    auto-scale:
      enabled: true                         # Enable auto-scaling
      min-workers: 2                        # Minimum workers
      max-workers: 50                       # Maximum workers
      scale-up-threshold: 0.8               # Scale up threshold
      scale-down-threshold: 0.3             # Scale down threshold
      scale-up-cooldown: 300s               # Scale up cooldown
      scale-down-cooldown: 600s             # Scale down cooldown
```

## Security Configuration

```yaml
minicloud:
  security:
    # Authentication Configuration
    authentication:
      enabled: true                         # Enable authentication
      type: jwt                             # Authentication type: jwt, oauth2, basic
      
    # JWT Configuration
    jwt:
      secret: ${JWT_SECRET:your-secret-key}
      expiration: 86400                     # Token expiration (seconds)
      refresh-expiration: 604800            # Refresh token expiration
      issuer: minicloud                     # Token issuer
      
    # OAuth2 Configuration
    oauth2:
      enabled: false                        # Enable OAuth2
      providers:
        google:
          client-id: ${GOOGLE_CLIENT_ID}
          client-secret: ${GOOGLE_CLIENT_SECRET}
          redirect-uri: ${GOOGLE_REDIRECT_URI}
        github:
          client-id: ${GITHUB_CLIENT_ID}
          client-secret: ${GITHUB_CLIENT_SECRET}
          
    # Authorization Configuration
    authorization:
      enabled: true                         # Enable authorization
      default-role: user                    # Default user role
      admin-users: ${ADMIN_USERS:admin}     # Admin users (comma-separated)
      
    # API Security Configuration
    api:
      rate-limiting:
        enabled: true                       # Enable rate limiting
        requests-per-minute: 1000           # Requests per minute
        burst-capacity: 100                 # Burst capacity
      cors:
        enabled: true                       # Enable CORS
        allowed-origins: "*"                # Allowed origins
        allowed-methods: ["GET", "POST", "PUT", "DELETE"]
        allowed-headers: ["*"]
        
    # TLS Configuration
    tls:
      enabled: false                        # Enable TLS
      keystore-path: ${TLS_KEYSTORE_PATH}
      keystore-password: ${TLS_KEYSTORE_PASSWORD}
      key-alias: ${TLS_KEY_ALIAS}
      
    # Encryption Configuration
    encryption:
      enabled: false                        # Enable data encryption
      algorithm: AES-256-GCM                # Encryption algorithm
      key-management: local                 # Key management: local, vault, aws-kms
```

## Monitoring Configuration

```yaml
minicloud:
  monitoring:
    # Metrics Configuration
    metrics:
      enabled: true                         # Enable metrics collection
      export-interval: 30s                  # Metrics export interval
      retention-period: 30d                 # Metrics retention period
      
      # Prometheus Configuration
      prometheus:
        enabled: true                       # Enable Prometheus metrics
        path: /actuator/prometheus           # Metrics endpoint path
        
      # Custom Metrics Configuration
      custom:
        query-metrics: true                 # Enable query metrics
        table-metrics: true                 # Enable table metrics
        worker-metrics: true                # Enable worker metrics
        system-metrics: true                # Enable system metrics
        
    # Logging Configuration
    logging:
      level: ${LOG_LEVEL:INFO}              # Log level
      structured: false                     # Enable structured logging
      correlation-id: true                  # Enable correlation IDs
      
      # Log Appenders
      appenders:
        console:
          enabled: true                     # Enable console logging
          pattern: "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n"
        file:
          enabled: true                     # Enable file logging
          path: logs/minicloud.log
          max-size: 100MB
          max-history: 30
        syslog:
          enabled: false                    # Enable syslog
          host: localhost
          port: 514
          
    # Alerting Configuration
    alerting:
      enabled: false                        # Enable alerting
      webhook-url: ${ALERT_WEBHOOK_URL}     # Alert webhook URL
      
      # Alert Rules
      rules:
        high-cpu-usage:
          threshold: 80                     # CPU usage threshold (%)
          duration: 5m                      # Duration before alert
        high-memory-usage:
          threshold: 85                     # Memory usage threshold (%)
          duration: 5m
        query-failure-rate:
          threshold: 10                     # Query failure rate threshold (%)
          duration: 10m
```

## NYC Datasets Configuration

```yaml
minicloud:
  nyc-data:
    enabled: true                           # Enable NYC datasets
    auto-load: false                        # Automatically load datasets on startup
    
    # Dataset Configuration
    datasets:
      taxi-yellow:
        enabled: true                       # Enable yellow taxi dataset
        years: [2020, 2021, 2022, 2023, 2024]  # Years to load
        partition-by: ["year", "month"]     # Partitioning columns
        clustering-by: ["pickup_location_id"]  # Clustering columns
        sample-size: 1000000                # Sample size for development
        
      taxi-green:
        enabled: true
        years: [2020, 2021, 2022, 2023, 2024]
        partition-by: ["year", "month"]
        sample-size: 500000
        
      fhv:
        enabled: true
        years: [2020, 2021, 2022, 2023, 2024]
        partition-by: ["year", "month", "dispatching_base_num"]
        sample-size: 2000000
        
      service-requests:
        enabled: true
        years: [2020, 2021, 2022, 2023, 2024]
        partition-by: ["year", "agency"]
        clustering-by: ["complaint_type"]
        sample-size: 3000000
        
      weather:
        enabled: true
        years: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
        partition-by: ["year", "month"]
        sample-size: 100000
        
    # Data Quality Configuration
    data-quality:
      enabled: true                         # Enable data quality checks
      validation-rules:
        taxi-trips:
          - fare_amount > 0                 # Fare amount validation
          - trip_distance >= 0              # Trip distance validation
          - passenger_count > 0             # Passenger count validation
        service-requests:
          - created_date IS NOT NULL       # Created date validation
          - agency IS NOT NULL             # Agency validation
          
    # Sample Queries Configuration
    sample-queries:
      enabled: true                         # Enable sample queries
      categories:
        - basic-statistics
        - time-series-analysis
        - geospatial-analysis
        - cross-dataset-correlation
```

## Environment Variables

### Core Environment Variables

```bash
# Database Configuration
POSTGRES_URL=jdbc:postgresql://localhost:5432/minicloud_catalog
POSTGRES_USER=minicloud
POSTGRES_PASSWORD=minicloud123

# Iceberg Configuration
ICEBERG_ENABLED=true
ICEBERG_CATALOG_TYPE=jdbc
ICEBERG_WAREHOUSE=s3a://iceberg-data/

# Storage Configuration
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin123
AWS_REGION=us-east-1
AWS_S3_ENDPOINT=http://localhost:9000
S3_PATH_STYLE_ACCESS=true

# Application Configuration
SPRING_PROFILES_ACTIVE=development
LOG_LEVEL=INFO
SERVER_PORT=8080

# Worker Configuration
WORKER_ID=worker-1
CONTROL_PLANE_ENDPOINTS=localhost:9090
MAX_MEMORY_MB=4096
MAX_CPU_CORES=4

# Security Configuration
JWT_SECRET=your-jwt-secret-key-here
ADMIN_USERS=admin

# Monitoring Configuration
METRICS_ENABLED=true
GRAFANA_ADMIN_PASSWORD=admin

# NYC Datasets Configuration
NYC_DATA_ENABLED=true
NYC_DATA_AUTO_LOAD=false
```

### Docker-Specific Environment Variables

```bash
# Docker Configuration
MINICLOUD_DOCKER_ENABLED=true
DOCKER_HOST=unix:///var/run/docker.sock

# Container Configuration
MINICLOUD_DATA_PATH=/data
MINICLOUD_TEMP_PATH=/tmp

# Network Configuration
MINICLOUD_NETWORK=minicloud-network

# Resource Limits
MEMORY_LIMIT=8g
CPU_LIMIT=4
```

## Docker Configuration

### Docker Compose Configuration

```yaml
version: '3.8'

services:
  control-plane:
    build:
      context: .
      dockerfile: minicloud-control-plane/Dockerfile
      target: ${BUILD_TARGET:-development}
      args:
        - JAVA_VERSION=${JAVA_VERSION:-17}
        - MAVEN_VERSION=${MAVEN_VERSION:-3.8}
    environment:
      - SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE:-docker}
      - POSTGRES_URL=${POSTGRES_URL}
      - POSTGRES_USER=${POSTGRES_USER}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - MINIO_ENDPOINT=${MINIO_ENDPOINT}
      - MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}
      - MINIO_SECRET_KEY=${MINIO_SECRET_KEY}
      - ICEBERG_ENABLED=${ICEBERG_ENABLED:-true}
      - ICEBERG_WAREHOUSE=${ICEBERG_WAREHOUSE}
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
      - AWS_REGION=${AWS_REGION:-us-east-1}
      - LOG_LEVEL=${LOG_LEVEL:-INFO}
    volumes:
      - ${DATA_PATH:-./data}:/data
      - ${LOGS_PATH:-./logs}:/app/logs
    ports:
      - "${CONTROL_PLANE_PORT:-8080}:8080"
      - "${CONTROL_PLANE_GRPC_PORT:-9090}:9090"
    networks:
      - minicloud-network
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s
    deploy:
      resources:
        limits:
          memory: ${CONTROL_PLANE_MEMORY:-8g}
          cpus: '${CONTROL_PLANE_CPUS:-4}'
        reservations:
          memory: ${CONTROL_PLANE_MEMORY_RESERVATION:-4g}
          cpus: '${CONTROL_PLANE_CPUS_RESERVATION:-2}'
    restart: unless-stopped
    
networks:
  minicloud-network:
    driver: bridge
    ipam:
      config:
        - subnet: ${NETWORK_SUBNET:-172.20.0.0/16}
```

### Dockerfile Configuration

```dockerfile
# Multi-stage Dockerfile
FROM openjdk:17-jdk-slim as base

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Development stage
FROM base as development
COPY target/*.jar app.jar
EXPOSE 8080 9090
ENTRYPOINT ["java", "-jar", "app.jar"]

# Production stage
FROM base as production
COPY target/*.jar app.jar

# Create non-root user
RUN groupadd -r minicloud && useradd -r -g minicloud minicloud
RUN chown -R minicloud:minicloud /app
USER minicloud

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

EXPOSE 8080 9090
ENTRYPOINT ["java", \
  "-XX:+UseG1GC", \
  "-XX:MaxGCPauseMillis=200", \
  "-XX:+UseStringDeduplication", \
  "-Xms2g", \
  "-Xmx6g", \
  "-jar", "app.jar"]
```

This configuration reference provides comprehensive documentation for all available configuration options in Mini Data Cloud with Iceberg integration.