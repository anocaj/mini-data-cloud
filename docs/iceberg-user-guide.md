# Apache Iceberg Integration User Guide

## Overview

Mini Data Cloud now includes full Apache Iceberg integration, transforming it into a Snowflake-like distributed data platform with enterprise-grade capabilities including ACID transactions, schema evolution, time travel queries, and advanced metadata management.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Iceberg Table Management](#iceberg-table-management)
3. [Schema Evolution](#schema-evolution)
4. [ACID Transactions](#acid-transactions)
5. [Time Travel Queries](#time-travel-queries)
6. [NYC Datasets](#nyc-datasets)
7. [Performance Optimization](#performance-optimization)
8. [Monitoring and Metrics](#monitoring-and-metrics)
9. [Troubleshooting](#troubleshooting)

## Getting Started

### Prerequisites

- Docker and Docker Compose installed
- At least 8GB RAM available
- Java 17+ (for development)
- Maven 3.8+ (for building from source)

### Quick Start

1. **Start the system:**
   ```bash
   docker compose up -d
   ```

2. **Wait for services to be ready:**
   ```bash
   # Check system health
   curl http://localhost:8080/health
   
   # Verify Iceberg catalog is initialized
   curl http://localhost:8080/api/iceberg/tables
   ```

3. **Load sample data:**
   ```bash
   # Upload CSV data and convert to Iceberg table
   curl -X POST -F "file=@sample-data/bank_transactions.csv" \
        -F "tableName=bank_transactions" \
        -F "format=iceberg" \
        http://localhost:8080/api/data/upload
   ```

4. **Run your first query:**
   ```bash
   curl -X POST -H "Content-Type: application/json" \
        -d '{"sql": "SELECT COUNT(*) FROM bank_transactions"}' \
        http://localhost:8080/api/query/execute
   ```

## Iceberg Table Management

### Creating Tables

#### From CSV Data
```bash
curl -X POST -F "file=@data.csv" \
     -F "tableName=my_table" \
     -F "format=iceberg" \
     http://localhost:8080/api/data/upload
```

#### Programmatically via REST API
```bash
curl -X POST -H "Content-Type: application/json" \
     -d '{
       "tableName": "sales_data",
       "schema": {
         "columns": [
           {"name": "id", "type": "bigint", "nullable": false},
           {"name": "product", "type": "string", "nullable": true},
           {"name": "amount", "type": "decimal(10,2)", "nullable": false},
           {"name": "sale_date", "type": "date", "nullable": false}
         ]
       },
       "partitionBy": ["sale_date"]
     }' \
     http://localhost:8080/api/iceberg/tables
```

### Listing Tables
```bash
# List all Iceberg tables
curl http://localhost:8080/api/iceberg/tables

# Get detailed table information
curl http://localhost:8080/api/iceberg/tables/my_table
```

### Table Properties
```bash
# Get table properties and metadata
curl http://localhost:8080/api/iceberg/tables/my_table/properties

# Update table properties
curl -X PUT -H "Content-Type: application/json" \
     -d '{"write.parquet.compression-codec": "zstd"}' \
     http://localhost:8080/api/iceberg/tables/my_table/properties
```

## Schema Evolution

### Adding Columns
```bash
curl -X POST -H "Content-Type: application/json" \
     -d '{
       "columnName": "customer_segment",
       "columnType": "string",
       "nullable": true,
       "comment": "Customer segmentation category"
     }' \
     http://localhost:8080/api/iceberg/tables/sales_data/schema/add-column
```

### Renaming Columns
```bash
curl -X POST -H "Content-Type: application/json" \
     -d '{
       "oldName": "amount",
       "newName": "sale_amount"
     }' \
     http://localhost:8080/api/iceberg/tables/sales_data/schema/rename-column
```

### Updating Column Types
```bash
curl -X POST -H "Content-Type: application/json" \
     -d '{
       "columnName": "sale_amount",
       "newType": "decimal(12,2)"
     }' \
     http://localhost:8080/api/iceberg/tables/sales_data/schema/update-column
```

### Schema History
```bash
# View schema evolution history
curl http://localhost:8080/api/iceberg/tables/sales_data/schema/history
```

## ACID Transactions

### Transaction Lifecycle

#### Begin Transaction
```bash
curl -X POST -H "Content-Type: application/json" \
     -d '{"tableName": "sales_data"}' \
     http://localhost:8080/api/iceberg/transactions/begin
```

#### Commit Transaction
```bash
curl -X POST \
     http://localhost:8080/api/iceberg/transactions/{transactionId}/commit
```

#### Rollback Transaction
```bash
curl -X POST \
     http://localhost:8080/api/iceberg/transactions/{transactionId}/rollback
```

### Transaction Status
```bash
# Check transaction status
curl http://localhost:8080/api/iceberg/transactions/{transactionId}/status

# List active transactions
curl http://localhost:8080/api/iceberg/transactions/active
```

### Concurrent Operations

Iceberg ensures ACID properties even with concurrent operations:

```sql
-- Multiple users can safely run concurrent operations
-- User 1:
INSERT INTO sales_data VALUES (1, 'Product A', 100.00, '2024-01-01');

-- User 2 (simultaneously):
UPDATE sales_data SET amount = amount * 1.1 WHERE product = 'Product B';

-- User 3 (simultaneously):
DELETE FROM sales_data WHERE sale_date < '2023-01-01';
```

## Time Travel Queries

### Query Historical Data

#### By Timestamp
```sql
-- Query data as it existed at a specific time
SELECT * FROM sales_data AS OF TIMESTAMP '2024-01-01 12:00:00';

-- Compare current vs historical data
SELECT 
  current.product,
  current.total_sales,
  historical.total_sales as historical_sales,
  current.total_sales - historical.total_sales as growth
FROM (
  SELECT product, SUM(amount) as total_sales 
  FROM sales_data 
  GROUP BY product
) current
JOIN (
  SELECT product, SUM(amount) as total_sales 
  FROM sales_data AS OF TIMESTAMP '2024-01-01 00:00:00'
  GROUP BY product
) historical ON current.product = historical.product;
```

#### By Snapshot ID
```sql
-- Query specific snapshot
SELECT * FROM sales_data AS OF SNAPSHOT 1234567890;
```

### Snapshot Management

#### List Snapshots
```bash
curl http://localhost:8080/api/iceberg/tables/sales_data/snapshots
```

#### Snapshot Details
```bash
curl http://localhost:8080/api/iceberg/tables/sales_data/snapshots/{snapshotId}
```

#### Time Travel via REST API
```bash
curl -X POST -H "Content-Type: application/json" \
     -d '{
       "sql": "SELECT COUNT(*) FROM sales_data AS OF TIMESTAMP '\''2024-01-01 12:00:00'\''",
       "timeTravel": true
     }' \
     http://localhost:8080/api/query/execute
```

## NYC Datasets

### Available Datasets

Mini Data Cloud includes comprehensive New York City open datasets for demonstration:

1. **Yellow Taxi Trips** - 100M+ records (2009-2024)
2. **Green Taxi Trips** - 50M+ records with borough-based partitioning
3. **For-Hire Vehicles** - 200M+ Uber/Lyft trips
4. **311 Service Requests** - 30M+ citizen requests
5. **Weather Data** - 10 years of hourly weather data

### Loading NYC Datasets

```bash
# Load all NYC datasets
curl -X POST http://localhost:8080/api/nyc/datasets/load-all

# Load specific dataset
curl -X POST http://localhost:8080/api/nyc/datasets/taxi-yellow/load

# Check loading status
curl http://localhost:8080/api/nyc/datasets/status
```

### Sample Queries

#### Taxi Trip Analysis
```sql
-- Top pickup locations by trip count
SELECT 
  pickup_location_id,
  COUNT(*) as trip_count,
  AVG(fare_amount) as avg_fare,
  AVG(trip_distance) as avg_distance
FROM taxi_yellow 
WHERE pickup_datetime >= '2024-01-01'
GROUP BY pickup_location_id
ORDER BY trip_count DESC
LIMIT 10;

-- Hourly trip patterns
SELECT 
  EXTRACT(HOUR FROM pickup_datetime) as hour,
  COUNT(*) as trips,
  AVG(fare_amount) as avg_fare
FROM taxi_yellow
WHERE pickup_datetime >= '2024-01-01'
  AND pickup_datetime < '2024-02-01'
GROUP BY EXTRACT(HOUR FROM pickup_datetime)
ORDER BY hour;
```

#### 311 Service Request Analysis
```sql
-- Service requests by borough and type
SELECT 
  borough,
  complaint_type,
  COUNT(*) as request_count,
  COUNT(CASE WHEN status = 'Closed' THEN 1 END) as closed_count,
  ROUND(COUNT(CASE WHEN status = 'Closed' THEN 1 END) * 100.0 / COUNT(*), 2) as closure_rate
FROM service_requests_311
WHERE created_date >= '2024-01-01'
GROUP BY borough, complaint_type
HAVING COUNT(*) > 100
ORDER BY request_count DESC;
```

#### Cross-Dataset Analysis
```sql
-- Correlate taxi trips with weather
SELECT 
  w.date,
  w.temperature,
  w.precipitation,
  COUNT(t.trip_id) as trip_count,
  AVG(t.fare_amount) as avg_fare
FROM weather_data w
LEFT JOIN taxi_yellow t ON DATE(t.pickup_datetime) = w.date
WHERE w.date >= '2024-01-01' AND w.date < '2024-02-01'
GROUP BY w.date, w.temperature, w.precipitation
ORDER BY w.date;
```

### Pre-configured Queries

```bash
# Get sample queries for each dataset
curl http://localhost:8080/api/nyc/sample-queries

# Execute a pre-configured query
curl -X POST http://localhost:8080/api/nyc/queries/taxi-hourly-patterns/execute

# Get query documentation
curl http://localhost:8080/api/nyc/queries/documentation
```

## Performance Optimization

### Partitioning Strategies

#### Date-based Partitioning
```sql
-- Optimal for time-series data
CREATE TABLE events (
  event_id BIGINT,
  event_time TIMESTAMP,
  user_id BIGINT,
  event_type STRING
) PARTITIONED BY (DATE(event_time));
```

#### Multi-level Partitioning
```sql
-- For high-volume data with multiple dimensions
CREATE TABLE transactions (
  transaction_id BIGINT,
  user_id BIGINT,
  amount DECIMAL(10,2),
  transaction_date DATE,
  region STRING
) PARTITIONED BY (transaction_date, region);
```

### Query Optimization

#### Predicate Pushdown
```sql
-- Efficient: filters pushed to storage layer
SELECT * FROM taxi_yellow 
WHERE pickup_datetime >= '2024-01-01' 
  AND pickup_datetime < '2024-01-02'
  AND pickup_location_id = 161;
```

#### Projection Pushdown
```sql
-- Efficient: only required columns read
SELECT pickup_location_id, fare_amount 
FROM taxi_yellow 
WHERE pickup_datetime >= '2024-01-01';
```

### Statistics and Cost-Based Optimization

```bash
# Update table statistics
curl -X POST http://localhost:8080/api/iceberg/tables/taxi_yellow/analyze

# View table statistics
curl http://localhost:8080/api/iceberg/tables/taxi_yellow/statistics

# View query execution plans
curl -X POST -H "Content-Type: application/json" \
     -d '{"sql": "SELECT * FROM taxi_yellow WHERE pickup_location_id = 161", "explain": true}' \
     http://localhost:8080/api/query/explain
```

## Monitoring and Metrics

### System Metrics

```bash
# Iceberg-specific metrics
curl http://localhost:8080/api/iceberg/metrics

# Query performance metrics
curl http://localhost:8080/api/iceberg/metrics/queries

# Table-level metrics
curl http://localhost:8080/api/iceberg/metrics/tables

# Transaction metrics
curl http://localhost:8080/api/iceberg/metrics/transactions
```

### Grafana Dashboards

Access pre-built dashboards at http://localhost:3000 (admin/admin):

1. **Iceberg Overview** - System-wide metrics and health
2. **Table Statistics** - Per-table performance and usage
3. **Transaction Monitoring** - ACID transaction metrics
4. **NYC Dataset Analytics** - Sample data insights

### Performance Monitoring

```sql
-- Query execution history
SELECT 
  query_id,
  sql_text,
  execution_time_ms,
  rows_returned,
  status
FROM query_history 
WHERE submitted_at >= CURRENT_DATE - INTERVAL '1' DAY
ORDER BY execution_time_ms DESC;
```

## Troubleshooting

### Common Issues

#### Table Not Found
```bash
# Check if table exists in catalog
curl http://localhost:8080/api/iceberg/tables

# Refresh catalog metadata
curl -X POST http://localhost:8080/api/iceberg/catalog/refresh
```

#### Schema Evolution Errors
```bash
# Check schema compatibility
curl http://localhost:8080/api/iceberg/tables/my_table/schema/validate

# View schema history
curl http://localhost:8080/api/iceberg/tables/my_table/schema/history
```

#### Transaction Conflicts
```bash
# Check active transactions
curl http://localhost:8080/api/iceberg/transactions/active

# Force rollback stuck transaction
curl -X POST http://localhost:8080/api/iceberg/transactions/{transactionId}/force-rollback
```

#### Performance Issues
```bash
# Check table statistics freshness
curl http://localhost:8080/api/iceberg/tables/my_table/statistics/last-updated

# Analyze table for updated statistics
curl -X POST http://localhost:8080/api/iceberg/tables/my_table/analyze

# Check query execution plans
curl -X POST -H "Content-Type: application/json" \
     -d '{"sql": "YOUR_SLOW_QUERY", "explain": true}' \
     http://localhost:8080/api/query/explain
```

### Log Analysis

```bash
# View control plane logs
docker logs minicloud-control-plane

# View worker logs
docker logs minicloud-worker-1

# View database logs
docker logs minicloud-metadata-db
```

### Health Checks

```bash
# System health
curl http://localhost:8080/health

# Iceberg catalog health
curl http://localhost:8080/api/iceberg/health

# Worker health
curl http://localhost:8080/api/workers/health

# Storage health (MinIO)
curl http://localhost:9000/minio/health/live
```

## Advanced Features

### Zero-Copy Cloning
```bash
# Create table clone
curl -X POST -H "Content-Type: application/json" \
     -d '{"sourceTable": "taxi_yellow", "targetTable": "taxi_yellow_backup"}' \
     http://localhost:8080/api/iceberg/tables/clone
```

### Data Compaction
```bash
# Compact table files
curl -X POST http://localhost:8080/api/iceberg/tables/taxi_yellow/compact

# Schedule automatic compaction
curl -X POST -H "Content-Type: application/json" \
     -d '{"schedule": "0 2 * * *", "enabled": true}' \
     http://localhost:8080/api/iceberg/tables/taxi_yellow/compact/schedule
```

### Metadata Management
```bash
# Expire old snapshots
curl -X POST -H "Content-Type: application/json" \
     -d '{"retentionDays": 7}' \
     http://localhost:8080/api/iceberg/tables/taxi_yellow/snapshots/expire

# Vacuum deleted files
curl -X POST http://localhost:8080/api/iceberg/tables/taxi_yellow/vacuum
```

## Best Practices

### Table Design
1. **Choose appropriate partitioning** - Use date/time for time-series data
2. **Optimize file sizes** - Target 100-500MB files for best performance
3. **Use clustering** - Cluster frequently filtered columns
4. **Regular maintenance** - Schedule compaction and snapshot cleanup

### Query Optimization
1. **Filter early** - Apply filters in WHERE clauses
2. **Limit projections** - Select only needed columns
3. **Use appropriate data types** - Choose efficient types for your data
4. **Leverage statistics** - Keep table statistics up to date

### Operational Excellence
1. **Monitor performance** - Use Grafana dashboards
2. **Regular backups** - Implement snapshot retention policies
3. **Test schema changes** - Validate compatibility before applying
4. **Capacity planning** - Monitor storage and compute usage

## API Reference

For complete API documentation, visit:
- REST API: http://localhost:8080/swagger-ui.html
- Iceberg API: http://localhost:8080/api/iceberg/docs
- NYC Datasets API: http://localhost:8080/api/nyc/docs

## Support and Community

- **Documentation**: [docs/](../docs/)
- **Issues**: GitHub Issues
- **Discussions**: GitHub Discussions
- **Examples**: [examples/](../examples/)