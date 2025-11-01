# Time Travel Queries in Mini Data Cloud

This document describes how to use time travel query capabilities with Apache Iceberg tables.

## Overview

Time travel queries allow you to query historical versions of your data by specifying either a timestamp or a specific snapshot ID. This is useful for:

- Auditing data changes over time
- Recovering from accidental data modifications
- Analyzing data as it existed at specific points in time
- Comparing data between different time periods

## SQL Syntax

### AS OF TIMESTAMP

Query data as it existed at a specific timestamp:

```sql
SELECT * FROM taxi_trips AS OF TIMESTAMP '2024-01-01 12:00:00';
```

Supported timestamp formats:
- `yyyy-MM-dd HH:mm:ss` (e.g., '2024-01-01 12:00:00')
- `yyyy-MM-dd'T'HH:mm:ss` (e.g., '2024-01-01T12:00:00')
- `yyyy-MM-dd'T'HH:mm:ss.SSS` (e.g., '2024-01-01T12:00:00.123')
- ISO format (e.g., '2024-01-01T12:00:00Z')

### AS OF SNAPSHOT

Query data from a specific snapshot ID:

```sql
SELECT * FROM taxi_trips AS OF SNAPSHOT 1234567890;
```

## REST API Endpoints

### Validate Time Travel Query

```http
POST /api/time-travel/validate
Content-Type: application/json

{
  "sql": "SELECT * FROM taxi_trips AS OF TIMESTAMP '2024-01-01 12:00:00'"
}
```

### Process Time Travel Query

```http
POST /api/time-travel/process
Content-Type: application/json

{
  "sql": "SELECT * FROM taxi_trips AS OF TIMESTAMP '2024-01-01 12:00:00'"
}
```

### List Table Snapshots

```http
GET /api/time-travel/snapshots/{namespace}/{tableName}
```

### Get Snapshot Information

```http
GET /api/time-travel/snapshots/{namespace}/{tableName}/{snapshotId}
```

### Get Snapshot Statistics

```http
GET /api/time-travel/snapshots/{namespace}/{tableName}/statistics
```

### Find Snapshots in Time Range

```http
GET /api/time-travel/snapshots/{namespace}/{tableName}/range?startTime=2024-01-01T00:00:00Z&endTime=2024-01-02T00:00:00Z
```

### Find Nearest Snapshot

```http
GET /api/time-travel/snapshots/{namespace}/{tableName}/nearest?timestamp=2024-01-01T12:00:00Z
```

### Read Snapshot Sample

```http
GET /api/time-travel/data/{namespace}/{tableName}/{snapshotId}/sample?sampleSize=100
```

## Examples

### Basic Time Travel Query

```sql
-- Query taxi trips as they existed on January 1st, 2024
SELECT pickup_location, COUNT(*) as trip_count
FROM taxi_trips AS OF TIMESTAMP '2024-01-01 00:00:00'
GROUP BY pickup_location
ORDER BY trip_count DESC
LIMIT 10;
```

### Comparing Data Between Time Points

```sql
-- Current data
SELECT COUNT(*) as current_count FROM taxi_trips;

-- Data as of last week
SELECT COUNT(*) as last_week_count 
FROM taxi_trips AS OF TIMESTAMP '2024-01-15 00:00:00';
```

### Using Specific Snapshots

```sql
-- Query using a specific snapshot ID
SELECT * FROM taxi_trips AS OF SNAPSHOT 1234567890
WHERE fare_amount > 50.00;
```

## Snapshot Management

### Listing Snapshots

Use the REST API to list all available snapshots for a table:

```bash
curl -X GET "http://localhost:8080/api/time-travel/snapshots/default/taxi_trips"
```

### Snapshot Cleanup Planning

Plan cleanup of old snapshots:

```bash
curl -X GET "http://localhost:8080/api/time-travel/snapshots/default/taxi_trips/cleanup/plan?retentionDays=30"
```

## Limitations

1. **Table Existence**: The table must exist and be an Iceberg table
2. **Snapshot Availability**: The requested timestamp must correspond to an available snapshot
3. **Retention Policy**: Very old snapshots may have been cleaned up based on retention policies
4. **Performance**: Time travel queries may be slower than current data queries

## Error Handling

Common errors and their meanings:

- `Snapshot not found`: The requested snapshot ID doesn't exist
- `No snapshot found for timestamp`: No snapshot exists at or before the specified timestamp
- `Table not found`: The specified table doesn't exist
- `Invalid timestamp format`: The timestamp string is not in a supported format

## Best Practices

1. **Use Recent Timestamps**: Query performance is better for recent snapshots
2. **Check Snapshot Availability**: Use the snapshot listing API to see available snapshots
3. **Consider Retention Policies**: Be aware that old snapshots may be automatically cleaned up
4. **Use Appropriate Filters**: Add WHERE clauses to limit the amount of historical data processed
5. **Monitor Query Performance**: Time travel queries may take longer than regular queries

## Configuration

Time travel functionality is enabled when Iceberg integration is active:

```yaml
minicloud:
  iceberg:
    enabled: true
```

The system automatically manages snapshots and provides time travel capabilities for all Iceberg tables.