# NYC Datasets Tutorial

## Overview

Mini Data Cloud includes comprehensive New York City open datasets that demonstrate real-world data processing capabilities at scale. This tutorial walks through analyzing these datasets using Apache Iceberg's advanced features.

## Available Datasets

### 1. Yellow Taxi Trips (100M+ records)
- **Time Range**: 2009-2024
- **Partitioning**: Year, Month
- **Clustering**: pickup_location_id
- **Key Fields**: pickup/dropoff times, locations, fare amounts, distances

### 2. Green Taxi Trips (50M+ records)
- **Time Range**: 2013-2024
- **Partitioning**: Year, Month
- **Key Fields**: Similar to yellow taxis, serves outer boroughs

### 3. For-Hire Vehicles (200M+ records)
- **Time Range**: 2015-2024
- **Partitioning**: Year, Month, dispatching_base_num
- **Key Fields**: Uber, Lyft, and other app-based rides

### 4. 311 Service Requests (30M+ records)
- **Time Range**: 2010-2024
- **Partitioning**: Year, Agency
- **Clustering**: complaint_type
- **Key Fields**: Citizen complaints, agency responses, locations

### 5. Weather Data (10 years)
- **Time Range**: 2015-2024
- **Partitioning**: Year, Month
- **Key Fields**: Temperature, precipitation, wind, visibility

## Getting Started

### Load Sample Datasets

```bash
# Load all NYC datasets (this may take several minutes)
curl -X POST http://localhost:8080/api/nyc/datasets/load-all

# Or load individual datasets
curl -X POST http://localhost:8080/api/nyc/datasets/taxi-yellow/load
curl -X POST http://localhost:8080/api/nyc/datasets/service-requests/load

# Check loading progress
curl http://localhost:8080/api/nyc/datasets/status
```

### Verify Data Loading

```sql
-- Check record counts
SELECT 'taxi_yellow' as dataset, COUNT(*) as records FROM taxi_yellow
UNION ALL
SELECT 'taxi_green' as dataset, COUNT(*) as records FROM taxi_green
UNION ALL
SELECT 'fhv_trips' as dataset, COUNT(*) as records FROM fhv_trips
UNION ALL
SELECT 'service_requests_311' as dataset, COUNT(*) as records FROM service_requests_311
UNION ALL
SELECT 'weather_data' as dataset, COUNT(*) as records FROM weather_data;
```

## Tutorial 1: Taxi Trip Analysis

### Basic Trip Statistics

```sql
-- Overall taxi statistics for 2024
SELECT 
  COUNT(*) as total_trips,
  AVG(trip_distance) as avg_distance,
  AVG(fare_amount) as avg_fare,
  AVG(total_amount) as avg_total,
  MIN(pickup_datetime) as earliest_trip,
  MAX(pickup_datetime) as latest_trip
FROM taxi_yellow 
WHERE YEAR(pickup_datetime) = 2024;
```

### Hourly Trip Patterns

```sql
-- Trip patterns by hour of day
SELECT 
  EXTRACT(HOUR FROM pickup_datetime) as hour,
  COUNT(*) as trip_count,
  AVG(fare_amount) as avg_fare,
  AVG(trip_distance) as avg_distance,
  AVG(passenger_count) as avg_passengers
FROM taxi_yellow
WHERE pickup_datetime >= '2024-01-01'
  AND pickup_datetime < '2024-02-01'
GROUP BY EXTRACT(HOUR FROM pickup_datetime)
ORDER BY hour;
```

### Popular Routes

```sql
-- Top 20 pickup-dropoff location pairs
SELECT 
  pickup_location_id,
  dropoff_location_id,
  COUNT(*) as trip_count,
  AVG(fare_amount) as avg_fare,
  AVG(trip_distance) as avg_distance,
  AVG(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60) as avg_duration_minutes
FROM taxi_yellow
WHERE pickup_datetime >= '2024-01-01'
  AND pickup_datetime < '2024-04-01'
  AND pickup_location_id != dropoff_location_id
GROUP BY pickup_location_id, dropoff_location_id
HAVING COUNT(*) > 100
ORDER BY trip_count DESC
LIMIT 20;
```

### Seasonal Analysis

```sql
-- Monthly trip trends with year-over-year comparison
SELECT 
  EXTRACT(YEAR FROM pickup_datetime) as year,
  EXTRACT(MONTH FROM pickup_datetime) as month,
  COUNT(*) as trips,
  AVG(fare_amount) as avg_fare,
  SUM(fare_amount) as total_revenue
FROM taxi_yellow
WHERE pickup_datetime >= '2022-01-01'
GROUP BY EXTRACT(YEAR FROM pickup_datetime), EXTRACT(MONTH FROM pickup_datetime)
ORDER BY year, month;
```

## Tutorial 2: 311 Service Request Analysis

### Service Request Overview

```sql
-- Service requests by borough and status
SELECT 
  borough,
  status,
  COUNT(*) as request_count,
  AVG(EXTRACT(EPOCH FROM (closed_date - created_date))/86400) as avg_resolution_days
FROM service_requests_311
WHERE created_date >= '2024-01-01'
  AND borough IS NOT NULL
GROUP BY borough, status
ORDER BY borough, request_count DESC;
```

### Agency Performance

```sql
-- Agency response times and resolution rates
SELECT 
  agency,
  complaint_type,
  COUNT(*) as total_requests,
  COUNT(CASE WHEN status = 'Closed' THEN 1 END) as closed_requests,
  ROUND(COUNT(CASE WHEN status = 'Closed' THEN 1 END) * 100.0 / COUNT(*), 2) as closure_rate,
  AVG(CASE 
    WHEN closed_date IS NOT NULL 
    THEN EXTRACT(EPOCH FROM (closed_date - created_date))/86400 
  END) as avg_resolution_days
FROM service_requests_311
WHERE created_date >= '2024-01-01'
  AND agency IS NOT NULL
GROUP BY agency, complaint_type
HAVING COUNT(*) > 50
ORDER BY total_requests DESC;
```

### Complaint Patterns

```sql
-- Most common complaints by time of year
SELECT 
  EXTRACT(MONTH FROM created_date) as month,
  complaint_type,
  COUNT(*) as complaint_count,
  RANK() OVER (PARTITION BY EXTRACT(MONTH FROM created_date) ORDER BY COUNT(*) DESC) as rank_in_month
FROM service_requests_311
WHERE created_date >= '2024-01-01'
  AND complaint_type IS NOT NULL
GROUP BY EXTRACT(MONTH FROM created_date), complaint_type
HAVING COUNT(*) > 20
ORDER BY month, rank_in_month;
```

## Tutorial 3: Cross-Dataset Analysis

### Taxi Trips vs Weather

```sql
-- Impact of weather on taxi usage
SELECT 
  w.date,
  w.temperature,
  w.precipitation,
  w.wind_speed,
  COUNT(t.trip_id) as taxi_trips,
  AVG(t.fare_amount) as avg_fare,
  CASE 
    WHEN w.precipitation > 0.1 THEN 'Rainy'
    WHEN w.temperature < 32 THEN 'Cold'
    WHEN w.temperature > 80 THEN 'Hot'
    ELSE 'Normal'
  END as weather_condition
FROM weather_data w
LEFT JOIN taxi_yellow t ON DATE(t.pickup_datetime) = w.date
WHERE w.date >= '2024-01-01' AND w.date < '2024-04-01'
GROUP BY w.date, w.temperature, w.precipitation, w.wind_speed
ORDER BY w.date;
```

### Service Requests vs Weather

```sql
-- Weather impact on 311 service requests
SELECT 
  weather_condition,
  complaint_category,
  AVG(daily_requests) as avg_daily_requests,
  COUNT(*) as days_observed
FROM (
  SELECT 
    DATE(sr.created_date) as date,
    CASE 
      WHEN w.precipitation > 0.1 THEN 'Rainy'
      WHEN w.temperature < 32 THEN 'Cold'
      WHEN w.temperature > 80 THEN 'Hot'
      ELSE 'Normal'
    END as weather_condition,
    CASE 
      WHEN sr.complaint_type LIKE '%Noise%' THEN 'Noise'
      WHEN sr.complaint_type LIKE '%Heat%' OR sr.complaint_type LIKE '%Water%' THEN 'Utilities'
      WHEN sr.complaint_type LIKE '%Street%' OR sr.complaint_type LIKE '%Traffic%' THEN 'Transportation'
      ELSE 'Other'
    END as complaint_category,
    COUNT(*) as daily_requests
  FROM service_requests_311 sr
  JOIN weather_data w ON DATE(sr.created_date) = w.date
  WHERE sr.created_date >= '2024-01-01' AND sr.created_date < '2024-04-01'
  GROUP BY DATE(sr.created_date), weather_condition, complaint_category
) daily_stats
GROUP BY weather_condition, complaint_category
ORDER BY weather_condition, avg_daily_requests DESC;
```

### Transportation Mode Comparison

```sql
-- Compare yellow taxi, green taxi, and FHV usage patterns
SELECT 
  DATE(pickup_datetime) as date,
  'Yellow Taxi' as service_type,
  COUNT(*) as trips,
  AVG(fare_amount) as avg_fare
FROM taxi_yellow
WHERE pickup_datetime >= '2024-01-01' AND pickup_datetime < '2024-02-01'
GROUP BY DATE(pickup_datetime)

UNION ALL

SELECT 
  DATE(pickup_datetime) as date,
  'Green Taxi' as service_type,
  COUNT(*) as trips,
  AVG(fare_amount) as avg_fare
FROM taxi_green
WHERE pickup_datetime >= '2024-01-01' AND pickup_datetime < '2024-02-01'
GROUP BY DATE(pickup_datetime)

UNION ALL

SELECT 
  DATE(pickup_datetime) as date,
  'For-Hire Vehicle' as service_type,
  COUNT(*) as trips,
  NULL as avg_fare  -- FHV data doesn't include fare
FROM fhv_trips
WHERE pickup_datetime >= '2024-01-01' AND pickup_datetime < '2024-02-01'
GROUP BY DATE(pickup_datetime)

ORDER BY date, service_type;
```

## Tutorial 4: Time Travel Analysis

### Historical Data Comparison

```sql
-- Compare current vs historical taxi usage
WITH current_stats AS (
  SELECT 
    pickup_location_id,
    COUNT(*) as current_trips,
    AVG(fare_amount) as current_avg_fare
  FROM taxi_yellow
  WHERE pickup_datetime >= '2024-01-01' AND pickup_datetime < '2024-02-01'
  GROUP BY pickup_location_id
),
historical_stats AS (
  SELECT 
    pickup_location_id,
    COUNT(*) as historical_trips,
    AVG(fare_amount) as historical_avg_fare
  FROM taxi_yellow AS OF TIMESTAMP '2023-01-15 00:00:00'
  WHERE pickup_datetime >= '2023-01-01' AND pickup_datetime < '2023-02-01'
  GROUP BY pickup_location_id
)
SELECT 
  c.pickup_location_id,
  c.current_trips,
  h.historical_trips,
  c.current_trips - h.historical_trips as trip_change,
  ROUND((c.current_trips - h.historical_trips) * 100.0 / h.historical_trips, 2) as pct_change,
  c.current_avg_fare,
  h.historical_avg_fare,
  c.current_avg_fare - h.historical_avg_fare as fare_change
FROM current_stats c
JOIN historical_stats h ON c.pickup_location_id = h.pickup_location_id
WHERE h.historical_trips > 100  -- Filter for statistical significance
ORDER BY ABS(pct_change) DESC
LIMIT 20;
```

### Data Quality Audit

```sql
-- Track data quality changes over time
SELECT 
  snapshot_timestamp,
  COUNT(*) as total_records,
  COUNT(CASE WHEN fare_amount IS NULL THEN 1 END) as null_fares,
  COUNT(CASE WHEN trip_distance < 0 THEN 1 END) as negative_distances,
  COUNT(CASE WHEN passenger_count = 0 THEN 1 END) as zero_passengers,
  AVG(fare_amount) as avg_fare
FROM (
  SELECT *, CURRENT_TIMESTAMP as snapshot_timestamp FROM taxi_yellow
  UNION ALL
  SELECT *, '2024-01-01 00:00:00' as snapshot_timestamp 
  FROM taxi_yellow AS OF TIMESTAMP '2024-01-01 00:00:00'
  UNION ALL
  SELECT *, '2023-01-01 00:00:00' as snapshot_timestamp 
  FROM taxi_yellow AS OF TIMESTAMP '2023-01-01 00:00:00'
) historical_data
WHERE pickup_datetime >= '2023-01-01'
GROUP BY snapshot_timestamp
ORDER BY snapshot_timestamp;
```

## Tutorial 5: Advanced Analytics

### Geospatial Analysis

```sql
-- Analyze trip patterns by location zones
WITH location_stats AS (
  SELECT 
    pickup_location_id,
    COUNT(*) as pickup_count,
    AVG(fare_amount) as avg_pickup_fare,
    AVG(trip_distance) as avg_pickup_distance
  FROM taxi_yellow
  WHERE pickup_datetime >= '2024-01-01'
  GROUP BY pickup_location_id
),
dropoff_stats AS (
  SELECT 
    dropoff_location_id,
    COUNT(*) as dropoff_count,
    AVG(fare_amount) as avg_dropoff_fare
  FROM taxi_yellow
  WHERE pickup_datetime >= '2024-01-01'
  GROUP BY dropoff_location_id
)
SELECT 
  COALESCE(p.pickup_location_id, d.dropoff_location_id) as location_id,
  COALESCE(p.pickup_count, 0) as pickups,
  COALESCE(d.dropoff_count, 0) as dropoffs,
  COALESCE(p.pickup_count, 0) + COALESCE(d.dropoff_count, 0) as total_activity,
  COALESCE(p.avg_pickup_fare, 0) as avg_pickup_fare,
  COALESCE(d.avg_dropoff_fare, 0) as avg_dropoff_fare,
  COALESCE(p.avg_pickup_distance, 0) as avg_distance
FROM location_stats p
FULL OUTER JOIN dropoff_stats d ON p.pickup_location_id = d.dropoff_location_id
ORDER BY total_activity DESC
LIMIT 50;
```

### Predictive Analytics Preparation

```sql
-- Create features for machine learning models
SELECT 
  pickup_location_id,
  dropoff_location_id,
  EXTRACT(HOUR FROM pickup_datetime) as pickup_hour,
  EXTRACT(DOW FROM pickup_datetime) as day_of_week,
  EXTRACT(MONTH FROM pickup_datetime) as month,
  passenger_count,
  trip_distance,
  fare_amount,
  -- Weather features
  w.temperature,
  w.precipitation,
  w.wind_speed,
  -- Derived features
  CASE WHEN EXTRACT(DOW FROM pickup_datetime) IN (0, 6) THEN 1 ELSE 0 END as is_weekend,
  CASE WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 7 AND 9 THEN 1 ELSE 0 END as is_morning_rush,
  CASE WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 17 AND 19 THEN 1 ELSE 0 END as is_evening_rush,
  -- Target variable
  total_amount
FROM taxi_yellow t
LEFT JOIN weather_data w ON DATE(t.pickup_datetime) = w.date
WHERE t.pickup_datetime >= '2024-01-01'
  AND t.pickup_datetime < '2024-04-01'
  AND t.fare_amount > 0
  AND t.trip_distance > 0
  AND t.passenger_count > 0;
```

## Performance Optimization Tips

### 1. Leverage Partitioning

```sql
-- Efficient: Uses partition pruning
SELECT * FROM taxi_yellow 
WHERE pickup_datetime >= '2024-01-01' 
  AND pickup_datetime < '2024-01-02';

-- Inefficient: Scans all partitions
SELECT * FROM taxi_yellow 
WHERE fare_amount > 50;
```

### 2. Use Clustering Columns

```sql
-- Efficient: Uses clustering on pickup_location_id
SELECT * FROM taxi_yellow 
WHERE pickup_location_id = 161;

-- Less efficient: No clustering benefit
SELECT * FROM taxi_yellow 
WHERE dropoff_location_id = 161;
```

### 3. Optimize Joins

```sql
-- Efficient: Join on partitioned columns
SELECT t.*, w.temperature
FROM taxi_yellow t
JOIN weather_data w ON DATE(t.pickup_datetime) = w.date
WHERE t.pickup_datetime >= '2024-01-01';
```

### 4. Use Appropriate Aggregations

```sql
-- Efficient: Pre-aggregated daily stats
WITH daily_stats AS (
  SELECT 
    DATE(pickup_datetime) as date,
    pickup_location_id,
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare
  FROM taxi_yellow
  WHERE pickup_datetime >= '2024-01-01'
  GROUP BY DATE(pickup_datetime), pickup_location_id
)
SELECT * FROM daily_stats WHERE trips > 100;
```

## Monitoring and Troubleshooting

### Check Query Performance

```bash
# Get query execution metrics
curl http://localhost:8080/api/iceberg/metrics/queries

# Explain query execution plan
curl -X POST -H "Content-Type: application/json" \
     -d '{"sql": "YOUR_QUERY_HERE", "explain": true}' \
     http://localhost:8080/api/query/explain
```

### Monitor Table Statistics

```bash
# Check table statistics freshness
curl http://localhost:8080/api/iceberg/tables/taxi_yellow/statistics

# Update statistics if needed
curl -X POST http://localhost:8080/api/iceberg/tables/taxi_yellow/analyze
```

### View Grafana Dashboards

Access http://localhost:3000 with admin/admin to view:
- NYC Dataset Analytics Dashboard
- Query Performance Metrics
- System Resource Utilization

## Next Steps

1. **Explore More Datasets**: Load additional NYC open datasets
2. **Build Dashboards**: Create custom visualizations in Grafana
3. **Implement ETL**: Set up automated data pipelines
4. **Machine Learning**: Use the prepared features for predictive models
5. **Real-time Analytics**: Implement streaming analytics on live data

## Resources

- [NYC Open Data Portal](https://opendata.cityofnewyork.us/)
- [Taxi & Limousine Commission](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [311 Service Requests](https://nyc.gov/311)
- [Weather Data Sources](https://www.weather.gov/)

## Sample Queries Collection

For more example queries, check:
```bash
curl http://localhost:8080/api/nyc/sample-queries
```

This returns a comprehensive collection of pre-built queries for each dataset, including:
- Basic statistics and summaries
- Time-series analysis
- Geospatial patterns
- Cross-dataset correlations
- Performance benchmarks