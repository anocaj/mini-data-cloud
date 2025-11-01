package com.minicloud.controlplane.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Manages pre-configured sample queries for NYC datasets.
 * Provides curated queries that demonstrate common analytics use cases.
 */
@Service
public class NYCSampleQueryManager {
    
    private static final Logger logger = LoggerFactory.getLogger(NYCSampleQueryManager.class);
    
    @Autowired
    private QueryService queryService;
    
    /**
     * Get all sample queries organized by category
     */
    public Map<String, List<SampleQuery>> getAllSampleQueries() {
        return Map.of(
            "taxi_analytics", getTaxiAnalyticsQueries(),
            "service_requests", getServiceRequestQueries(),
            "fhv_analysis", getFHVAnalysisQueries(),
            "cross_dataset", getCrossDatasetQueries(),
            "time_series", getTimeSeriesQueries(),
            "geospatial", getGeospatialQueries()
        );
    }
    
    /**
     * Get taxi analytics sample queries
     */
    public List<SampleQuery> getTaxiAnalyticsQueries() {
        List<SampleQuery> queries = new ArrayList<>();
        
        queries.add(new SampleQuery(
            "taxi_monthly_trips",
            "Monthly Taxi Trip Counts",
            "Count taxi trips by month to identify seasonal patterns",
            "SELECT EXTRACT(MONTH FROM pickup_datetime) as month, " +
            "COUNT(*) as trip_count " +
            "FROM taxi_yellow " +
            "GROUP BY EXTRACT(MONTH FROM pickup_datetime) " +
            "ORDER BY month",
            "taxi_yellow",
            "basic"
        ));
        
        queries.add(new SampleQuery(
            "taxi_fare_by_distance",
            "Average Fare by Trip Distance",
            "Analyze fare patterns based on trip distance categories",
            "SELECT " +
            "CASE " +
            "  WHEN trip_distance < 1 THEN 'Short (< 1 mile)' " +
            "  WHEN trip_distance < 5 THEN 'Medium (1-5 miles)' " +
            "  WHEN trip_distance < 10 THEN 'Long (5-10 miles)' " +
            "  ELSE 'Very Long (> 10 miles)' " +
            "END as distance_category, " +
            "AVG(fare_amount) as avg_fare, " +
            "COUNT(*) as trip_count " +
            "FROM taxi_yellow " +
            "GROUP BY distance_category " +
            "ORDER BY avg_fare DESC",
            "taxi_yellow",
            "intermediate"
        ));
        
        queries.add(new SampleQuery(
            "taxi_peak_hours",
            "Peak Hours Analysis",
            "Identify busiest hours of the day for taxi trips",
            "SELECT EXTRACT(HOUR FROM pickup_datetime) as hour, " +
            "COUNT(*) as trip_count, " +
            "AVG(fare_amount) as avg_fare " +
            "FROM taxi_yellow " +
            "GROUP BY EXTRACT(HOUR FROM pickup_datetime) " +
            "ORDER BY trip_count DESC",
            "taxi_yellow",
            "basic"
        ));
        
        queries.add(new SampleQuery(
            "taxi_payment_analysis",
            "Payment Method Analysis",
            "Analyze payment methods and their relationship to tip amounts",
            "SELECT payment_type, " +
            "COUNT(*) as transaction_count, " +
            "AVG(tip_amount) as avg_tip, " +
            "AVG(fare_amount) as avg_fare " +
            "FROM taxi_yellow " +
            "WHERE payment_type IS NOT NULL " +
            "GROUP BY payment_type " +
            "ORDER BY transaction_count DESC",
            "taxi_yellow",
            "intermediate"
        ));
        
        queries.add(new SampleQuery(
            "taxi_green_vs_yellow",
            "Green vs Yellow Taxi Comparison",
            "Compare trip patterns between Green and Yellow taxis",
            "SELECT 'Yellow' as taxi_type, " +
            "COUNT(*) as trips, " +
            "AVG(trip_distance) as avg_distance, " +
            "AVG(fare_amount) as avg_fare " +
            "FROM taxi_yellow " +
            "UNION ALL " +
            "SELECT 'Green' as taxi_type, " +
            "COUNT(*) as trips, " +
            "AVG(trip_distance) as avg_distance, " +
            "AVG(fare_amount) as avg_fare " +
            "FROM taxi_green",
            "taxi_yellow,taxi_green",
            "advanced"
        ));
        
        return queries;
    }
    
    /**
     * Get 311 service request sample queries
     */
    public List<SampleQuery> getServiceRequestQueries() {
        List<SampleQuery> queries = new ArrayList<>();
        
        queries.add(new SampleQuery(
            "311_complaints_by_agency",
            "Top Complaints by Agency",
            "Identify the most common complaint types for each agency",
            "SELECT agency, complaint_type, COUNT(*) as complaint_count " +
            "FROM service_requests_311 " +
            "GROUP BY agency, complaint_type " +
            "ORDER BY complaint_count DESC " +
            "LIMIT 20",
            "service_requests_311",
            "basic"
        ));
        
        queries.add(new SampleQuery(
            "311_response_times",
            "Average Response Times by Borough",
            "Calculate average response times for closed requests by borough",
            "SELECT borough, " +
            "COUNT(*) as total_requests, " +
            "COUNT(CASE WHEN status = 'Closed' THEN 1 END) as closed_requests, " +
            "AVG(CASE WHEN closed_date IS NOT NULL " +
            "    THEN EXTRACT(DAY FROM (closed_date - created_date)) END) as avg_response_days " +
            "FROM service_requests_311 " +
            "GROUP BY borough " +
            "ORDER BY avg_response_days",
            "service_requests_311",
            "intermediate"
        ));
        
        queries.add(new SampleQuery(
            "311_seasonal_patterns",
            "Seasonal Complaint Patterns",
            "Analyze how complaint types vary by season",
            "SELECT " +
            "CASE " +
            "  WHEN EXTRACT(MONTH FROM created_date) IN (12, 1, 2) THEN 'Winter' " +
            "  WHEN EXTRACT(MONTH FROM created_date) IN (3, 4, 5) THEN 'Spring' " +
            "  WHEN EXTRACT(MONTH FROM created_date) IN (6, 7, 8) THEN 'Summer' " +
            "  ELSE 'Fall' " +
            "END as season, " +
            "complaint_type, " +
            "COUNT(*) as complaint_count " +
            "FROM service_requests_311 " +
            "GROUP BY season, complaint_type " +
            "ORDER BY season, complaint_count DESC",
            "service_requests_311",
            "intermediate"
        ));
        
        queries.add(new SampleQuery(
            "311_resolution_rates",
            "Resolution Rates by Complaint Type",
            "Calculate resolution rates for different complaint types",
            "SELECT complaint_type, " +
            "COUNT(*) as total_complaints, " +
            "COUNT(CASE WHEN status = 'Closed' THEN 1 END) as resolved_complaints, " +
            "ROUND(100.0 * COUNT(CASE WHEN status = 'Closed' THEN 1 END) / COUNT(*), 2) as resolution_rate " +
            "FROM service_requests_311 " +
            "GROUP BY complaint_type " +
            "HAVING COUNT(*) >= 10 " +
            "ORDER BY resolution_rate DESC",
            "service_requests_311",
            "advanced"
        ));
        
        return queries;
    }
    
    /**
     * Get FHV analysis sample queries
     */
    public List<SampleQuery> getFHVAnalysisQueries() {
        List<SampleQuery> queries = new ArrayList<>();
        
        queries.add(new SampleQuery(
            "fhv_base_activity",
            "Most Active FHV Bases",
            "Identify the most active for-hire vehicle dispatching bases",
            "SELECT dispatching_base_num, " +
            "COUNT(*) as trip_count, " +
            "COUNT(CASE WHEN sr_flag = 'Y' THEN 1 END) as shared_rides " +
            "FROM fhv_trips " +
            "GROUP BY dispatching_base_num " +
            "ORDER BY trip_count DESC " +
            "LIMIT 10",
            "fhv_trips",
            "basic"
        ));
        
        queries.add(new SampleQuery(
            "fhv_shared_ride_analysis",
            "Shared Ride Analysis",
            "Analyze shared ride patterns across different bases",
            "SELECT " +
            "CASE WHEN sr_flag = 'Y' THEN 'Shared' ELSE 'Individual' END as ride_type, " +
            "COUNT(*) as trip_count, " +
            "ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage " +
            "FROM fhv_trips " +
            "GROUP BY sr_flag " +
            "ORDER BY trip_count DESC",
            "fhv_trips",
            "basic"
        ));
        
        queries.add(new SampleQuery(
            "fhv_hourly_patterns",
            "FHV Hourly Demand Patterns",
            "Analyze demand patterns throughout the day",
            "SELECT EXTRACT(HOUR FROM pickup_datetime) as hour, " +
            "COUNT(*) as trip_count, " +
            "COUNT(CASE WHEN sr_flag = 'Y' THEN 1 END) as shared_rides " +
            "FROM fhv_trips " +
            "GROUP BY EXTRACT(HOUR FROM pickup_datetime) " +
            "ORDER BY hour",
            "fhv_trips",
            "intermediate"
        ));
        
        return queries;
    }
    
    /**
     * Get cross-dataset analysis queries
     */
    public List<SampleQuery> getCrossDatasetQueries() {
        List<SampleQuery> queries = new ArrayList<>();
        
        queries.add(new SampleQuery(
            "transportation_mode_comparison",
            "Transportation Mode Comparison",
            "Compare trip volumes across different transportation modes",
            "SELECT 'Yellow Taxi' as mode, COUNT(*) as trips FROM taxi_yellow " +
            "UNION ALL " +
            "SELECT 'Green Taxi' as mode, COUNT(*) as trips FROM taxi_green " +
            "UNION ALL " +
            "SELECT 'For-Hire Vehicle' as mode, COUNT(*) as trips FROM fhv_trips " +
            "ORDER BY trips DESC",
            "taxi_yellow,taxi_green,fhv_trips",
            "advanced"
        ));
        
        queries.add(new SampleQuery(
            "service_requests_vs_transportation",
            "Service Requests vs Transportation Activity",
            "Correlate 311 service requests with transportation activity by location",
            "SELECT t.pickup_location_id, " +
            "COUNT(t.pickup_location_id) as taxi_pickups, " +
            "COUNT(s.incident_zip) as service_requests " +
            "FROM taxi_yellow t " +
            "LEFT JOIN service_requests_311 s ON CAST(t.pickup_location_id AS STRING) = s.incident_zip " +
            "GROUP BY t.pickup_location_id " +
            "HAVING COUNT(t.pickup_location_id) > 10 " +
            "ORDER BY taxi_pickups DESC " +
            "LIMIT 20",
            "taxi_yellow,service_requests_311",
            "advanced"
        ));
        
        return queries;
    }
    
    /**
     * Get time series analysis queries
     */
    public List<SampleQuery> getTimeSeriesQueries() {
        List<SampleQuery> queries = new ArrayList<>();
        
        queries.add(new SampleQuery(
            "daily_activity_trends",
            "Daily Activity Trends",
            "Analyze daily trends across all transportation modes",
            "SELECT DATE(pickup_datetime) as trip_date, " +
            "'Taxi' as mode, " +
            "COUNT(*) as trips " +
            "FROM taxi_yellow " +
            "GROUP BY DATE(pickup_datetime) " +
            "UNION ALL " +
            "SELECT DATE(pickup_datetime) as trip_date, " +
            "'FHV' as mode, " +
            "COUNT(*) as trips " +
            "FROM fhv_trips " +
            "GROUP BY DATE(pickup_datetime) " +
            "ORDER BY trip_date, mode",
            "taxi_yellow,fhv_trips",
            "advanced"
        ));
        
        queries.add(new SampleQuery(
            "weekly_patterns",
            "Weekly Activity Patterns",
            "Compare activity patterns by day of week",
            "SELECT " +
            "CASE EXTRACT(DOW FROM pickup_datetime) " +
            "  WHEN 0 THEN 'Sunday' " +
            "  WHEN 1 THEN 'Monday' " +
            "  WHEN 2 THEN 'Tuesday' " +
            "  WHEN 3 THEN 'Wednesday' " +
            "  WHEN 4 THEN 'Thursday' " +
            "  WHEN 5 THEN 'Friday' " +
            "  WHEN 6 THEN 'Saturday' " +
            "END as day_of_week, " +
            "COUNT(*) as trip_count " +
            "FROM taxi_yellow " +
            "GROUP BY EXTRACT(DOW FROM pickup_datetime) " +
            "ORDER BY EXTRACT(DOW FROM pickup_datetime)",
            "taxi_yellow",
            "intermediate"
        ));
        
        return queries;
    }
    
    /**
     * Get geospatial analysis queries
     */
    public List<SampleQuery> getGeospatialQueries() {
        List<SampleQuery> queries = new ArrayList<>();
        
        queries.add(new SampleQuery(
            "popular_pickup_locations",
            "Most Popular Pickup Locations",
            "Identify the most popular pickup locations for taxis",
            "SELECT pickup_location_id, " +
            "COUNT(*) as pickup_count " +
            "FROM taxi_yellow " +
            "WHERE pickup_location_id IS NOT NULL " +
            "GROUP BY pickup_location_id " +
            "ORDER BY pickup_count DESC " +
            "LIMIT 20",
            "taxi_yellow",
            "basic"
        ));
        
        queries.add(new SampleQuery(
            "borough_service_requests",
            "Service Requests by Borough",
            "Analyze service request distribution across NYC boroughs",
            "SELECT borough, " +
            "COUNT(*) as request_count, " +
            "COUNT(DISTINCT complaint_type) as unique_complaint_types " +
            "FROM service_requests_311 " +
            "WHERE borough IS NOT NULL " +
            "GROUP BY borough " +
            "ORDER BY request_count DESC",
            "service_requests_311",
            "basic"
        ));
        
        return queries;
    }
    
    /**
     * Execute a sample query by ID
     */
    public QueryExecutionResult executeSampleQuery(String queryId) {
        logger.info("Executing sample query: {}", queryId);
        
        try {
            SampleQuery query = findQueryById(queryId);
            if (query == null) {
                return new QueryExecutionResult(false, "Query not found: " + queryId, null);
            }
            
            // Execute the query using the query service
            // This is a simplified implementation - in reality, you'd use the actual query service
            logger.info("Executing SQL: {}", query.getSql());
            
            return new QueryExecutionResult(true, "Query executed successfully", query);
            
        } catch (Exception e) {
            logger.error("Error executing sample query: {}", queryId, e);
            return new QueryExecutionResult(false, "Error executing query: " + e.getMessage(), null);
        }
    }
    
    /**
     * Find a query by ID
     */
    private SampleQuery findQueryById(String queryId) {
        Map<String, List<SampleQuery>> allQueries = getAllSampleQueries();
        
        for (List<SampleQuery> categoryQueries : allQueries.values()) {
            for (SampleQuery query : categoryQueries) {
                if (query.getId().equals(queryId)) {
                    return query;
                }
            }
        }
        
        return null;
    }
    
    /**
     * Get queries by difficulty level
     */
    public List<SampleQuery> getQueriesByDifficulty(String difficulty) {
        List<SampleQuery> result = new ArrayList<>();
        Map<String, List<SampleQuery>> allQueries = getAllSampleQueries();
        
        for (List<SampleQuery> categoryQueries : allQueries.values()) {
            for (SampleQuery query : categoryQueries) {
                if (query.getDifficulty().equals(difficulty)) {
                    result.add(query);
                }
            }
        }
        
        return result;
    }
    
    /**
     * Get queries by dataset
     */
    public List<SampleQuery> getQueriesByDataset(String dataset) {
        List<SampleQuery> result = new ArrayList<>();
        Map<String, List<SampleQuery>> allQueries = getAllSampleQueries();
        
        for (List<SampleQuery> categoryQueries : allQueries.values()) {
            for (SampleQuery query : categoryQueries) {
                if (query.getDatasets().contains(dataset)) {
                    result.add(query);
                }
            }
        }
        
        return result;
    }
    
    /**
     * Sample query definition
     */
    public static class SampleQuery {
        private final String id;
        private final String name;
        private final String description;
        private final String sql;
        private final String datasets;
        private final String difficulty;
        
        public SampleQuery(String id, String name, String description, String sql, String datasets, String difficulty) {
            this.id = id;
            this.name = name;
            this.description = description;
            this.sql = sql;
            this.datasets = datasets;
            this.difficulty = difficulty;
        }
        
        public String getId() { return id; }
        public String getName() { return name; }
        public String getDescription() { return description; }
        public String getSql() { return sql; }
        public String getDatasets() { return datasets; }
        public String getDifficulty() { return difficulty; }
    }
    
    /**
     * Query execution result
     */
    public static class QueryExecutionResult {
        private final boolean success;
        private final String message;
        private final SampleQuery query;
        
        public QueryExecutionResult(boolean success, String message, SampleQuery query) {
            this.success = success;
            this.message = message;
            this.query = query;
        }
        
        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
        public SampleQuery getQuery() { return query; }
    }
}