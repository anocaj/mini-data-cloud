package com.minicloud.controlplane.service;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Provides comprehensive documentation for NYC datasets.
 * Includes schema information, usage examples, and best practices.
 */
@Service
public class NYCDocumentationService {
    
    /**
     * Get complete documentation for all NYC datasets
     */
    public Map<String, DatasetDocumentation> getAllDatasetDocumentation() {
        return Map.of(
            "taxi_yellow", getYellowTaxiDocumentation(),
            "taxi_green", getGreenTaxiDocumentation(),
            "service_requests_311", get311ServiceRequestDocumentation(),
            "fhv_trips", getFHVTripsDocumentation()
        );
    }
    
    /**
     * Get Yellow Taxi documentation
     */
    public DatasetDocumentation getYellowTaxiDocumentation() {
        return new DatasetDocumentation(
            "taxi_yellow",
            "NYC Yellow Taxi Trip Records",
            "Yellow taxi trip records include pickup and dropoff dates/times, pickup and dropoff locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.",
            "NYC Taxi and Limousine Commission (TLC)",
            "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
            getYellowTaxiSchema(),
            getYellowTaxiUsageExamples(),
            getYellowTaxiNotes()
        );
    }
    
    /**
     * Get Green Taxi documentation
     */
    public DatasetDocumentation getGreenTaxiDocumentation() {
        return new DatasetDocumentation(
            "taxi_green",
            "NYC Green Taxi Trip Records",
            "Green taxi trip records (street-hail livery) include the same data as Yellow taxis but serve areas outside Manhattan's central business district. Green taxis can pick up passengers in the outer boroughs and upper Manhattan.",
            "NYC Taxi and Limousine Commission (TLC)",
            "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
            getGreenTaxiSchema(),
            getGreenTaxiUsageExamples(),
            getGreenTaxiNotes()
        );
    }
    
    /**
     * Get 311 Service Request documentation
     */
    public DatasetDocumentation get311ServiceRequestDocumentation() {
        return new DatasetDocumentation(
            "service_requests_311",
            "NYC 311 Service Requests",
            "311 Service Requests from NYC Open Data. This dataset contains all 311 service requests from 2010 to present. 311 is a non-emergency phone number that provides access to non-emergency municipal services.",
            "NYC Department of Information Technology & Telecommunications (DoITT)",
            "https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9",
            get311ServiceRequestSchema(),
            get311ServiceRequestUsageExamples(),
            get311ServiceRequestNotes()
        );
    }
    
    /**
     * Get FHV Trips documentation
     */
    public DatasetDocumentation getFHVTripsDocumentation() {
        return new DatasetDocumentation(
            "fhv_trips",
            "For-Hire Vehicle Trip Records",
            "For-Hire Vehicle (FHV) trip records include data from ride-sharing companies like Uber and Lyft, as well as traditional car services. Records include pickup and dropoff times, locations, and dispatching base information.",
            "NYC Taxi and Limousine Commission (TLC)",
            "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
            getFHVTripsSchema(),
            getFHVTripsUsageExamples(),
            getFHVTripsNotes()
        );
    }
    
    /**
     * Get Yellow Taxi schema
     */
    private List<SchemaField> getYellowTaxiSchema() {
        return List.of(
            new SchemaField("vendor_id", "INTEGER", "A code indicating the TPEP provider (1=Creative Mobile Technologies, 2=VeriFone Inc.)"),
            new SchemaField("pickup_datetime", "TIMESTAMP", "The date and time when the meter was engaged"),
            new SchemaField("dropoff_datetime", "TIMESTAMP", "The date and time when the meter was disengaged"),
            new SchemaField("passenger_count", "INTEGER", "The number of passengers in the vehicle (driver entered value)"),
            new SchemaField("trip_distance", "DOUBLE", "The elapsed trip distance in miles reported by the taximeter"),
            new SchemaField("pickup_longitude", "DOUBLE", "Longitude where the meter was engaged"),
            new SchemaField("pickup_latitude", "DOUBLE", "Latitude where the meter was engaged"),
            new SchemaField("rate_code_id", "INTEGER", "The final rate code in effect at the end of the trip (1=Standard rate, 2=JFK, 3=Newark, 4=Nassau or Westchester, 5=Negotiated fare, 6=Group ride)"),
            new SchemaField("store_and_fwd_flag", "STRING", "Flag indicating whether the trip record was held in vehicle memory before sending (Y=store and forward trip, N=not a store and forward trip)"),
            new SchemaField("dropoff_longitude", "DOUBLE", "Longitude where the meter was disengaged"),
            new SchemaField("dropoff_latitude", "DOUBLE", "Latitude where the meter was disengaged"),
            new SchemaField("payment_type", "INTEGER", "A numeric code signifying how the passenger paid (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip)"),
            new SchemaField("fare_amount", "DOUBLE", "The time-and-distance fare calculated by the meter"),
            new SchemaField("extra", "DOUBLE", "Miscellaneous extras and surcharges (currently $0.50 and $1 rush hour and overnight charges)"),
            new SchemaField("mta_tax", "DOUBLE", "MTA tax that is automatically triggered based on the metered rate in use ($0.50)"),
            new SchemaField("tip_amount", "DOUBLE", "Tip amount (automatically populated for credit card tips, cash tips are not included)"),
            new SchemaField("tolls_amount", "DOUBLE", "Total amount of all tolls paid in trip"),
            new SchemaField("total_amount", "DOUBLE", "The total amount charged to passengers (does not include cash tips)"),
            new SchemaField("pickup_location_id", "INTEGER", "TLC Taxi Zone where the meter was engaged"),
            new SchemaField("dropoff_location_id", "INTEGER", "TLC Taxi Zone where the meter was disengaged")
        );
    }
    
    /**
     * Get Green Taxi schema
     */
    private List<SchemaField> getGreenTaxiSchema() {
        List<SchemaField> schema = getYellowTaxiSchema(); // Same base schema
        // Add Green taxi specific fields
        schema.add(new SchemaField("improvement_surcharge", "DOUBLE", "Improvement surcharge assessed trips at the flag drop ($0.30)"));
        schema.add(new SchemaField("pickup_borough", "STRING", "Borough where the trip started"));
        schema.add(new SchemaField("dropoff_borough", "STRING", "Borough where the trip ended"));
        return schema;
    }
    
    /**
     * Get 311 Service Request schema
     */
    private List<SchemaField> get311ServiceRequestSchema() {
        return List.of(
            new SchemaField("unique_key", "STRING", "Unique identifier for the service request"),
            new SchemaField("created_date", "TIMESTAMP", "Date and time the service request was created"),
            new SchemaField("closed_date", "TIMESTAMP", "Date and time the service request was closed"),
            new SchemaField("agency", "STRING", "Acronym of the responding City Government Agency"),
            new SchemaField("agency_name", "STRING", "Full name of the responding City Government Agency"),
            new SchemaField("complaint_type", "STRING", "First level descriptor of the service request"),
            new SchemaField("descriptor", "STRING", "Second level descriptor of the service request"),
            new SchemaField("location_type", "STRING", "Type of location where the incident occurred"),
            new SchemaField("incident_zip", "STRING", "Incident location zip code"),
            new SchemaField("incident_address", "STRING", "Incident location address"),
            new SchemaField("street_name", "STRING", "Name of the street where the incident occurred"),
            new SchemaField("cross_street_1", "STRING", "First cross street"),
            new SchemaField("cross_street_2", "STRING", "Second cross street"),
            new SchemaField("intersection_street_1", "STRING", "First intersecting street"),
            new SchemaField("intersection_street_2", "STRING", "Second intersecting street"),
            new SchemaField("address_type", "STRING", "Type of address (ADDRESS, INTERSECTION, etc.)"),
            new SchemaField("city", "STRING", "City where the incident occurred"),
            new SchemaField("landmark", "STRING", "Landmark near the incident location"),
            new SchemaField("facility_type", "STRING", "Type of facility if applicable"),
            new SchemaField("status", "STRING", "Status of the service request (Open, Closed, Pending, etc.)"),
            new SchemaField("due_date", "TIMESTAMP", "Date by which the agency should respond"),
            new SchemaField("resolution_description", "STRING", "Description of how the issue was resolved"),
            new SchemaField("resolution_action_updated_date", "TIMESTAMP", "Date when resolution action was last updated"),
            new SchemaField("community_board", "STRING", "Community board for the incident location"),
            new SchemaField("borough", "STRING", "Borough where the incident occurred"),
            new SchemaField("latitude", "DOUBLE", "Latitude of the incident location"),
            new SchemaField("longitude", "DOUBLE", "Longitude of the incident location"),
            new SchemaField("location", "STRING", "Latitude and longitude coordinates as a string")
        );
    }
    
    /**
     * Get FHV Trips schema
     */
    private List<SchemaField> getFHVTripsSchema() {
        return List.of(
            new SchemaField("dispatching_base_num", "STRING", "The TLC Base License Number of the base that dispatched the trip"),
            new SchemaField("pickup_datetime", "TIMESTAMP", "Date and time when passenger was picked up"),
            new SchemaField("dropoff_datetime", "TIMESTAMP", "Date and time when passenger was dropped off"),
            new SchemaField("pickup_location_id", "INTEGER", "TLC Taxi Zone where the trip started"),
            new SchemaField("dropoff_location_id", "INTEGER", "TLC Taxi Zone where the trip ended"),
            new SchemaField("sr_flag", "STRING", "Indicates if the trip was part of a shared ride chain (Y=shared ride, N=not shared)"),
            new SchemaField("affiliated_base_number", "STRING", "The TLC Base License Number of the affiliated base")
        );
    }
    
    /**
     * Get Yellow Taxi usage examples
     */
    private List<UsageExample> getYellowTaxiUsageExamples() {
        return List.of(
            new UsageExample(
                "Basic Trip Analysis",
                "SELECT COUNT(*) as total_trips, AVG(trip_distance) as avg_distance FROM taxi_yellow",
                "Get basic statistics about taxi trips"
            ),
            new UsageExample(
                "Peak Hours Analysis",
                "SELECT EXTRACT(HOUR FROM pickup_datetime) as hour, COUNT(*) as trips FROM taxi_yellow GROUP BY hour ORDER BY trips DESC",
                "Find the busiest hours for taxi pickups"
            ),
            new UsageExample(
                "Fare Analysis by Distance",
                "SELECT CASE WHEN trip_distance < 2 THEN 'Short' WHEN trip_distance < 10 THEN 'Medium' ELSE 'Long' END as distance_category, AVG(fare_amount) as avg_fare FROM taxi_yellow GROUP BY distance_category",
                "Analyze average fares by trip distance categories"
            ),
            new UsageExample(
                "Payment Method Distribution",
                "SELECT payment_type, COUNT(*) as count, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage FROM taxi_yellow GROUP BY payment_type",
                "Analyze payment method preferences"
            )
        );
    }
    
    /**
     * Get Green Taxi usage examples
     */
    private List<UsageExample> getGreenTaxiUsageExamples() {
        return List.of(
            new UsageExample(
                "Borough Analysis",
                "SELECT pickup_borough, COUNT(*) as trips FROM taxi_green GROUP BY pickup_borough ORDER BY trips DESC",
                "Analyze trip distribution by pickup borough"
            ),
            new UsageExample(
                "Inter-Borough Trips",
                "SELECT pickup_borough, dropoff_borough, COUNT(*) as trips FROM taxi_green WHERE pickup_borough != dropoff_borough GROUP BY pickup_borough, dropoff_borough ORDER BY trips DESC",
                "Find the most common inter-borough trips"
            )
        );
    }
    
    /**
     * Get 311 Service Request usage examples
     */
    private List<UsageExample> get311ServiceRequestUsageExamples() {
        return List.of(
            new UsageExample(
                "Top Complaint Types",
                "SELECT complaint_type, COUNT(*) as count FROM service_requests_311 GROUP BY complaint_type ORDER BY count DESC LIMIT 10",
                "Find the most common complaint types"
            ),
            new UsageExample(
                "Agency Response Analysis",
                "SELECT agency, COUNT(*) as total_requests, COUNT(CASE WHEN status = 'Closed' THEN 1 END) as closed_requests FROM service_requests_311 GROUP BY agency",
                "Analyze response rates by agency"
            ),
            new UsageExample(
                "Seasonal Patterns",
                "SELECT EXTRACT(MONTH FROM created_date) as month, complaint_type, COUNT(*) as count FROM service_requests_311 GROUP BY month, complaint_type ORDER BY month, count DESC",
                "Analyze seasonal patterns in complaints"
            )
        );
    }
    
    /**
     * Get FHV Trips usage examples
     */
    private List<UsageExample> getFHVTripsUsageExamples() {
        return List.of(
            new UsageExample(
                "Base Activity Analysis",
                "SELECT dispatching_base_num, COUNT(*) as trips FROM fhv_trips GROUP BY dispatching_base_num ORDER BY trips DESC LIMIT 10",
                "Find the most active dispatching bases"
            ),
            new UsageExample(
                "Shared Ride Analysis",
                "SELECT sr_flag, COUNT(*) as trips, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage FROM fhv_trips GROUP BY sr_flag",
                "Analyze shared ride vs individual ride patterns"
            )
        );
    }
    
    /**
     * Get Yellow Taxi notes
     */
    private List<String> getYellowTaxiNotes() {
        return List.of(
            "Yellow taxis can only pick up passengers in Manhattan below 96th Street and at the airports",
            "Tip amounts are only recorded for credit card payments; cash tips are not included",
            "Location coordinates may be (0,0) for some records due to GPS issues",
            "Rate codes: 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group ride",
            "Store and forward trips occur when the taxi's GPS connection is lost"
        );
    }
    
    /**
     * Get Green Taxi notes
     */
    private List<String> getGreenTaxiNotes() {
        return List.of(
            "Green taxis serve areas outside Manhattan's central business district",
            "Can pick up street hails in the outer boroughs and upper Manhattan (above 96th Street)",
            "Include improvement surcharge of $0.30 per trip",
            "Borough information helps analyze outer borough transportation patterns"
        );
    }
    
    /**
     * Get 311 Service Request notes
     */
    private List<String> get311ServiceRequestNotes() {
        return List.of(
            "Not all service requests have location information",
            "Some requests may be duplicates if citizens call multiple times",
            "Response times vary significantly by agency and complaint type",
            "Status 'Closed' doesn't necessarily mean the issue was resolved",
            "Community board information helps with local area analysis"
        );
    }
    
    /**
     * Get FHV Trips notes
     */
    private List<String> getFHVTripsNotes() {
        return List.of(
            "Includes trips from Uber, Lyft, and other ride-sharing services",
            "Shared ride flag indicates if the trip was part of a shared ride chain",
            "Dispatching base numbers identify the company that dispatched the trip",
            "Location IDs correspond to TLC Taxi Zones",
            "Some bases may be affiliated with larger companies"
        );
    }
    
    /**
     * Get query best practices
     */
    public List<String> getQueryBestPractices() {
        return List.of(
            "Use date filters to limit query scope and improve performance",
            "Consider partitioning when querying large date ranges",
            "Use LIMIT clauses for exploratory queries",
            "Index on commonly filtered columns like pickup_datetime",
            "Be aware of NULL values in location and fare fields",
            "Use appropriate aggregation functions for your analysis",
            "Consider time zones when working with datetime fields",
            "Validate data ranges before performing calculations"
        );
    }
    
    /**
     * Get common analysis patterns
     */
    public List<String> getCommonAnalysisPatterns() {
        return List.of(
            "Time-based analysis: hourly, daily, weekly, monthly patterns",
            "Geospatial analysis: pickup/dropoff location patterns",
            "Fare analysis: relationship between distance, time, and fare",
            "Service quality: response times, resolution rates",
            "Comparative analysis: different transportation modes",
            "Seasonal analysis: how patterns change throughout the year",
            "Demand forecasting: predicting future transportation needs"
        );
    }
    
    /**
     * Dataset documentation
     */
    public static class DatasetDocumentation {
        private final String name;
        private final String displayName;
        private final String description;
        private final String source;
        private final String sourceUrl;
        private final List<SchemaField> schema;
        private final List<UsageExample> usageExamples;
        private final List<String> notes;
        
        public DatasetDocumentation(String name, String displayName, String description, String source,
                                  String sourceUrl, List<SchemaField> schema, List<UsageExample> usageExamples,
                                  List<String> notes) {
            this.name = name;
            this.displayName = displayName;
            this.description = description;
            this.source = source;
            this.sourceUrl = sourceUrl;
            this.schema = schema;
            this.usageExamples = usageExamples;
            this.notes = notes;
        }
        
        // Getters
        public String getName() { return name; }
        public String getDisplayName() { return displayName; }
        public String getDescription() { return description; }
        public String getSource() { return source; }
        public String getSourceUrl() { return sourceUrl; }
        public List<SchemaField> getSchema() { return schema; }
        public List<UsageExample> getUsageExamples() { return usageExamples; }
        public List<String> getNotes() { return notes; }
    }
    
    /**
     * Schema field definition
     */
    public static class SchemaField {
        private final String name;
        private final String type;
        private final String description;
        
        public SchemaField(String name, String type, String description) {
            this.name = name;
            this.type = type;
            this.description = description;
        }
        
        public String getName() { return name; }
        public String getType() { return type; }
        public String getDescription() { return description; }
    }
    
    /**
     * Usage example
     */
    public static class UsageExample {
        private final String title;
        private final String sql;
        private final String description;
        
        public UsageExample(String title, String sql, String description) {
            this.title = title;
            this.sql = sql;
            this.description = description;
        }
        
        public String getTitle() { return title; }
        public String getSql() { return sql; }
        public String getDescription() { return description; }
    }
}