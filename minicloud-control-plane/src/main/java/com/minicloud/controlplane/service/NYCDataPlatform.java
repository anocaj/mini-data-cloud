package com.minicloud.controlplane.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Orchestrates the complete NYC data ecosystem for demonstration purposes.
 * Manages loading and processing of NYC open datasets including taxi trips,
 * 311 service requests, and For-Hire Vehicle data.
 */
@Service
public class NYCDataPlatform {
    
    private static final Logger logger = LoggerFactory.getLogger(NYCDataPlatform.class);
    
    @Autowired
    private TaxiDataProcessor taxiDataProcessor;
    
    @Autowired
    private ServiceRequestProcessor serviceRequestProcessor;
    
    @Autowired
    private FHVDataProcessor fhvDataProcessor;
    
    @Autowired
    private DataQualityManager dataQualityManager;
    
    @Value("${minicloud.nyc-data.enabled:false}")
    private boolean nycDataEnabled;
    
    @Value("${minicloud.nyc-data.auto-load:false}")
    private boolean autoLoadEnabled;
    
    private final ExecutorService executorService = Executors.newFixedThreadPool(3);
    
    /**
     * Initialize NYC data platform on application startup
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initializeNYCDataPlatform() {
        if (!nycDataEnabled) {
            logger.info("NYC data platform is disabled");
            return;
        }
        
        logger.info("Initializing NYC data platform...");
        
        if (autoLoadEnabled) {
            loadSampleDatasets();
        }
        
        logger.info("NYC data platform initialized successfully");
    }
    
    /**
     * Load all sample NYC datasets asynchronously
     */
    public void loadSampleDatasets() {
        logger.info("Starting to load NYC sample datasets...");
        
        CompletableFuture<Void> taxiLoading = CompletableFuture.runAsync(() -> {
            try {
                taxiDataProcessor.loadSampleTaxiData();
            } catch (Exception e) {
                logger.error("Failed to load taxi data", e);
            }
        }, executorService);
        
        CompletableFuture<Void> serviceRequestLoading = CompletableFuture.runAsync(() -> {
            try {
                serviceRequestProcessor.loadSample311Data();
            } catch (Exception e) {
                logger.error("Failed to load 311 service request data", e);
            }
        }, executorService);
        
        CompletableFuture<Void> fhvLoading = CompletableFuture.runAsync(() -> {
            try {
                fhvDataProcessor.loadSampleFHVData();
            } catch (Exception e) {
                logger.error("Failed to load FHV data", e);
            }
        }, executorService);
        
        // Wait for all loading operations to complete
        CompletableFuture.allOf(taxiLoading, serviceRequestLoading, fhvLoading)
            .thenRun(() -> {
                logger.info("All NYC datasets loaded successfully");
                performDataQualityValidation();
            })
            .exceptionally(throwable -> {
                logger.error("Error loading NYC datasets", throwable);
                return null;
            });
    }
    
    /**
     * Perform data quality validation on loaded datasets
     */
    private void performDataQualityValidation() {
        logger.info("Starting data quality validation for NYC datasets...");
        
        try {
            List<DataQualityManager.ValidationResult> results = dataQualityManager.validateAllNYCDatasets();
            
            for (DataQualityManager.ValidationResult result : results) {
                if (result.isValid()) {
                    logger.info("Data quality validation passed for dataset: {}", result.getDatasetName());
                } else {
                    logger.warn("Data quality issues found in dataset {}: {}", 
                               result.getDatasetName(), result.getIssues());
                }
            }
            
        } catch (Exception e) {
            logger.error("Data quality validation failed", e);
        }
    }
    
    /**
     * Get status of all NYC datasets
     */
    public NYCDatasetStatus getDatasetStatus() {
        return new NYCDatasetStatus(
            taxiDataProcessor.getLoadingStatus(),
            serviceRequestProcessor.getLoadingStatus(),
            fhvDataProcessor.getLoadingStatus()
        );
    }
    
    /**
     * Reload a specific dataset
     */
    public void reloadDataset(String datasetName) {
        logger.info("Reloading NYC dataset: {}", datasetName);
        
        switch (datasetName.toLowerCase()) {
            case "taxi":
            case "taxi_yellow":
            case "taxi_green":
                taxiDataProcessor.loadSampleTaxiData();
                break;
            case "311":
            case "service_requests":
                serviceRequestProcessor.loadSample311Data();
                break;
            case "fhv":
            case "for_hire_vehicles":
                fhvDataProcessor.loadSampleFHVData();
                break;
            default:
                throw new IllegalArgumentException("Unknown dataset: " + datasetName);
        }
    }
    
    /**
     * Get available sample queries for NYC datasets
     */
    public List<SampleQuery> getSampleQueries() {
        return List.of(
            // Taxi queries
            new SampleQuery("taxi_monthly_trips", 
                "Monthly taxi trip counts",
                "SELECT EXTRACT(MONTH FROM pickup_datetime) as month, COUNT(*) as trip_count FROM taxi_yellow GROUP BY EXTRACT(MONTH FROM pickup_datetime) ORDER BY month"),
            
            new SampleQuery("taxi_fare_analysis",
                "Average fare by distance",
                "SELECT CASE WHEN trip_distance < 1 THEN 'Short' WHEN trip_distance < 5 THEN 'Medium' ELSE 'Long' END as distance_category, AVG(fare_amount) as avg_fare FROM taxi_yellow GROUP BY distance_category"),
            
            // 311 Service Request queries
            new SampleQuery("311_complaints_by_agency",
                "Top complaint types by agency",
                "SELECT agency, complaint_type, COUNT(*) as complaint_count FROM service_requests_311 GROUP BY agency, complaint_type ORDER BY complaint_count DESC LIMIT 20"),
            
            new SampleQuery("311_response_times",
                "Average response times by borough",
                "SELECT borough, AVG(EXTRACT(DAY FROM (closed_date - created_date))) as avg_response_days FROM service_requests_311 WHERE closed_date IS NOT NULL GROUP BY borough ORDER BY avg_response_days"),
            
            // FHV queries
            new SampleQuery("fhv_base_activity",
                "Most active FHV bases",
                "SELECT dispatching_base_num, COUNT(*) as trip_count FROM fhv_trips GROUP BY dispatching_base_num ORDER BY trip_count DESC LIMIT 10"),
            
            // Cross-dataset queries
            new SampleQuery("transportation_comparison",
                "Transportation mode comparison",
                "SELECT 'Yellow Taxi' as mode, COUNT(*) as trips FROM taxi_yellow UNION ALL SELECT 'FHV' as mode, COUNT(*) as trips FROM fhv_trips")
        );
    }
    
    /**
     * Status of NYC datasets
     */
    public static class NYCDatasetStatus {
        private final String taxiStatus;
        private final String serviceRequestStatus;
        private final String fhvStatus;
        
        public NYCDatasetStatus(String taxiStatus, String serviceRequestStatus, String fhvStatus) {
            this.taxiStatus = taxiStatus;
            this.serviceRequestStatus = serviceRequestStatus;
            this.fhvStatus = fhvStatus;
        }
        
        public String getTaxiStatus() { return taxiStatus; }
        public String getServiceRequestStatus() { return serviceRequestStatus; }
        public String getFhvStatus() { return fhvStatus; }
    }
    
    /**
     * Sample query definition
     */
    public static class SampleQuery {
        private final String id;
        private final String name;
        private final String sql;
        
        public SampleQuery(String id, String name, String sql) {
            this.id = id;
            this.name = name;
            this.sql = sql;
        }
        
        public String getId() { return id; }
        public String getName() { return name; }
        public String getSql() { return sql; }
    }
}