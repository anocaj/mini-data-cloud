package com.minicloud.controlplane.controller;

import com.minicloud.controlplane.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST controller for NYC data platform operations.
 * Provides endpoints for loading, managing, and querying NYC datasets.
 */
@RestController
@RequestMapping("/api/v1/nyc")
@CrossOrigin(origins = "*")
public class NYCDataController {
    
    private static final Logger logger = LoggerFactory.getLogger(NYCDataController.class);
    
    @Autowired
    private NYCDataPlatform nycDataPlatform;
    
    @Autowired
    private TaxiDataProcessor taxiDataProcessor;
    
    @Autowired
    private ServiceRequestProcessor serviceRequestProcessor;
    
    @Autowired
    private FHVDataProcessor fhvDataProcessor;
    
    @Autowired
    private DataQualityManager dataQualityManager;
    
    @Autowired
    private PartitioningStrategy partitioningStrategy;
    
    @Autowired
    private NYCSampleQueryManager sampleQueryManager;
    
    @Autowired
    private NYCDocumentationService documentationService;
    
    /**
     * Get NYC data platform status
     */
    @GetMapping("/status")
    public ResponseEntity<NYCDataPlatform.NYCDatasetStatus> getStatus() {
        logger.info("Getting NYC data platform status");
        
        try {
            NYCDataPlatform.NYCDatasetStatus status = nycDataPlatform.getDatasetStatus();
            return ResponseEntity.ok(status);
            
        } catch (Exception e) {
            logger.error("Error getting NYC data platform status", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Load all sample NYC datasets
     */
    @PostMapping("/datasets/load")
    public ResponseEntity<Map<String, String>> loadAllDatasets() {
        logger.info("Loading all NYC sample datasets");
        
        try {
            nycDataPlatform.loadSampleDatasets();
            
            return ResponseEntity.ok(Map.of(
                "status", "loading_started",
                "message", "NYC datasets are being loaded asynchronously"
            ));
            
        } catch (Exception e) {
            logger.error("Error loading NYC datasets", e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", "Failed to load datasets: " + e.getMessage()));
        }
    }
    
    /**
     * Load a specific dataset
     */
    @PostMapping("/datasets/{datasetName}/load")
    public ResponseEntity<Map<String, String>> loadDataset(@PathVariable String datasetName) {
        logger.info("Loading NYC dataset: {}", datasetName);
        
        try {
            nycDataPlatform.reloadDataset(datasetName);
            
            return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "Dataset " + datasetName + " loaded successfully"
            ));
            
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                .body(Map.of("error", "Unknown dataset: " + datasetName));
                
        } catch (Exception e) {
            logger.error("Error loading dataset: {}", datasetName, e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", "Failed to load dataset: " + e.getMessage()));
        }
    }
    
    /**
     * Get sample queries for NYC datasets
     */
    @GetMapping("/queries/samples")
    public ResponseEntity<List<NYCDataPlatform.SampleQuery>> getSampleQueries() {
        logger.info("Getting sample queries for NYC datasets");
        
        try {
            List<NYCDataPlatform.SampleQuery> queries = nycDataPlatform.getSampleQueries();
            return ResponseEntity.ok(queries);
            
        } catch (Exception e) {
            logger.error("Error getting sample queries", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get all sample queries organized by category
     */
    @GetMapping("/queries/samples/all")
    public ResponseEntity<Map<String, List<NYCSampleQueryManager.SampleQuery>>> getAllSampleQueries() {
        logger.info("Getting all sample queries organized by category");
        
        try {
            Map<String, List<NYCSampleQueryManager.SampleQuery>> queries = sampleQueryManager.getAllSampleQueries();
            return ResponseEntity.ok(queries);
            
        } catch (Exception e) {
            logger.error("Error getting all sample queries", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get sample queries by difficulty level
     */
    @GetMapping("/queries/samples/difficulty/{difficulty}")
    public ResponseEntity<List<NYCSampleQueryManager.SampleQuery>> getQueriesByDifficulty(@PathVariable String difficulty) {
        logger.info("Getting sample queries by difficulty: {}", difficulty);
        
        try {
            List<NYCSampleQueryManager.SampleQuery> queries = sampleQueryManager.getQueriesByDifficulty(difficulty);
            return ResponseEntity.ok(queries);
            
        } catch (Exception e) {
            logger.error("Error getting queries by difficulty: {}", difficulty, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get sample queries by dataset
     */
    @GetMapping("/queries/samples/dataset/{dataset}")
    public ResponseEntity<List<NYCSampleQueryManager.SampleQuery>> getQueriesByDataset(@PathVariable String dataset) {
        logger.info("Getting sample queries for dataset: {}", dataset);
        
        try {
            List<NYCSampleQueryManager.SampleQuery> queries = sampleQueryManager.getQueriesByDataset(dataset);
            return ResponseEntity.ok(queries);
            
        } catch (Exception e) {
            logger.error("Error getting queries for dataset: {}", dataset, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Execute a sample query
     */
    @PostMapping("/queries/samples/{queryId}/execute")
    public ResponseEntity<NYCSampleQueryManager.QueryExecutionResult> executeSampleQuery(@PathVariable String queryId) {
        logger.info("Executing sample query: {}", queryId);
        
        try {
            NYCSampleQueryManager.QueryExecutionResult result = sampleQueryManager.executeSampleQuery(queryId);
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            logger.error("Error executing sample query: {}", queryId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get taxi data statistics
     */
    @GetMapping("/datasets/taxi/stats")
    public ResponseEntity<TaxiDataProcessor.TaxiDataStats> getTaxiStats() {
        logger.info("Getting taxi data statistics");
        
        try {
            TaxiDataProcessor.TaxiDataStats stats = taxiDataProcessor.getDataStats();
            return ResponseEntity.ok(stats);
            
        } catch (Exception e) {
            logger.error("Error getting taxi statistics", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get 311 service request statistics
     */
    @GetMapping("/datasets/311/stats")
    public ResponseEntity<ServiceRequestProcessor.ServiceRequestStats> get311Stats() {
        logger.info("Getting 311 service request statistics");
        
        try {
            ServiceRequestProcessor.ServiceRequestStats stats = serviceRequestProcessor.getDataStats();
            return ResponseEntity.ok(stats);
            
        } catch (Exception e) {
            logger.error("Error getting 311 statistics", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get FHV data statistics
     */
    @GetMapping("/datasets/fhv/stats")
    public ResponseEntity<FHVDataProcessor.FHVDataStats> getFHVStats() {
        logger.info("Getting FHV data statistics");
        
        try {
            FHVDataProcessor.FHVDataStats stats = fhvDataProcessor.getDataStats();
            return ResponseEntity.ok(stats);
            
        } catch (Exception e) {
            logger.error("Error getting FHV statistics", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get FHV dispatching base analysis
     */
    @GetMapping("/datasets/fhv/analysis/bases")
    public ResponseEntity<FHVDataProcessor.DispatchingBaseAnalysis> getFHVBaseAnalysis() {
        logger.info("Getting FHV dispatching base analysis");
        
        try {
            FHVDataProcessor.DispatchingBaseAnalysis analysis = fhvDataProcessor.getDispatchingBaseAnalysis();
            return ResponseEntity.ok(analysis);
            
        } catch (Exception e) {
            logger.error("Error getting FHV base analysis", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Validate data quality for all datasets
     */
    @PostMapping("/datasets/validate")
    public ResponseEntity<List<DataQualityManager.ValidationResult>> validateDataQuality() {
        logger.info("Validating data quality for all NYC datasets");
        
        try {
            List<DataQualityManager.ValidationResult> results = dataQualityManager.validateAllNYCDatasets();
            return ResponseEntity.ok(results);
            
        } catch (Exception e) {
            logger.error("Error validating data quality", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Validate data quality for a specific dataset
     */
    @PostMapping("/datasets/{datasetName}/validate")
    public ResponseEntity<DataQualityManager.ValidationResult> validateDataset(@PathVariable String datasetName) {
        logger.info("Validating data quality for dataset: {}", datasetName);
        
        try {
            DataQualityManager.ValidationResult result;
            
            switch (datasetName.toLowerCase()) {
                case "taxi_yellow":
                    result = dataQualityManager.validateTaxiDataset("nyc", "taxi_yellow");
                    break;
                case "taxi_green":
                    result = dataQualityManager.validateTaxiDataset("nyc", "taxi_green");
                    break;
                case "service_requests_311":
                case "311":
                    result = dataQualityManager.validate311Dataset("nyc", "service_requests_311");
                    break;
                case "fhv_trips":
                case "fhv":
                    result = dataQualityManager.validateFHVDataset("nyc", "fhv_trips");
                    break;
                default:
                    return ResponseEntity.badRequest().build();
            }
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            logger.error("Error validating dataset: {}", datasetName, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get partitioning recommendation for a dataset
     */
    @GetMapping("/datasets/{datasetName}/partitioning/recommendation")
    public ResponseEntity<PartitioningStrategy.PartitioningRecommendation> getPartitioningRecommendation(
            @PathVariable String datasetName) {
        logger.info("Getting partitioning recommendation for dataset: {}", datasetName);
        
        try {
            PartitioningStrategy.PartitioningRecommendation recommendation = 
                partitioningStrategy.getPartitioningRecommendation("nyc", datasetName);
            
            return ResponseEntity.ok(recommendation);
            
        } catch (Exception e) {
            logger.error("Error getting partitioning recommendation for dataset: {}", datasetName, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Analyze partitioning effectiveness for a dataset
     */
    @GetMapping("/datasets/{datasetName}/partitioning/analysis")
    public ResponseEntity<PartitioningStrategy.PartitioningAnalysis> analyzePartitioning(
            @PathVariable String datasetName) {
        logger.info("Analyzing partitioning effectiveness for dataset: {}", datasetName);
        
        try {
            PartitioningStrategy.PartitioningAnalysis analysis = 
                partitioningStrategy.analyzePartitioningEffectiveness("nyc", datasetName);
            
            return ResponseEntity.ok(analysis);
            
        } catch (Exception e) {
            logger.error("Error analyzing partitioning for dataset: {}", datasetName, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Optimize partitioning based on query patterns
     */
    @PostMapping("/datasets/{datasetName}/partitioning/optimize")
    public ResponseEntity<PartitioningStrategy.PartitioningOptimizationResult> optimizePartitioning(
            @PathVariable String datasetName,
            @RequestBody List<String> queryPatterns) {
        logger.info("Optimizing partitioning for dataset: {} based on {} query patterns", 
                   datasetName, queryPatterns.size());
        
        try {
            PartitioningStrategy.PartitioningOptimizationResult result = 
                partitioningStrategy.optimizePartitioning("nyc", datasetName, queryPatterns);
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            logger.error("Error optimizing partitioning for dataset: {}", datasetName, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Apply intelligent partitioning to a dataset
     */
    @PostMapping("/datasets/{datasetName}/partitioning/apply-intelligent")
    public ResponseEntity<Map<String, String>> applyIntelligentPartitioning(@PathVariable String datasetName) {
        logger.info("Applying intelligent partitioning to dataset: {}", datasetName);
        
        try {
            switch (datasetName.toLowerCase()) {
                case "taxi_yellow":
                case "taxi_green":
                    partitioningStrategy.applyTaxiTripPartitioning("nyc", datasetName);
                    break;
                case "service_requests_311":
                case "311":
                    partitioningStrategy.apply311ServiceRequestPartitioning("nyc", "service_requests_311");
                    break;
                case "fhv_trips":
                case "fhv":
                    partitioningStrategy.applyFHVTripPartitioning("nyc", "fhv_trips");
                    break;
                default:
                    return ResponseEntity.badRequest()
                        .body(Map.of("error", "Unknown dataset: " + datasetName));
            }
            
            return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "Intelligent partitioning applied to " + datasetName
            ));
            
        } catch (Exception e) {
            logger.error("Error applying intelligent partitioning to dataset: {}", datasetName, e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", "Failed to apply partitioning: " + e.getMessage()));
        }
    }
    
    /**
     * Get available datasets
     */
    @GetMapping("/datasets")
    public ResponseEntity<List<DatasetInfo>> getAvailableDatasets() {
        logger.info("Getting available NYC datasets");
        
        try {
            List<DatasetInfo> datasets = List.of(
                new DatasetInfo("taxi_yellow", "NYC Yellow Taxi Trips", 
                               "Yellow taxi trip records with pickup/dropoff locations and fare details",
                               taxiDataProcessor.getLoadingStatus()),
                new DatasetInfo("taxi_green", "NYC Green Taxi Trips", 
                               "Green taxi trip records serving outer boroughs",
                               taxiDataProcessor.getLoadingStatus()),
                new DatasetInfo("service_requests_311", "NYC 311 Service Requests", 
                               "Citizen service requests and complaints to NYC agencies",
                               serviceRequestProcessor.getLoadingStatus()),
                new DatasetInfo("fhv_trips", "For-Hire Vehicle Trips", 
                               "Uber, Lyft, and other for-hire vehicle trip records",
                               fhvDataProcessor.getLoadingStatus())
            );
            
            return ResponseEntity.ok(datasets);
            
        } catch (Exception e) {
            logger.error("Error getting available datasets", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Dataset information
     */
    public static class DatasetInfo {
        private final String name;
        private final String displayName;
        private final String description;
        private final String status;
        
        public DatasetInfo(String name, String displayName, String description, String status) {
            this.name = name;
            this.displayName = displayName;
            this.description = description;
            this.status = status;
        }
        
        public String getName() { return name; }
        public String getDisplayName() { return displayName; }
        public String getDescription() { return description; }
        public String getStatus() { return status; }
    }
    
    /**
     * Get documentation for all datasets
     */
    @GetMapping("/documentation")
    public ResponseEntity<Map<String, NYCDocumentationService.DatasetDocumentation>> getAllDocumentation() {
        logger.info("Getting documentation for all NYC datasets");
        
        try {
            Map<String, NYCDocumentationService.DatasetDocumentation> documentation = 
                documentationService.getAllDatasetDocumentation();
            return ResponseEntity.ok(documentation);
            
        } catch (Exception e) {
            logger.error("Error getting documentation", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get documentation for a specific dataset
     */
    @GetMapping("/documentation/{datasetName}")
    public ResponseEntity<NYCDocumentationService.DatasetDocumentation> getDatasetDocumentation(
            @PathVariable String datasetName) {
        logger.info("Getting documentation for dataset: {}", datasetName);
        
        try {
            NYCDocumentationService.DatasetDocumentation documentation;
            
            switch (datasetName.toLowerCase()) {
                case "taxi_yellow":
                    documentation = documentationService.getYellowTaxiDocumentation();
                    break;
                case "taxi_green":
                    documentation = documentationService.getGreenTaxiDocumentation();
                    break;
                case "service_requests_311":
                case "311":
                    documentation = documentationService.get311ServiceRequestDocumentation();
                    break;
                case "fhv_trips":
                case "fhv":
                    documentation = documentationService.getFHVTripsDocumentation();
                    break;
                default:
                    return ResponseEntity.notFound().build();
            }
            
            return ResponseEntity.ok(documentation);
            
        } catch (Exception e) {
            logger.error("Error getting documentation for dataset: {}", datasetName, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get query best practices
     */
    @GetMapping("/documentation/best-practices")
    public ResponseEntity<List<String>> getQueryBestPractices() {
        logger.info("Getting query best practices");
        
        try {
            List<String> bestPractices = documentationService.getQueryBestPractices();
            return ResponseEntity.ok(bestPractices);
            
        } catch (Exception e) {
            logger.error("Error getting best practices", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get common analysis patterns
     */
    @GetMapping("/documentation/analysis-patterns")
    public ResponseEntity<List<String>> getCommonAnalysisPatterns() {
        logger.info("Getting common analysis patterns");
        
        try {
            List<String> patterns = documentationService.getCommonAnalysisPatterns();
            return ResponseEntity.ok(patterns);
            
        } catch (Exception e) {
            logger.error("Error getting analysis patterns", e);
            return ResponseEntity.internalServerError().build();
        }
    }
}