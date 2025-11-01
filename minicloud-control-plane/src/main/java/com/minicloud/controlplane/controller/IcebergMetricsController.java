package com.minicloud.controlplane.controller;

import com.minicloud.controlplane.service.IcebergQueryMetricsService;
import com.minicloud.controlplane.service.IcebergCatalogMetricsService;
import com.minicloud.controlplane.service.NYCDatasetMetricsService;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * REST controller for exposing Iceberg performance metrics and monitoring data.
 */
@RestController
@RequestMapping("/api/iceberg/metrics")
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class IcebergMetricsController {
    
    private static final Logger logger = LoggerFactory.getLogger(IcebergMetricsController.class);
    
    private final IcebergQueryMetricsService queryMetricsService;
    private final IcebergCatalogMetricsService catalogMetricsService;
    private final NYCDatasetMetricsService nycDatasetMetricsService;
    
    @Autowired
    public IcebergMetricsController(IcebergQueryMetricsService queryMetricsService,
                                  IcebergCatalogMetricsService catalogMetricsService,
                                  NYCDatasetMetricsService nycDatasetMetricsService) {
        this.queryMetricsService = queryMetricsService;
        this.catalogMetricsService = catalogMetricsService;
        this.nycDatasetMetricsService = nycDatasetMetricsService;
    }
    
    /**
     * Gets overall Iceberg performance metrics.
     */
    @GetMapping("/performance")
    public ResponseEntity<IcebergQueryMetricsService.IcebergPerformanceMetrics> getOverallPerformanceMetrics() {
        try {
            logger.debug("Getting overall Iceberg performance metrics");
            
            IcebergQueryMetricsService.IcebergPerformanceMetrics metrics = 
                queryMetricsService.getOverallPerformanceMetrics();
            
            return ResponseEntity.ok(metrics);
            
        } catch (Exception e) {
            logger.error("Failed to get overall performance metrics", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Gets performance metrics for a specific table.
     */
    @GetMapping("/performance/table/{namespace}/{tableName}")
    public ResponseEntity<IcebergQueryMetricsService.TablePerformanceMetrics> getTablePerformanceMetrics(
            @PathVariable String namespace,
            @PathVariable String tableName) {
        
        try {
            logger.debug("Getting performance metrics for table: {}.{}", namespace, tableName);
            
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            IcebergQueryMetricsService.TablePerformanceMetrics metrics = 
                queryMetricsService.getTablePerformanceMetrics(tableId);
            
            return ResponseEntity.ok(metrics);
            
        } catch (Exception e) {
            logger.error("Failed to get table performance metrics for {}.{}", namespace, tableName, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Gets a summary of key Iceberg metrics for dashboard display.
     */
    @GetMapping("/summary")
    public ResponseEntity<Map<String, Object>> getMetricsSummary() {
        try {
            logger.debug("Getting Iceberg metrics summary");
            
            IcebergQueryMetricsService.IcebergPerformanceMetrics metrics = 
                queryMetricsService.getOverallPerformanceMetrics();
            
            Map<String, Object> summary = new HashMap<>();
            summary.put("total_queries", metrics.getTotalQueries());
            summary.put("total_time_travel_queries", metrics.getTotalTimeTravelQueries());
            summary.put("total_transactions", metrics.getTotalTransactions());
            summary.put("active_queries", metrics.getActiveQueries());
            summary.put("active_transactions", metrics.getActiveTransactions());
            summary.put("average_query_time_ms", metrics.getAverageQueryTimeMs());
            summary.put("average_time_travel_time_ms", metrics.getAverageTimeTravelTimeMs());
            summary.put("average_transaction_time_ms", metrics.getAverageTransactionTimeMs());
            summary.put("total_files_scanned", metrics.getTotalFilesScanned());
            summary.put("total_files_pruned", metrics.getTotalFilesPruned());
            summary.put("file_pruning_efficiency", metrics.getFilePruningEfficiency());
            
            return ResponseEntity.ok(summary);
            
        } catch (Exception e) {
            logger.error("Failed to get metrics summary", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Records the start of a query for metrics tracking (used by query execution).
     */
    @PostMapping("/query/start")
    public ResponseEntity<Void> recordQueryStart(
            @RequestParam String queryId,
            @RequestParam String namespace,
            @RequestParam String tableName,
            @RequestParam(defaultValue = "SELECT") String queryType) {
        
        try {
            logger.debug("Recording query start: {} for table: {}.{}", queryId, namespace, tableName);
            
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            queryMetricsService.recordQueryStart(queryId, tableId, queryType);
            
            return ResponseEntity.ok().build();
            
        } catch (Exception e) {
            logger.error("Failed to record query start for query: {}", queryId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Records the completion of a query for metrics tracking.
     */
    @PostMapping("/query/complete")
    public ResponseEntity<Void> recordQueryCompletion(
            @RequestParam String queryId,
            @RequestParam(defaultValue = "0") long rowsReturned,
            @RequestParam(defaultValue = "0") long bytesScanned,
            @RequestParam(defaultValue = "0") int filesScanned,
            @RequestParam(defaultValue = "0") int filesPruned) {
        
        try {
            logger.debug("Recording query completion: {} ({} rows, {} bytes, {} files scanned, {} files pruned)", 
                queryId, rowsReturned, bytesScanned, filesScanned, filesPruned);
            
            queryMetricsService.recordQueryCompletion(queryId, rowsReturned, bytesScanned, filesScanned, filesPruned);
            
            return ResponseEntity.ok().build();
            
        } catch (Exception e) {
            logger.error("Failed to record query completion for query: {}", queryId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Records a query failure for metrics tracking.
     */
    @PostMapping("/query/failure")
    public ResponseEntity<Void> recordQueryFailure(
            @RequestParam String queryId,
            @RequestParam String errorMessage) {
        
        try {
            logger.debug("Recording query failure: {} - {}", queryId, errorMessage);
            
            queryMetricsService.recordQueryFailure(queryId, errorMessage);
            
            return ResponseEntity.ok().build();
            
        } catch (Exception e) {
            logger.error("Failed to record query failure for query: {}", queryId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Records the start of a time travel query.
     */
    @PostMapping("/time-travel/start")
    public ResponseEntity<Void> recordTimeTravelQueryStart(
            @RequestParam String queryId,
            @RequestParam String namespace,
            @RequestParam String tableName,
            @RequestParam(required = false) String targetTimestamp,
            @RequestParam(required = false) Long snapshotId) {
        
        try {
            logger.debug("Recording time travel query start: {} for table: {}.{}", queryId, namespace, tableName);
            
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            java.time.Instant timestamp = targetTimestamp != null ? 
                java.time.Instant.parse(targetTimestamp) : null;
            
            queryMetricsService.recordTimeTravelQueryStart(queryId, tableId, timestamp, snapshotId);
            
            return ResponseEntity.ok().build();
            
        } catch (Exception e) {
            logger.error("Failed to record time travel query start for query: {}", queryId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Records the completion of a time travel query.
     */
    @PostMapping("/time-travel/complete")
    public ResponseEntity<Void> recordTimeTravelQueryCompletion(
            @RequestParam String queryId,
            @RequestParam(defaultValue = "0") long rowsReturned,
            @RequestParam(defaultValue = "0") int snapshotsEvaluated,
            @RequestParam(defaultValue = "true") boolean snapshotFound) {
        
        try {
            logger.debug("Recording time travel query completion: {} ({} rows, {} snapshots evaluated, snapshot found: {})", 
                queryId, rowsReturned, snapshotsEvaluated, snapshotFound);
            
            queryMetricsService.recordTimeTravelQueryCompletion(queryId, rowsReturned, snapshotsEvaluated, snapshotFound);
            
            return ResponseEntity.ok().build();
            
        } catch (Exception e) {
            logger.error("Failed to record time travel query completion for query: {}", queryId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Records the start of a transaction.
     */
    @PostMapping("/transaction/start")
    public ResponseEntity<Void> recordTransactionStart(
            @RequestParam String transactionId,
            @RequestParam String namespace,
            @RequestParam String tableName,
            @RequestParam String operationType) {
        
        try {
            logger.debug("Recording transaction start: {} for table: {}.{} ({})", 
                transactionId, namespace, tableName, operationType);
            
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            queryMetricsService.recordTransactionStart(transactionId, tableId, operationType);
            
            return ResponseEntity.ok().build();
            
        } catch (Exception e) {
            logger.error("Failed to record transaction start for transaction: {}", transactionId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Records the completion of a transaction.
     */
    @PostMapping("/transaction/complete")
    public ResponseEntity<Void> recordTransactionCompletion(
            @RequestParam String transactionId,
            @RequestParam(defaultValue = "0") long rowsAffected,
            @RequestParam(defaultValue = "0") int filesAdded,
            @RequestParam(defaultValue = "0") int filesDeleted,
            @RequestParam(defaultValue = "true") boolean committed) {
        
        try {
            logger.debug("Recording transaction completion: {} ({} rows affected, {} files added, {} files deleted, committed: {})", 
                transactionId, rowsAffected, filesAdded, filesDeleted, committed);
            
            queryMetricsService.recordTransactionCompletion(transactionId, rowsAffected, filesAdded, filesDeleted, committed);
            
            return ResponseEntity.ok().build();
            
        } catch (Exception e) {
            logger.error("Failed to record transaction completion for transaction: {}", transactionId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Gets catalog performance metrics.
     */
    @GetMapping("/catalog/performance")
    public ResponseEntity<IcebergCatalogMetricsService.CatalogPerformanceMetrics> getCatalogPerformanceMetrics() {
        try {
            logger.debug("Getting catalog performance metrics");
            
            IcebergCatalogMetricsService.CatalogPerformanceMetrics metrics = 
                catalogMetricsService.getCatalogPerformanceMetrics();
            
            return ResponseEntity.ok(metrics);
            
        } catch (Exception e) {
            logger.error("Failed to get catalog performance metrics", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Gets NYC dataset performance metrics.
     */
    @GetMapping("/nyc/performance")
    public ResponseEntity<NYCDatasetMetricsService.NYCDatasetPerformanceMetrics> getNYCDatasetPerformanceMetrics() {
        try {
            logger.debug("Getting NYC dataset performance metrics");
            
            NYCDatasetMetricsService.NYCDatasetPerformanceMetrics metrics = 
                nycDatasetMetricsService.getPerformanceMetrics();
            
            return ResponseEntity.ok(metrics);
            
        } catch (Exception e) {
            logger.error("Failed to get NYC dataset performance metrics", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Gets metrics for a specific NYC dataset.
     */
    @GetMapping("/nyc/dataset/{dataset}")
    public ResponseEntity<NYCDatasetMetricsService.DatasetMetrics> getDatasetMetrics(
            @PathVariable String dataset) {
        
        try {
            logger.debug("Getting metrics for NYC dataset: {}", dataset);
            
            NYCDatasetMetricsService.DatasetMetrics metrics = 
                nycDatasetMetricsService.getDatasetMetrics(dataset);
            
            return ResponseEntity.ok(metrics);
            
        } catch (Exception e) {
            logger.error("Failed to get metrics for dataset: {}", dataset, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Gets comprehensive metrics summary including all services.
     */
    @GetMapping("/comprehensive")
    public ResponseEntity<Map<String, Object>> getComprehensiveMetrics() {
        try {
            logger.debug("Getting comprehensive Iceberg metrics");
            
            Map<String, Object> comprehensive = new HashMap<>();
            
            // Query metrics
            IcebergQueryMetricsService.IcebergPerformanceMetrics queryMetrics = 
                queryMetricsService.getOverallPerformanceMetrics();
            comprehensive.put("query_metrics", queryMetrics);
            
            // Catalog metrics
            IcebergCatalogMetricsService.CatalogPerformanceMetrics catalogMetrics = 
                catalogMetricsService.getCatalogPerformanceMetrics();
            comprehensive.put("catalog_metrics", catalogMetrics);
            
            // NYC dataset metrics
            NYCDatasetMetricsService.NYCDatasetPerformanceMetrics nycMetrics = 
                nycDatasetMetricsService.getPerformanceMetrics();
            comprehensive.put("nyc_dataset_metrics", nycMetrics);
            
            return ResponseEntity.ok(comprehensive);
            
        } catch (Exception e) {
            logger.error("Failed to get comprehensive metrics", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Health check endpoint for metrics service.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getMetricsHealth() {
        try {
            Map<String, Object> health = new HashMap<>();
            health.put("status", "healthy");
            health.put("service", "iceberg-metrics");
            health.put("timestamp", java.time.Instant.now().toString());
            
            // Add basic metrics as health indicators
            IcebergQueryMetricsService.IcebergPerformanceMetrics metrics = 
                queryMetricsService.getOverallPerformanceMetrics();
            
            health.put("active_queries", metrics.getActiveQueries());
            health.put("active_transactions", metrics.getActiveTransactions());
            health.put("total_queries", metrics.getTotalQueries());
            
            return ResponseEntity.ok(health);
            
        } catch (Exception e) {
            logger.error("Failed to get metrics health", e);
            
            Map<String, Object> health = new HashMap<>();
            health.put("status", "unhealthy");
            health.put("service", "iceberg-metrics");
            health.put("error", e.getMessage());
            health.put("timestamp", java.time.Instant.now().toString());
            
            return ResponseEntity.status(500).body(health);
        }
    }
}