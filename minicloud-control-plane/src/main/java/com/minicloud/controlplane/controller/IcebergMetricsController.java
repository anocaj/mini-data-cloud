package com.minicloud.controlplane.controller;

import com.minicloud.controlplane.service.IcebergQueryMetricsService;
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
    
    private final IcebergQueryMetricsService metricsService;
    
    @Autowired
    public IcebergMetricsController(IcebergQueryMetricsService metricsService) {
        this.metricsService = metricsService;
    }
    
    /**
     * Gets overall Iceberg performance metrics.
     */
    @GetMapping("/performance")
    public ResponseEntity<IcebergQueryMetricsService.IcebergPerformanceMetrics> getOverallPerformanceMetrics() {
        try {
            logger.debug("Getting overall Iceberg performance metrics");
            
            IcebergQueryMetricsService.IcebergPerformanceMetrics metrics = 
                metricsService.getOverallPerformanceMetrics();
            
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
                metricsService.getTablePerformanceMetrics(tableId);
            
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
                metricsService.getOverallPerformanceMetrics();
            
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
            metricsService.recordQueryStart(queryId, tableId, queryType);
            
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
            
            metricsService.recordQueryCompletion(queryId, rowsReturned, bytesScanned, filesScanned, filesPruned);
            
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
            
            metricsService.recordQueryFailure(queryId, errorMessage);
            
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
            
            metricsService.recordTimeTravelQueryStart(queryId, tableId, timestamp, snapshotId);
            
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
            
            metricsService.recordTimeTravelQueryCompletion(queryId, rowsReturned, snapshotsEvaluated, snapshotFound);
            
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
            metricsService.recordTransactionStart(transactionId, tableId, operationType);
            
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
            
            metricsService.recordTransactionCompletion(transactionId, rowsAffected, filesAdded, filesDeleted, committed);
            
            return ResponseEntity.ok().build();
            
        } catch (Exception e) {
            logger.error("Failed to record transaction completion for transaction: {}", transactionId, e);
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
                metricsService.getOverallPerformanceMetrics();
            
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