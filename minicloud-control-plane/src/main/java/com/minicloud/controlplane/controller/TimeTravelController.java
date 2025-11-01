package com.minicloud.controlplane.controller;

import com.minicloud.controlplane.service.TimeTravelQueryProcessor;
import com.minicloud.controlplane.service.SnapshotManagementService;
import com.minicloud.controlplane.service.HistoricalDataAccessLayer;
import com.minicloud.controlplane.service.TimeTravelQueryProcessor.TimeTravelQueryResult;
import com.minicloud.controlplane.service.SnapshotManagementService.SnapshotInfo;
import com.minicloud.controlplane.service.SnapshotManagementService.SnapshotStatistics;
import com.minicloud.controlplane.service.SnapshotManagementService.SnapshotCleanupResult;
import com.minicloud.controlplane.service.HistoricalDataAccessLayer.HistoricalDataResult;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * REST controller for time travel query operations and snapshot management.
 * Provides endpoints for executing time travel queries and managing table snapshots.
 */
@RestController
@RequestMapping("/api/time-travel")
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class TimeTravelController {
    
    private static final Logger logger = LoggerFactory.getLogger(TimeTravelController.class);
    
    private final TimeTravelQueryProcessor timeTravelQueryProcessor;
    private final SnapshotManagementService snapshotManagementService;
    private final HistoricalDataAccessLayer historicalDataAccessLayer;
    
    @Autowired
    public TimeTravelController(TimeTravelQueryProcessor timeTravelQueryProcessor,
                               SnapshotManagementService snapshotManagementService,
                               HistoricalDataAccessLayer historicalDataAccessLayer) {
        this.timeTravelQueryProcessor = timeTravelQueryProcessor;
        this.snapshotManagementService = snapshotManagementService;
        this.historicalDataAccessLayer = historicalDataAccessLayer;
    }
    
    /**
     * Validates if a query contains time travel syntax.
     */
    @PostMapping("/validate")
    public ResponseEntity<Map<String, Object>> validateTimeTravelQuery(@RequestBody Map<String, String> request) {
        logger.info("Validating time travel query");
        
        try {
            String sql = request.get("sql");
            if (sql == null || sql.trim().isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of(
                    "error", "SQL query is required",
                    "isTimeTravelQuery", false
                ));
            }
            
            boolean isTimeTravelQuery = timeTravelQueryProcessor.isTimeTravelQuery(sql);
            
            Map<String, Object> response = new HashMap<>();
            response.put("isTimeTravelQuery", isTimeTravelQuery);
            response.put("sql", sql);
            
            if (isTimeTravelQuery) {
                try {
                    TimeTravelQueryResult result = timeTravelQueryProcessor.processTimeTravelQuery(sql);
                    response.put("valid", true);
                    response.put("tableIdentifier", result.getTableIdentifier().toString());
                    response.put("snapshotId", result.getSnapshotId());
                    response.put("effectiveTimestamp", result.getEffectiveTimestamp().toString());
                    response.put("type", result.getType().toString());
                } catch (Exception e) {
                    response.put("valid", false);
                    response.put("error", e.getMessage());
                }
            } else {
                response.put("valid", false);
                response.put("error", "Query does not contain time travel syntax");
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error validating time travel query", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "error", "Validation failed: " + e.getMessage(),
                "isTimeTravelQuery", false
            ));
        }
    }
    
    /**
     * Processes a time travel query and returns the execution plan.
     */
    @PostMapping("/process")
    public ResponseEntity<Map<String, Object>> processTimeTravelQuery(@RequestBody Map<String, String> request) {
        logger.info("Processing time travel query");
        
        try {
            String sql = request.get("sql");
            if (sql == null || sql.trim().isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of("error", "SQL query is required"));
            }
            
            if (!timeTravelQueryProcessor.isTimeTravelQuery(sql)) {
                return ResponseEntity.badRequest().body(Map.of("error", "Query is not a time travel query"));
            }
            
            TimeTravelQueryResult result = timeTravelQueryProcessor.processTimeTravelQuery(sql);
            
            Map<String, Object> response = new HashMap<>();
            response.put("originalSql", result.getOriginalSql());
            response.put("modifiedSql", result.getModifiedSql());
            response.put("tableIdentifier", result.getTableIdentifier().toString());
            response.put("snapshotId", result.getSnapshotId());
            response.put("effectiveTimestamp", result.getEffectiveTimestamp().toString());
            response.put("type", result.getType().toString());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error processing time travel query", e);
            return ResponseEntity.internalServerError().body(Map.of("error", "Processing failed: " + e.getMessage()));
        }
    }
    
    /**
     * Lists all snapshots for a table.
     */
    @GetMapping("/snapshots/{namespace}/{tableName}")
    public ResponseEntity<List<SnapshotInfo>> listTableSnapshots(
            @PathVariable String namespace,
            @PathVariable String tableName) {
        logger.info("Listing snapshots for table: {}.{}", namespace, tableName);
        
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            List<SnapshotInfo> snapshots = snapshotManagementService.listSnapshots(tableId);
            
            return ResponseEntity.ok(snapshots);
            
        } catch (Exception e) {
            logger.error("Error listing snapshots for table: {}.{}", namespace, tableName, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Gets information about a specific snapshot.
     */
    @GetMapping("/snapshots/{namespace}/{tableName}/{snapshotId}")
    public ResponseEntity<SnapshotInfo> getSnapshotInfo(
            @PathVariable String namespace,
            @PathVariable String tableName,
            @PathVariable long snapshotId) {
        logger.info("Getting snapshot info: {}.{} snapshot: {}", namespace, tableName, snapshotId);
        
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            SnapshotInfo snapshotInfo = snapshotManagementService.getSnapshotInfo(tableId, snapshotId);
            
            return ResponseEntity.ok(snapshotInfo);
            
        } catch (Exception e) {
            logger.error("Error getting snapshot info: {}.{} snapshot: {}", namespace, tableName, snapshotId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Gets snapshot statistics for a table.
     */
    @GetMapping("/snapshots/{namespace}/{tableName}/statistics")
    public ResponseEntity<SnapshotStatistics> getSnapshotStatistics(
            @PathVariable String namespace,
            @PathVariable String tableName) {
        logger.info("Getting snapshot statistics for table: {}.{}", namespace, tableName);
        
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            SnapshotStatistics statistics = snapshotManagementService.getSnapshotStatistics(tableId);
            
            return ResponseEntity.ok(statistics);
            
        } catch (Exception e) {
            logger.error("Error getting snapshot statistics for table: {}.{}", namespace, tableName, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Finds snapshots within a time range.
     */
    @GetMapping("/snapshots/{namespace}/{tableName}/range")
    public ResponseEntity<List<SnapshotInfo>> findSnapshotsInRange(
            @PathVariable String namespace,
            @PathVariable String tableName,
            @RequestParam String startTime,
            @RequestParam String endTime) {
        logger.info("Finding snapshots in range for table: {}.{} from {} to {}", 
                   namespace, tableName, startTime, endTime);
        
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            
            Instant start = Instant.parse(startTime);
            Instant end = Instant.parse(endTime);
            
            List<SnapshotInfo> snapshots = snapshotManagementService.findSnapshotsInTimeRange(tableId, start, end);
            
            return ResponseEntity.ok(snapshots);
            
        } catch (DateTimeParseException e) {
            logger.error("Invalid timestamp format", e);
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            logger.error("Error finding snapshots in range for table: {}.{}", namespace, tableName, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Finds the snapshot closest to a specific timestamp.
     */
    @GetMapping("/snapshots/{namespace}/{tableName}/nearest")
    public ResponseEntity<SnapshotInfo> findSnapshotNearTimestamp(
            @PathVariable String namespace,
            @PathVariable String tableName,
            @RequestParam String timestamp) {
        logger.info("Finding snapshot nearest to {} for table: {}.{}", timestamp, namespace, tableName);
        
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            Instant targetTime = Instant.parse(timestamp);
            
            SnapshotInfo snapshot = snapshotManagementService.findSnapshotNearTimestamp(tableId, targetTime);
            
            return ResponseEntity.ok(snapshot);
            
        } catch (DateTimeParseException e) {
            logger.error("Invalid timestamp format", e);
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            logger.error("Error finding nearest snapshot for table: {}.{}", namespace, tableName, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Plans snapshot cleanup for a table.
     */
    @GetMapping("/snapshots/{namespace}/{tableName}/cleanup/plan")
    public ResponseEntity<SnapshotCleanupResult> planSnapshotCleanup(
            @PathVariable String namespace,
            @PathVariable String tableName,
            @RequestParam(defaultValue = "30") int retentionDays) {
        logger.info("Planning snapshot cleanup for table: {}.{} (retention: {} days)", 
                   namespace, tableName, retentionDays);
        
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            SnapshotCleanupResult result = snapshotManagementService.planSnapshotCleanup(tableId, retentionDays);
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            logger.error("Error planning snapshot cleanup for table: {}.{}", namespace, tableName, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Reads a sample of data from a specific snapshot.
     */
    @GetMapping("/data/{namespace}/{tableName}/{snapshotId}/sample")
    public ResponseEntity<Map<String, Object>> readSnapshotSample(
            @PathVariable String namespace,
            @PathVariable String tableName,
            @PathVariable long snapshotId,
            @RequestParam(defaultValue = "100") int sampleSize) {
        logger.info("Reading sample from snapshot: {}.{} snapshot: {} size: {}", 
                   namespace, tableName, snapshotId, sampleSize);
        
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            HistoricalDataResult result = historicalDataAccessLayer.readSnapshotSample(tableId, snapshotId, sampleSize);
            
            Map<String, Object> response = new HashMap<>();
            response.put("tableIdentifier", result.getTableIdentifier().toString());
            response.put("snapshotId", result.getSnapshotId());
            response.put("effectiveTimestamp", result.getEffectiveTimestamp().toString());
            response.put("recordCount", result.getRecordCount());
            response.put("schema", result.getSchema().toString());
            response.put("records", result.getRecordsAsMap());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error reading snapshot sample: {}.{} snapshot: {}", namespace, tableName, snapshotId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Validates a snapshot for time travel queries.
     */
    @PostMapping("/snapshots/{namespace}/{tableName}/{snapshotId}/validate")
    public ResponseEntity<Map<String, Object>> validateSnapshot(
            @PathVariable String namespace,
            @PathVariable String tableName,
            @PathVariable long snapshotId) {
        logger.info("Validating snapshot for time travel: {}.{} snapshot: {}", namespace, tableName, snapshotId);
        
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            snapshotManagementService.validateSnapshotForTimeTravel(tableId, snapshotId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("valid", true);
            response.put("snapshotId", snapshotId);
            response.put("message", "Snapshot is valid for time travel queries");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Snapshot validation failed: {}.{} snapshot: {}", namespace, tableName, snapshotId, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("valid", false);
            response.put("snapshotId", snapshotId);
            response.put("error", e.getMessage());
            
            return ResponseEntity.ok(response);
        }
    }
    
    /**
     * Health check endpoint for time travel functionality.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("service", "TimeTravelController");
        health.put("timestamp", Instant.now().toString());
        health.put("features", List.of(
            "time-travel-queries",
            "snapshot-management", 
            "historical-data-access",
            "snapshot-validation",
            "cleanup-planning"
        ));
        
        return ResponseEntity.ok(health);
    }
}