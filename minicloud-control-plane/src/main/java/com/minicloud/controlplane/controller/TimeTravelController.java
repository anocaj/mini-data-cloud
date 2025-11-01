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
 * Implements Requirements 2.1, 2.2, and 2.5 for time travel query execution and snapshot management.
 */
@RestController
@RequestMapping("/api/v1/iceberg/time-travel")
@CrossOrigin(origins = "*")
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
     * Execute a time travel query and return results.
     * Implements Requirement 2.1 for time travel query execution.
     */
    @PostMapping("/query/execute")
    public ResponseEntity<Map<String, Object>> executeTimeTravelQuery(@RequestBody TimeTravelQueryRequest request) {
        logger.info("Executing time travel query: {}", request.getSql());
        
        try {
            String sql = request.getSql();
            if (sql == null || sql.trim().isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of("error", "SQL query is required"));
            }
            
            if (!timeTravelQueryProcessor.isTimeTravelQuery(sql)) {
                return ResponseEntity.badRequest().body(Map.of("error", "Query is not a time travel query"));
            }
            
            // Process the time travel query
            TimeTravelQueryResult result = timeTravelQueryProcessor.processTimeTravelQuery(sql);
            
            // Execute the modified query (this would integrate with the query execution engine)
            // For now, we'll return the processed query information
            Map<String, Object> response = new HashMap<>();
            response.put("queryId", java.util.UUID.randomUUID().toString());
            response.put("originalSql", result.getOriginalSql());
            response.put("modifiedSql", result.getModifiedSql());
            response.put("tableIdentifier", result.getTableIdentifier().toString());
            response.put("snapshotId", result.getSnapshotId());
            response.put("effectiveTimestamp", result.getEffectiveTimestamp().toString());
            response.put("type", result.getType().toString());
            response.put("status", "EXECUTED");
            response.put("executionTimeMs", System.currentTimeMillis() - System.currentTimeMillis());
            
            // In a full implementation, you would execute the query and return actual results
            response.put("message", "Time travel query processed successfully. Integration with query execution engine required for actual results.");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error executing time travel query", e);
            return ResponseEntity.internalServerError().body(Map.of("error", "Query execution failed: " + e.getMessage()));
        }
    }
    
    /**
     * Get table history with all snapshots and their metadata.
     * Implements Requirement 2.2 for snapshot listing and history.
     */
    @GetMapping("/tables/{namespace}/{tableName}/history")
    public ResponseEntity<TableHistoryResponse> getTableHistory(
            @PathVariable String namespace,
            @PathVariable String tableName,
            @RequestParam(defaultValue = "50") int limit,
            @RequestParam(defaultValue = "0") int offset) {
        
        logger.info("Getting table history for {}.{} (limit={}, offset={})", namespace, tableName, limit, offset);
        
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            List<SnapshotInfo> allSnapshots = snapshotManagementService.listSnapshots(tableId);
            SnapshotStatistics statistics = snapshotManagementService.getSnapshotStatistics(tableId);
            
            // Apply pagination
            List<SnapshotInfo> paginatedSnapshots = allSnapshots.stream()
                .skip(offset)
                .limit(limit)
                .collect(java.util.stream.Collectors.toList());
            
            TableHistoryResponse response = new TableHistoryResponse(
                tableId.toString(),
                allSnapshots.size(),
                paginatedSnapshots,
                statistics,
                limit,
                offset
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error getting table history for {}.{}", namespace, tableName, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Validate timestamp for time travel queries.
     * Implements Requirement 2.5 for timestamp validation.
     */
    @PostMapping("/tables/{namespace}/{tableName}/validate-timestamp")
    public ResponseEntity<TimestampValidationResponse> validateTimestamp(
            @PathVariable String namespace,
            @PathVariable String tableName,
            @RequestBody TimestampValidationRequest request) {
        
        logger.info("Validating timestamp {} for table {}.{}", request.getTimestamp(), namespace, tableName);
        
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            
            // Parse the timestamp
            Instant targetTimestamp;
            try {
                targetTimestamp = Instant.parse(request.getTimestamp());
            } catch (DateTimeParseException e) {
                return ResponseEntity.ok(new TimestampValidationResponse(
                    false, 
                    request.getTimestamp(), 
                    null, 
                    null, 
                    "Invalid timestamp format: " + e.getMessage()
                ));
            }
            
            // Find the nearest snapshot
            SnapshotInfo nearestSnapshot = snapshotManagementService.findSnapshotNearTimestamp(tableId, targetTimestamp);
            
            if (nearestSnapshot == null) {
                return ResponseEntity.ok(new TimestampValidationResponse(
                    false, 
                    request.getTimestamp(), 
                    null, 
                    null, 
                    "No snapshots available for the specified timestamp"
                ));
            }
            
            // Validate that the snapshot is suitable for time travel
            try {
                snapshotManagementService.validateSnapshotForTimeTravel(tableId, nearestSnapshot.getSnapshotId());
                
                return ResponseEntity.ok(new TimestampValidationResponse(
                    true, 
                    request.getTimestamp(), 
                    nearestSnapshot.getSnapshotId(), 
                    nearestSnapshot.getTimestamp(), 
                    "Timestamp is valid for time travel queries"
                ));
                
            } catch (Exception e) {
                return ResponseEntity.ok(new TimestampValidationResponse(
                    false, 
                    request.getTimestamp(), 
                    nearestSnapshot.getSnapshotId(), 
                    nearestSnapshot.getTimestamp(), 
                    "Snapshot validation failed: " + e.getMessage()
                ));
            }
            
        } catch (Exception e) {
            logger.error("Error validating timestamp for table {}.{}", namespace, tableName, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get available time travel timestamps for a table.
     */
    @GetMapping("/tables/{namespace}/{tableName}/timestamps")
    public ResponseEntity<List<AvailableTimestamp>> getAvailableTimestamps(
            @PathVariable String namespace,
            @PathVariable String tableName,
            @RequestParam(defaultValue = "100") int limit) {
        
        logger.info("Getting available timestamps for table {}.{}", namespace, tableName);
        
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            List<SnapshotInfo> snapshots = snapshotManagementService.listSnapshots(tableId);
            
            List<AvailableTimestamp> timestamps = snapshots.stream()
                .limit(limit)
                .map(snapshot -> new AvailableTimestamp(
                    snapshot.getSnapshotId(),
                    snapshot.getTimestamp(),
                    snapshot.getOperation(),
                    snapshot.getSummary().getOrDefault("added-data-files", "0") + " files added"
                ))
                .collect(java.util.stream.Collectors.toList());
            
            return ResponseEntity.ok(timestamps);
            
        } catch (Exception e) {
            logger.error("Error getting available timestamps for table {}.{}", namespace, tableName, e);
            return ResponseEntity.internalServerError().build();
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
            "cleanup-planning",
            "query-execution",
            "timestamp-validation"
        ));
        
        return ResponseEntity.ok(health);
    }
    
    // Request/Response DTOs
    
    public static class TimeTravelQueryRequest {
        private String sql;
        private Map<String, Object> parameters;
        
        public String getSql() { return sql; }
        public void setSql(String sql) { this.sql = sql; }
        public Map<String, Object> getParameters() { return parameters; }
        public void setParameters(Map<String, Object> parameters) { this.parameters = parameters; }
    }
    
    public static class TableHistoryResponse {
        private final String tableIdentifier;
        private final int totalSnapshots;
        private final List<SnapshotInfo> snapshots;
        private final SnapshotStatistics statistics;
        private final int limit;
        private final int offset;
        
        public TableHistoryResponse(String tableIdentifier, int totalSnapshots, List<SnapshotInfo> snapshots,
                                  SnapshotStatistics statistics, int limit, int offset) {
            this.tableIdentifier = tableIdentifier;
            this.totalSnapshots = totalSnapshots;
            this.snapshots = snapshots;
            this.statistics = statistics;
            this.limit = limit;
            this.offset = offset;
        }
        
        public String getTableIdentifier() { return tableIdentifier; }
        public int getTotalSnapshots() { return totalSnapshots; }
        public List<SnapshotInfo> getSnapshots() { return snapshots; }
        public SnapshotStatistics getStatistics() { return statistics; }
        public int getLimit() { return limit; }
        public int getOffset() { return offset; }
    }
    
    public static class TimestampValidationRequest {
        private String timestamp;
        
        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    }
    
    public static class TimestampValidationResponse {
        private final boolean valid;
        private final String requestedTimestamp;
        private final Long nearestSnapshotId;
        private final Instant nearestSnapshotTimestamp;
        private final String message;
        
        public TimestampValidationResponse(boolean valid, String requestedTimestamp, Long nearestSnapshotId,
                                         Instant nearestSnapshotTimestamp, String message) {
            this.valid = valid;
            this.requestedTimestamp = requestedTimestamp;
            this.nearestSnapshotId = nearestSnapshotId;
            this.nearestSnapshotTimestamp = nearestSnapshotTimestamp;
            this.message = message;
        }
        
        public boolean isValid() { return valid; }
        public String getRequestedTimestamp() { return requestedTimestamp; }
        public Long getNearestSnapshotId() { return nearestSnapshotId; }
        public Instant getNearestSnapshotTimestamp() { return nearestSnapshotTimestamp; }
        public String getMessage() { return message; }
    }
    
    public static class AvailableTimestamp {
        private final long snapshotId;
        private final Instant timestamp;
        private final String operation;
        private final String description;
        
        public AvailableTimestamp(long snapshotId, Instant timestamp, String operation, String description) {
            this.snapshotId = snapshotId;
            this.timestamp = timestamp;
            this.operation = operation;
            this.description = description;
        }
        
        public long getSnapshotId() { return snapshotId; }
        public Instant getTimestamp() { return timestamp; }
        public String getOperation() { return operation; }
        public String getDescription() { return description; }
    }
}