package com.minicloud.controlplane.service;

import com.minicloud.controlplane.sql.ParsedQuery;
import com.minicloud.controlplane.sql.SqlParsingService;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.Table;
import org.apache.iceberg.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Service for processing time travel queries with AS OF TIMESTAMP syntax.
 * Implements SQL extensions for historical data access using Iceberg snapshots.
 */
@Service
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class TimeTravelQueryProcessor {
    
    private static final Logger logger = LoggerFactory.getLogger(TimeTravelQueryProcessor.class);
    
    // Pattern to match AS OF TIMESTAMP syntax in SQL
    private static final Pattern AS_OF_TIMESTAMP_PATTERN = Pattern.compile(
        "\\bAS\\s+OF\\s+TIMESTAMP\\s+'([^']+)'", 
        Pattern.CASE_INSENSITIVE
    );
    
    // Pattern to match AS OF SNAPSHOT syntax in SQL
    private static final Pattern AS_OF_SNAPSHOT_PATTERN = Pattern.compile(
        "\\bAS\\s+OF\\s+SNAPSHOT\\s+(\\d+)", 
        Pattern.CASE_INSENSITIVE
    );
    
    // Supported timestamp formats
    private static final DateTimeFormatter[] TIMESTAMP_FORMATTERS = {
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
        DateTimeFormatter.ISO_LOCAL_DATE_TIME
    };
    
    private final GlobalCatalogService globalCatalogService;
    private final SqlParsingService sqlParsingService;
    private final SnapshotManagementService snapshotManagementService;
    
    @Autowired
    public TimeTravelQueryProcessor(
            GlobalCatalogService globalCatalogService,
            SqlParsingService sqlParsingService,
            SnapshotManagementService snapshotManagementService) {
        this.globalCatalogService = globalCatalogService;
        this.sqlParsingService = sqlParsingService;
        this.snapshotManagementService = snapshotManagementService;
    }
    
    /**
     * Checks if a SQL query contains time travel syntax.
     */
    public boolean isTimeTravelQuery(String sql) {
        return AS_OF_TIMESTAMP_PATTERN.matcher(sql).find() || 
               AS_OF_SNAPSHOT_PATTERN.matcher(sql).find();
    }
    
    /**
     * Processes a time travel query and returns the modified query with snapshot information.
     */
    public TimeTravelQueryResult processTimeTravelQuery(String originalSql) {
        logger.info("Processing time travel query: {}", originalSql.substring(0, Math.min(originalSql.length(), 100)) + "...");
        
        try {
            // Check for AS OF TIMESTAMP syntax
            Matcher timestampMatcher = AS_OF_TIMESTAMP_PATTERN.matcher(originalSql);
            if (timestampMatcher.find()) {
                String timestampStr = timestampMatcher.group(1);
                return processTimestampQuery(originalSql, timestampStr, timestampMatcher);
            }
            
            // Check for AS OF SNAPSHOT syntax
            Matcher snapshotMatcher = AS_OF_SNAPSHOT_PATTERN.matcher(originalSql);
            if (snapshotMatcher.find()) {
                String snapshotIdStr = snapshotMatcher.group(1);
                return processSnapshotQuery(originalSql, snapshotIdStr, snapshotMatcher);
            }
            
            throw new TimeTravelQueryException("No valid time travel syntax found in query");
            
        } catch (Exception e) {
            logger.error("Failed to process time travel query", e);
            throw new TimeTravelQueryException("Failed to process time travel query: " + e.getMessage(), e);
        }
    }
    
    /**
     * Processes a query with AS OF TIMESTAMP syntax.
     */
    private TimeTravelQueryResult processTimestampQuery(String originalSql, String timestampStr, Matcher matcher) {
        logger.debug("Processing AS OF TIMESTAMP query with timestamp: {}", timestampStr);
        
        // Parse the timestamp
        Instant timestamp = parseTimestamp(timestampStr);
        
        // Extract table name from the query
        String tableName = extractTableName(originalSql);
        TableIdentifier tableIdentifier = TableIdentifier.of("default", tableName);
        
        // Validate table exists
        if (!globalCatalogService.tableExists(tableIdentifier)) {
            throw new TimeTravelQueryException("Table not found: " + tableName);
        }
        
        // Find the appropriate snapshot for the timestamp
        Table table = globalCatalogService.loadTable(tableIdentifier);
        Snapshot targetSnapshot = findSnapshotAtTimestamp(table, timestamp);
        
        if (targetSnapshot == null) {
            throw new TimeTravelQueryException("No snapshot found for timestamp: " + timestampStr);
        }
        
        // Validate snapshot with snapshot management service
        snapshotManagementService.validateSnapshotForTimeTravel(tableIdentifier, targetSnapshot.snapshotId());
        
        // Remove the AS OF TIMESTAMP clause from the SQL
        String modifiedSql = matcher.replaceFirst("");
        
        logger.info("Time travel query processed: table={}, timestamp={}, snapshot={}", 
                   tableName, timestamp, targetSnapshot.snapshotId());
        
        return new TimeTravelQueryResult(
            originalSql,
            modifiedSql,
            tableIdentifier,
            targetSnapshot,
            timestamp,
            TimeTravelQueryResult.TimeTravelType.TIMESTAMP
        );
    }
    
    /**
     * Processes a query with AS OF SNAPSHOT syntax.
     */
    private TimeTravelQueryResult processSnapshotQuery(String originalSql, String snapshotIdStr, Matcher matcher) {
        logger.debug("Processing AS OF SNAPSHOT query with snapshot ID: {}", snapshotIdStr);
        
        long snapshotId;
        try {
            snapshotId = Long.parseLong(snapshotIdStr);
        } catch (NumberFormatException e) {
            throw new TimeTravelQueryException("Invalid snapshot ID: " + snapshotIdStr);
        }
        
        // Extract table name from the query
        String tableName = extractTableName(originalSql);
        TableIdentifier tableIdentifier = TableIdentifier.of("default", tableName);
        
        // Validate table exists
        if (!globalCatalogService.tableExists(tableIdentifier)) {
            throw new TimeTravelQueryException("Table not found: " + tableName);
        }
        
        // Load table and validate snapshot exists
        Table table = globalCatalogService.loadTable(tableIdentifier);
        Snapshot targetSnapshot = table.snapshot(snapshotId);
        
        if (targetSnapshot == null) {
            throw new TimeTravelQueryException("Snapshot not found: " + snapshotId);
        }
        
        // Validate snapshot with snapshot management service
        snapshotManagementService.validateSnapshotForTimeTravel(tableIdentifier, snapshotId);
        
        // Remove the AS OF SNAPSHOT clause from the SQL
        String modifiedSql = matcher.replaceFirst("");
        
        logger.info("Time travel query processed: table={}, snapshot={}", tableName, snapshotId);
        
        return new TimeTravelQueryResult(
            originalSql,
            modifiedSql,
            tableIdentifier,
            targetSnapshot,
            Instant.ofEpochMilli(targetSnapshot.timestampMillis()),
            TimeTravelQueryResult.TimeTravelType.SNAPSHOT
        );
    }
    
    /**
     * Parses a timestamp string using various supported formats.
     */
    private Instant parseTimestamp(String timestampStr) {
        for (DateTimeFormatter formatter : TIMESTAMP_FORMATTERS) {
            try {
                LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                return localDateTime.toInstant(ZoneOffset.UTC);
            } catch (DateTimeParseException e) {
                // Try next formatter
            }
        }
        
        // Try parsing as ISO instant
        try {
            return Instant.parse(timestampStr);
        } catch (DateTimeParseException e) {
            throw new TimeTravelQueryException("Invalid timestamp format: " + timestampStr + 
                ". Supported formats: yyyy-MM-dd HH:mm:ss, yyyy-MM-dd'T'HH:mm:ss, ISO format");
        }
    }
    
    /**
     * Extracts the table name from a SQL query.
     * This is a simplified implementation that looks for the FROM clause.
     */
    private String extractTableName(String sql) {
        // Simple regex to extract table name from FROM clause
        Pattern fromPattern = Pattern.compile("\\bFROM\\s+([\\w_]+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = fromPattern.matcher(sql);
        
        if (matcher.find()) {
            return matcher.group(1);
        }
        
        throw new TimeTravelQueryException("Could not extract table name from query");
    }
    
    /**
     * Finds the snapshot that was active at the specified timestamp.
     */
    private Snapshot findSnapshotAtTimestamp(Table table, Instant timestamp) {
        long timestampMs = timestamp.toEpochMilli();
        Snapshot targetSnapshot = null;
        
        // Find the latest snapshot that was created before or at the specified timestamp
        for (Snapshot snapshot : table.snapshots()) {
            if (snapshot.timestampMillis() <= timestampMs) {
                if (targetSnapshot == null || snapshot.timestampMillis() > targetSnapshot.timestampMillis()) {
                    targetSnapshot = snapshot;
                }
            }
        }
        
        return targetSnapshot;
    }
    
    /**
     * Creates a snapshot-based query planner for executing time travel queries.
     */
    public SnapshotQueryPlanner createSnapshotQueryPlanner(TimeTravelQueryResult timeTravelResult) {
        return new SnapshotQueryPlanner(
            timeTravelResult,
            globalCatalogService,
            snapshotManagementService
        );
    }
    
    /**
     * Result of processing a time travel query.
     */
    public static class TimeTravelQueryResult {
        
        public enum TimeTravelType {
            TIMESTAMP, SNAPSHOT
        }
        
        private final String originalSql;
        private final String modifiedSql;
        private final TableIdentifier tableIdentifier;
        private final Snapshot targetSnapshot;
        private final Instant effectiveTimestamp;
        private final TimeTravelType type;
        
        public TimeTravelQueryResult(String originalSql, String modifiedSql, TableIdentifier tableIdentifier,
                                   Snapshot targetSnapshot, Instant effectiveTimestamp, TimeTravelType type) {
            this.originalSql = originalSql;
            this.modifiedSql = modifiedSql;
            this.tableIdentifier = tableIdentifier;
            this.targetSnapshot = targetSnapshot;
            this.effectiveTimestamp = effectiveTimestamp;
            this.type = type;
        }
        
        public String getOriginalSql() { return originalSql; }
        public String getModifiedSql() { return modifiedSql; }
        public TableIdentifier getTableIdentifier() { return tableIdentifier; }
        public Snapshot getTargetSnapshot() { return targetSnapshot; }
        public Instant getEffectiveTimestamp() { return effectiveTimestamp; }
        public TimeTravelType getType() { return type; }
        
        public long getSnapshotId() { return targetSnapshot.snapshotId(); }
        
        @Override
        public String toString() {
            return "TimeTravelQueryResult{" +
                    "table=" + tableIdentifier +
                    ", snapshotId=" + targetSnapshot.snapshotId() +
                    ", timestamp=" + effectiveTimestamp +
                    ", type=" + type +
                    '}';
        }
    }
    
    /**
     * Exception thrown when time travel query processing fails.
     */
    public static class TimeTravelQueryException extends RuntimeException {
        public TimeTravelQueryException(String message) {
            super(message);
        }
        
        public TimeTravelQueryException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}