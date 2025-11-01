package com.minicloud.controlplane.service;

import com.minicloud.controlplane.service.SnapshotQueryPlanner.SnapshotQueryPlan;
import com.minicloud.controlplane.service.TimeTravelQueryProcessor.TimeTravelQueryResult;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.Table;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * Service for accessing historical data from Iceberg snapshots.
 * Provides low-level data access capabilities for time travel queries.
 */
@Service
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class HistoricalDataAccessLayer {
    
    private static final Logger logger = LoggerFactory.getLogger(HistoricalDataAccessLayer.class);
    
    private final GlobalCatalogService globalCatalogService;
    private final SnapshotManagementService snapshotManagementService;
    
    @Autowired
    public HistoricalDataAccessLayer(GlobalCatalogService globalCatalogService,
                                   SnapshotManagementService snapshotManagementService) {
        this.globalCatalogService = globalCatalogService;
        this.snapshotManagementService = snapshotManagementService;
    }
    
    /**
     * Reads data from a specific snapshot of a table.
     */
    public HistoricalDataResult readSnapshotData(SnapshotQueryPlan queryPlan) {
        return readSnapshotData(queryPlan, null, -1);
    }
    
    /**
     * Reads data from a specific snapshot with optional filtering and limiting.
     */
    public HistoricalDataResult readSnapshotData(SnapshotQueryPlan queryPlan, 
                                               Expression filter, int limit) {
        logger.info("Reading data from snapshot: {} for table: {}", 
                   queryPlan.getSnapshotId(), queryPlan.getTableIdentifier());
        
        try {
            // Load the table
            Table table = globalCatalogService.loadTable(queryPlan.getTableIdentifier());
            
            // Create a table scan at the specific snapshot
            var tableScan = table.newScan().useSnapshot(queryPlan.getSnapshotId());
            
            // Apply filter if provided
            if (filter != null) {
                tableScan = tableScan.filter(filter);
                logger.debug("Applied filter to snapshot scan: {}", filter);
            }
            
            // Read the data
            List<Record> records = new ArrayList<>();
            Schema schema = table.schema();
            
            // Note: In a full implementation, this would use Iceberg's data reading APIs
            // For now, we'll create a mock implementation that simulates reading records
            List<Record> mockRecords = createMockRecords(schema, limit > 0 ? limit : 10);
            records.addAll(mockRecords);
            
            logger.info("Read {} records from snapshot {}", records.size(), queryPlan.getSnapshotId());
            
            return new HistoricalDataResult(
                queryPlan.getTableIdentifier(),
                queryPlan.getSnapshot(),
                schema,
                records,
                queryPlan.getEffectiveTimestamp(),
                filter != null ? filter.toString() : null
            );
            
        } catch (Exception e) {
            logger.error("Error accessing historical data", e);
            throw new RuntimeException("Historical data access failed: " + e.getMessage(), e);
        }
    }
    
    /**
     * Reads a sample of data from a snapshot for preview purposes.
     */
    public HistoricalDataResult readSnapshotSample(TableIdentifier tableId, long snapshotId, int sampleSize) {
        logger.debug("Reading sample of {} records from snapshot: {}", sampleSize, snapshotId);
        
        // Validate snapshot exists and is accessible
        snapshotManagementService.validateSnapshotForTimeTravel(tableId, snapshotId);
        
        // Load table and get snapshot
        Table table = globalCatalogService.loadTable(tableId);
        Snapshot snapshot = table.snapshot(snapshotId);
        
        if (snapshot == null) {
            throw new RuntimeException("Snapshot not found: " + snapshotId);
        }
        
        // Create a simple query plan for sampling
        SnapshotQueryPlan samplePlan = new SnapshotQueryPlan(
            "SELECT * FROM " + tableId.name() + " LIMIT " + sampleSize,
            "SELECT * FROM " + tableId.name() + " LIMIT " + sampleSize,
            tableId,
            snapshot,
            new ArrayList<>(), // Data files not needed for sampling
            Instant.ofEpochMilli(snapshot.timestampMillis())
        );
        
        return readSnapshotData(samplePlan, null, sampleSize);
    }
    
    /**
     * Gets schema information for a table at a specific snapshot.
     */
    public Schema getSnapshotSchema(TableIdentifier tableId, long snapshotId) {
        logger.debug("Getting schema for table {} at snapshot {}", tableId, snapshotId);
        
        // Validate snapshot
        snapshotManagementService.validateSnapshotForTimeTravel(tableId, snapshotId);
        
        // Load table - schema should be consistent across snapshots for the same table
        Table table = globalCatalogService.loadTable(tableId);
        return table.schema();
    }
    
    /**
     * Counts records in a snapshot with optional filtering.
     */
    public long countSnapshotRecords(TableIdentifier tableId, long snapshotId, Expression filter) {
        logger.debug("Counting records in snapshot {} with filter: {}", snapshotId, filter);
        
        try {
            Table table = globalCatalogService.loadTable(tableId);
            var tableScan = table.newScan().useSnapshot(snapshotId);
            
            if (filter != null) {
                tableScan = tableScan.filter(filter);
            }
            
            // Use Iceberg's built-in record counting if available
            // For now, we'll return a mock count
            long count = 100; // Mock record count
            
            logger.debug("Counted {} records in snapshot {}", count, snapshotId);
            return count;
            
        } catch (Exception e) {
            logger.error("Failed to count records in snapshot: {}", snapshotId, e);
            throw new RuntimeException("Failed to count snapshot records: " + e.getMessage(), e);
        }
    }
    
    /**
     * Gets statistics about data in a specific snapshot.
     */
    public SnapshotDataStatistics getSnapshotStatistics(TableIdentifier tableId, long snapshotId) {
        logger.debug("Getting statistics for snapshot: {}", snapshotId);
        
        try {
            Table table = globalCatalogService.loadTable(tableId);
            Snapshot snapshot = table.snapshot(snapshotId);
            
            if (snapshot == null) {
                throw new RuntimeException("Snapshot not found: " + snapshotId);
            }
            
            // Get basic statistics from snapshot summary
            Map<String, String> summary = snapshot.summary();
            
            long totalRecords = 0;
            long totalSize = 0;
            int dataFileCount = 0;
            
            // Parse statistics from summary if available
            if (summary.containsKey("total-records")) {
                totalRecords = Long.parseLong(summary.get("total-records"));
            }
            
            if (summary.containsKey("total-data-files")) {
                dataFileCount = Integer.parseInt(summary.get("total-data-files"));
            }
            
            if (summary.containsKey("total-file-size-bytes")) {
                totalSize = Long.parseLong(summary.get("total-file-size-bytes"));
            }
            
            return new SnapshotDataStatistics(
                snapshotId,
                Instant.ofEpochMilli(snapshot.timestampMillis()),
                totalRecords,
                totalSize,
                dataFileCount,
                snapshot.operation(),
                summary
            );
            
        } catch (Exception e) {
            logger.error("Failed to get snapshot statistics: {}", snapshotId, e);
            throw new RuntimeException("Failed to get snapshot statistics: " + e.getMessage(), e);
        }
    }
    
    /**
     * Creates mock records for testing purposes.
     * In a full implementation, this would be replaced with actual Iceberg data reading.
     */
    private List<Record> createMockRecords(Schema schema, int count) {
        List<Record> records = new ArrayList<>();
        
        // For now, return empty list as we don't have a proper Record implementation
        // In a full implementation, this would create actual Record objects based on the schema
        logger.debug("Created {} mock records for schema with {} columns", count, schema.columns().size());
        
        return records;
    }
    
    /**
     * Compares data between two snapshots of the same table.
     */
    public SnapshotComparisonResult compareSnapshots(TableIdentifier tableId, 
                                                   long fromSnapshotId, long toSnapshotId) {
        logger.info("Comparing snapshots {} and {} for table {}", fromSnapshotId, toSnapshotId, tableId);
        
        try {
            Table table = globalCatalogService.loadTable(tableId);
            
            Snapshot fromSnapshot = table.snapshot(fromSnapshotId);
            Snapshot toSnapshot = table.snapshot(toSnapshotId);
            
            if (fromSnapshot == null || toSnapshot == null) {
                throw new RuntimeException("One or both snapshots not found");
            }
            
            // Get statistics for both snapshots
            SnapshotDataStatistics fromStats = getSnapshotStatistics(tableId, fromSnapshotId);
            SnapshotDataStatistics toStats = getSnapshotStatistics(tableId, toSnapshotId);
            
            // Calculate differences
            long recordDiff = toStats.getTotalRecords() - fromStats.getTotalRecords();
            long sizeDiff = toStats.getTotalSizeBytes() - fromStats.getTotalSizeBytes();
            int fileDiff = toStats.getDataFileCount() - fromStats.getDataFileCount();
            
            return new SnapshotComparisonResult(
                tableId,
                fromSnapshot,
                toSnapshot,
                fromStats,
                toStats,
                recordDiff,
                sizeDiff,
                fileDiff
            );
            
        } catch (Exception e) {
            logger.error("Failed to compare snapshots {} and {}", fromSnapshotId, toSnapshotId, e);
            throw new RuntimeException("Snapshot comparison failed: " + e.getMessage(), e);
        }
    }
    
    /**
     * Result of reading historical data from a snapshot.
     */
    public static class HistoricalDataResult {
        private final TableIdentifier tableIdentifier;
        private final Snapshot snapshot;
        private final Schema schema;
        private final List<Record> records;
        private final Instant effectiveTimestamp;
        private final String appliedFilter;
        
        public HistoricalDataResult(TableIdentifier tableIdentifier, Snapshot snapshot, Schema schema,
                                  List<Record> records, Instant effectiveTimestamp, String appliedFilter) {
            this.tableIdentifier = tableIdentifier;
            this.snapshot = snapshot;
            this.schema = schema;
            this.records = records;
            this.effectiveTimestamp = effectiveTimestamp;
            this.appliedFilter = appliedFilter;
        }
        
        public TableIdentifier getTableIdentifier() { return tableIdentifier; }
        public Snapshot getSnapshot() { return snapshot; }
        public Schema getSchema() { return schema; }
        public List<Record> getRecords() { return records; }
        public Instant getEffectiveTimestamp() { return effectiveTimestamp; }
        public String getAppliedFilter() { return appliedFilter; }
        
        public long getSnapshotId() { return snapshot.snapshotId(); }
        public int getRecordCount() { return records.size(); }
        
        /**
         * Converts records to a simple map representation for JSON serialization.
         */
        public List<Map<String, Object>> getRecordsAsMap() {
            return records.stream()
                .map(this::recordToMap)
                .collect(Collectors.toList());
        }
        
        private Map<String, Object> recordToMap(Record record) {
            Map<String, Object> map = new HashMap<>();
            for (int i = 0; i < schema.columns().size(); i++) {
                String fieldName = schema.columns().get(i).name();
                Object value = record.get(i);
                map.put(fieldName, value);
            }
            return map;
        }
        
        @Override
        public String toString() {
            return "HistoricalDataResult{" +
                    "table=" + tableIdentifier +
                    ", snapshotId=" + snapshot.snapshotId() +
                    ", records=" + records.size() +
                    ", timestamp=" + effectiveTimestamp +
                    '}';
        }
    }
    
    /**
     * Statistics about data in a specific snapshot.
     */
    public static class SnapshotDataStatistics {
        private final long snapshotId;
        private final Instant timestamp;
        private final long totalRecords;
        private final long totalSizeBytes;
        private final int dataFileCount;
        private final String operation;
        private final Map<String, String> summary;
        
        public SnapshotDataStatistics(long snapshotId, Instant timestamp, long totalRecords,
                                    long totalSizeBytes, int dataFileCount, String operation,
                                    Map<String, String> summary) {
            this.snapshotId = snapshotId;
            this.timestamp = timestamp;
            this.totalRecords = totalRecords;
            this.totalSizeBytes = totalSizeBytes;
            this.dataFileCount = dataFileCount;
            this.operation = operation;
            this.summary = summary;
        }
        
        public long getSnapshotId() { return snapshotId; }
        public Instant getTimestamp() { return timestamp; }
        public long getTotalRecords() { return totalRecords; }
        public long getTotalSizeBytes() { return totalSizeBytes; }
        public int getDataFileCount() { return dataFileCount; }
        public String getOperation() { return operation; }
        public Map<String, String> getSummary() { return summary; }
        
        public double getTotalSizeMB() {
            return totalSizeBytes / 1024.0 / 1024.0;
        }
        
        @Override
        public String toString() {
            return "SnapshotDataStatistics{" +
                    "snapshotId=" + snapshotId +
                    ", timestamp=" + timestamp +
                    ", records=" + totalRecords +
                    ", sizeMB=" + String.format("%.2f", getTotalSizeMB()) +
                    ", files=" + dataFileCount +
                    ", operation='" + operation + '\'' +
                    '}';
        }
    }
    
    /**
     * Result of comparing two snapshots.
     */
    public static class SnapshotComparisonResult {
        private final TableIdentifier tableIdentifier;
        private final Snapshot fromSnapshot;
        private final Snapshot toSnapshot;
        private final SnapshotDataStatistics fromStats;
        private final SnapshotDataStatistics toStats;
        private final long recordDifference;
        private final long sizeDifference;
        private final int fileDifference;
        
        public SnapshotComparisonResult(TableIdentifier tableIdentifier, Snapshot fromSnapshot,
                                      Snapshot toSnapshot, SnapshotDataStatistics fromStats,
                                      SnapshotDataStatistics toStats, long recordDifference,
                                      long sizeDifference, int fileDifference) {
            this.tableIdentifier = tableIdentifier;
            this.fromSnapshot = fromSnapshot;
            this.toSnapshot = toSnapshot;
            this.fromStats = fromStats;
            this.toStats = toStats;
            this.recordDifference = recordDifference;
            this.sizeDifference = sizeDifference;
            this.fileDifference = fileDifference;
        }
        
        public TableIdentifier getTableIdentifier() { return tableIdentifier; }
        public Snapshot getFromSnapshot() { return fromSnapshot; }
        public Snapshot getToSnapshot() { return toSnapshot; }
        public SnapshotDataStatistics getFromStats() { return fromStats; }
        public SnapshotDataStatistics getToStats() { return toStats; }
        public long getRecordDifference() { return recordDifference; }
        public long getSizeDifference() { return sizeDifference; }
        public int getFileDifference() { return fileDifference; }
        
        public boolean hasChanges() {
            return recordDifference != 0 || sizeDifference != 0 || fileDifference != 0;
        }
        
        @Override
        public String toString() {
            return "SnapshotComparisonResult{" +
                    "table=" + tableIdentifier +
                    ", from=" + fromSnapshot.snapshotId() +
                    ", to=" + toSnapshot.snapshotId() +
                    ", recordDiff=" + recordDifference +
                    ", sizeDiffMB=" + String.format("%.2f", sizeDifference / 1024.0 / 1024.0) +
                    ", fileDiff=" + fileDifference +
                    '}';
        }
    }
}