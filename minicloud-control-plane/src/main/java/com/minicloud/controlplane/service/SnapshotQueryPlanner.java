package com.minicloud.controlplane.service;

import com.minicloud.controlplane.service.TimeTravelQueryProcessor.TimeTravelQueryResult;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.Table;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.ManifestFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

/**
 * Query planner for executing queries against specific Iceberg snapshots.
 * Handles snapshot-based query planning and historical data access.
 */
@Component
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class SnapshotQueryPlanner {
    
    private static final Logger logger = LoggerFactory.getLogger(SnapshotQueryPlanner.class);
    
    private final TimeTravelQueryResult timeTravelResult;
    private final GlobalCatalogService globalCatalogService;
    private final SnapshotManagementService snapshotManagementService;
    
    public SnapshotQueryPlanner(TimeTravelQueryResult timeTravelResult,
                               GlobalCatalogService globalCatalogService,
                               SnapshotManagementService snapshotManagementService) {
        this.timeTravelResult = timeTravelResult;
        this.globalCatalogService = globalCatalogService;
        this.snapshotManagementService = snapshotManagementService;
    }
    
    /**
     * Creates an execution plan for the time travel query.
     */
    public SnapshotQueryPlan createExecutionPlan() {
        logger.info("Creating execution plan for time travel query: {}", timeTravelResult);
        
        try {
            // Load the table at the specific snapshot
            Table table = globalCatalogService.loadTable(timeTravelResult.getTableIdentifier());
            Snapshot snapshot = timeTravelResult.getTargetSnapshot();
            
            // Get the data files that were part of this snapshot
            List<DataFile> dataFiles = getDataFilesForSnapshot(table, snapshot);
            
            // Create the query plan
            SnapshotQueryPlan plan = new SnapshotQueryPlan(
                timeTravelResult.getOriginalSql(),
                timeTravelResult.getModifiedSql(),
                timeTravelResult.getTableIdentifier(),
                snapshot,
                dataFiles,
                timeTravelResult.getEffectiveTimestamp()
            );
            
            logger.info("Created execution plan for snapshot {} with {} data files", 
                       snapshot.snapshotId(), dataFiles.size());
            
            return plan;
            
        } catch (Exception e) {
            logger.error("Failed to create execution plan for time travel query", e);
            throw new RuntimeException("Failed to create execution plan: " + e.getMessage(), e);
        }
    }
    
    /**
     * Gets all data files that were part of the specified snapshot.
     */
    private List<DataFile> getDataFilesForSnapshot(Table table, Snapshot snapshot) {
        logger.debug("Getting data files for snapshot: {}", snapshot.snapshotId());
        
        List<DataFile> dataFiles = new ArrayList<>();
        
        try {
            // Get all manifest files for this snapshot
            List<ManifestFile> manifestFiles = snapshot.allManifests(table.io());
            
            // Read data files from each manifest
            for (ManifestFile manifestFile : manifestFiles) {
                try (CloseableIterable<DataFile> manifestDataFiles = 
                         ManifestFiles.read(manifestFile, table.io())) {
                    
                    for (DataFile dataFile : manifestDataFiles) {
                        // Only include files that were added (not deleted) in this snapshot
                        if (dataFile != null) {
                            dataFiles.add(dataFile);
                        }
                    }
                }
            }
            
            logger.debug("Found {} data files for snapshot {}", dataFiles.size(), snapshot.snapshotId());
            return dataFiles;
            
        } catch (IOException e) {
            logger.error("Failed to read data files for snapshot: {}", snapshot.snapshotId(), e);
            throw new RuntimeException("Failed to read data files for snapshot: " + snapshot.snapshotId(), e);
        }
    }
    
    /**
     * Validates that the snapshot is suitable for query execution.
     */
    public void validateSnapshotForExecution() {
        Snapshot snapshot = timeTravelResult.getTargetSnapshot();
        TableIdentifier tableId = timeTravelResult.getTableIdentifier();
        
        logger.debug("Validating snapshot {} for execution", snapshot.snapshotId());
        
        // Use snapshot management service for validation
        snapshotManagementService.validateSnapshotForTimeTravel(tableId, snapshot.snapshotId());
        
        // Additional validation for query execution
        if (snapshot.allManifests(globalCatalogService.loadTable(tableId).io()).isEmpty()) {
            throw new RuntimeException("Snapshot " + snapshot.snapshotId() + " has no manifest files");
        }
        
        logger.debug("Snapshot {} validated successfully for execution", snapshot.snapshotId());
    }
    
    /**
     * Gets metadata about the snapshot being queried.
     */
    public SnapshotMetadata getSnapshotMetadata() {
        Snapshot snapshot = timeTravelResult.getTargetSnapshot();
        Table table = globalCatalogService.loadTable(timeTravelResult.getTableIdentifier());
        
        // Count data files and calculate total size
        List<DataFile> dataFiles = getDataFilesForSnapshot(table, snapshot);
        long totalSizeBytes = dataFiles.stream().mapToLong(DataFile::fileSizeInBytes).sum();
        long totalRecords = dataFiles.stream().mapToLong(DataFile::recordCount).sum();
        
        return new SnapshotMetadata(
            snapshot.snapshotId(),
            Instant.ofEpochMilli(snapshot.timestampMillis()),
            snapshot.operation(),
            snapshot.summary(),
            dataFiles.size(),
            totalSizeBytes,
            totalRecords
        );
    }
    
    /**
     * Execution plan for a snapshot-based query.
     */
    public static class SnapshotQueryPlan {
        private final String originalSql;
        private final String modifiedSql;
        private final TableIdentifier tableIdentifier;
        private final Snapshot snapshot;
        private final List<DataFile> dataFiles;
        private final Instant effectiveTimestamp;
        
        public SnapshotQueryPlan(String originalSql, String modifiedSql, TableIdentifier tableIdentifier,
                               Snapshot snapshot, List<DataFile> dataFiles, Instant effectiveTimestamp) {
            this.originalSql = originalSql;
            this.modifiedSql = modifiedSql;
            this.tableIdentifier = tableIdentifier;
            this.snapshot = snapshot;
            this.dataFiles = dataFiles;
            this.effectiveTimestamp = effectiveTimestamp;
        }
        
        public String getOriginalSql() { return originalSql; }
        public String getModifiedSql() { return modifiedSql; }
        public TableIdentifier getTableIdentifier() { return tableIdentifier; }
        public Snapshot getSnapshot() { return snapshot; }
        public List<DataFile> getDataFiles() { return dataFiles; }
        public Instant getEffectiveTimestamp() { return effectiveTimestamp; }
        
        public long getSnapshotId() { return snapshot.snapshotId(); }
        public int getDataFileCount() { return dataFiles.size(); }
        
        public long getTotalSizeBytes() {
            return dataFiles.stream().mapToLong(DataFile::fileSizeInBytes).sum();
        }
        
        public long getTotalRecords() {
            return dataFiles.stream().mapToLong(DataFile::recordCount).sum();
        }
        
        @Override
        public String toString() {
            return "SnapshotQueryPlan{" +
                    "table=" + tableIdentifier +
                    ", snapshotId=" + snapshot.snapshotId() +
                    ", dataFiles=" + dataFiles.size() +
                    ", totalSizeMB=" + (getTotalSizeBytes() / 1024 / 1024) +
                    ", totalRecords=" + getTotalRecords() +
                    '}';
        }
    }
    
    /**
     * Metadata about a snapshot used for query execution.
     */
    public static class SnapshotMetadata {
        private final long snapshotId;
        private final Instant timestamp;
        private final String operation;
        private final java.util.Map<String, String> summary;
        private final int dataFileCount;
        private final long totalSizeBytes;
        private final long totalRecords;
        
        public SnapshotMetadata(long snapshotId, Instant timestamp, String operation,
                              java.util.Map<String, String> summary, int dataFileCount,
                              long totalSizeBytes, long totalRecords) {
            this.snapshotId = snapshotId;
            this.timestamp = timestamp;
            this.operation = operation;
            this.summary = summary;
            this.dataFileCount = dataFileCount;
            this.totalSizeBytes = totalSizeBytes;
            this.totalRecords = totalRecords;
        }
        
        public long getSnapshotId() { return snapshotId; }
        public Instant getTimestamp() { return timestamp; }
        public String getOperation() { return operation; }
        public java.util.Map<String, String> getSummary() { return summary; }
        public int getDataFileCount() { return dataFileCount; }
        public long getTotalSizeBytes() { return totalSizeBytes; }
        public long getTotalRecords() { return totalRecords; }
        
        public double getTotalSizeMB() {
            return totalSizeBytes / 1024.0 / 1024.0;
        }
        
        @Override
        public String toString() {
            return "SnapshotMetadata{" +
                    "snapshotId=" + snapshotId +
                    ", timestamp=" + timestamp +
                    ", operation='" + operation + '\'' +
                    ", dataFiles=" + dataFileCount +
                    ", sizeMB=" + String.format("%.2f", getTotalSizeMB()) +
                    ", records=" + totalRecords +
                    '}';
        }
    }
}