package com.minicloud.controlplane.service;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.Table;
import org.apache.iceberg.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * Service for managing Iceberg table snapshots including listing, validation, and cleanup operations.
 * Implements snapshot retention policies and provides metadata retrieval for time travel queries.
 */
@Service
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class SnapshotManagementService {
    
    private static final Logger logger = LoggerFactory.getLogger(SnapshotManagementService.class);
    
    // Default retention policy: keep snapshots for 30 days
    private static final int DEFAULT_RETENTION_DAYS = 30;
    
    // Minimum number of snapshots to keep regardless of age
    private static final int MIN_SNAPSHOTS_TO_KEEP = 5;
    
    private final GlobalCatalogService globalCatalogService;
    
    @Autowired
    public SnapshotManagementService(GlobalCatalogService globalCatalogService) {
        this.globalCatalogService = globalCatalogService;
    }
    
    /**
     * Lists all snapshots for a table with metadata.
     */
    public List<SnapshotInfo> listSnapshots(TableIdentifier tableId) {
        logger.debug("Listing snapshots for table: {}", tableId);
        
        try {
            Table table = globalCatalogService.loadTable(tableId);
            List<SnapshotInfo> snapshots = new ArrayList<>();
            
            for (Snapshot snapshot : table.snapshots()) {
                snapshots.add(createSnapshotInfo(snapshot, table));
            }
            
            // Sort by timestamp (newest first)
            snapshots.sort(Comparator.comparing(SnapshotInfo::getTimestamp).reversed());
            
            logger.debug("Found {} snapshots for table: {}", snapshots.size(), tableId);
            return snapshots;
            
        } catch (Exception e) {
            logger.error("Failed to list snapshots for table: {}", tableId, e);
            throw new RuntimeException("Failed to list snapshots: " + e.getMessage(), e);
        }
    }
    
    /**
     * Gets detailed information about a specific snapshot.
     */
    public SnapshotInfo getSnapshotInfo(TableIdentifier tableId, long snapshotId) {
        logger.debug("Getting snapshot info for: {} snapshot: {}", tableId, snapshotId);
        
        try {
            Table table = globalCatalogService.loadTable(tableId);
            Snapshot snapshot = table.snapshot(snapshotId);
            
            if (snapshot == null) {
                throw new RuntimeException("Snapshot not found: " + snapshotId);
            }
            
            return createSnapshotInfo(snapshot, table);
            
        } catch (Exception e) {
            logger.error("Failed to get snapshot info: {} snapshot: {}", tableId, snapshotId, e);
            throw new RuntimeException("Failed to get snapshot info: " + e.getMessage(), e);
        }
    }
    
    /**
     * Validates that a snapshot exists and is suitable for time travel queries.
     */
    public void validateSnapshotForTimeTravel(TableIdentifier tableId, long snapshotId) {
        logger.debug("Validating snapshot {} for time travel on table: {}", snapshotId, tableId);
        
        try {
            Table table = globalCatalogService.loadTable(tableId);
            Snapshot snapshot = table.snapshot(snapshotId);
            
            if (snapshot == null) {
                throw new SnapshotValidationException("Snapshot not found: " + snapshotId);
            }
            
            // Check if snapshot has data files
            if (snapshot.allManifests(table.io()).isEmpty()) {
                throw new SnapshotValidationException("Snapshot " + snapshotId + " has no data files");
            }
            
            // Check if snapshot is too old (based on retention policy)
            Instant snapshotTime = Instant.ofEpochMilli(snapshot.timestampMillis());
            Instant cutoffTime = Instant.now().minus(DEFAULT_RETENTION_DAYS, ChronoUnit.DAYS);
            
            if (snapshotTime.isBefore(cutoffTime)) {
                logger.warn("Snapshot {} is older than retention policy ({} days)", snapshotId, DEFAULT_RETENTION_DAYS);
                // Don't fail validation, just warn - old snapshots might still be accessible
            }
            
            logger.debug("Snapshot {} validated successfully for time travel", snapshotId);
            
        } catch (SnapshotValidationException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to validate snapshot {} for table: {}", snapshotId, tableId, e);
            throw new SnapshotValidationException("Snapshot validation failed: " + e.getMessage(), e);
        }
    }
    
    /**
     * Finds snapshots within a time range.
     */
    public List<SnapshotInfo> findSnapshotsInTimeRange(TableIdentifier tableId, 
                                                       Instant startTime, Instant endTime) {
        logger.debug("Finding snapshots for table {} between {} and {}", tableId, startTime, endTime);
        
        List<SnapshotInfo> allSnapshots = listSnapshots(tableId);
        
        return allSnapshots.stream()
            .filter(snapshot -> {
                Instant snapshotTime = snapshot.getTimestamp();
                return !snapshotTime.isBefore(startTime) && !snapshotTime.isAfter(endTime);
            })
            .collect(Collectors.toList());
    }
    
    /**
     * Finds the snapshot closest to a specific timestamp.
     */
    public SnapshotInfo findSnapshotNearTimestamp(TableIdentifier tableId, Instant targetTime) {
        logger.debug("Finding snapshot closest to {} for table: {}", targetTime, tableId);
        
        List<SnapshotInfo> snapshots = listSnapshots(tableId);
        
        if (snapshots.isEmpty()) {
            throw new RuntimeException("No snapshots found for table: " + tableId);
        }
        
        // Find the snapshot with timestamp closest to but not after the target time
        SnapshotInfo closestSnapshot = null;
        long minTimeDiff = Long.MAX_VALUE;
        
        for (SnapshotInfo snapshot : snapshots) {
            long timeDiff = targetTime.toEpochMilli() - snapshot.getTimestamp().toEpochMilli();
            
            // Only consider snapshots that are at or before the target time
            if (timeDiff >= 0 && timeDiff < minTimeDiff) {
                minTimeDiff = timeDiff;
                closestSnapshot = snapshot;
            }
        }
        
        if (closestSnapshot == null) {
            throw new RuntimeException("No snapshot found before timestamp: " + targetTime);
        }
        
        logger.debug("Found closest snapshot: {} at {} for target time: {}", 
                    closestSnapshot.getSnapshotId(), closestSnapshot.getTimestamp(), targetTime);
        
        return closestSnapshot;
    }
    
    /**
     * Gets snapshots that are eligible for cleanup based on retention policy.
     */
    public List<SnapshotInfo> getSnapshotsEligibleForCleanup(TableIdentifier tableId) {
        return getSnapshotsEligibleForCleanup(tableId, DEFAULT_RETENTION_DAYS);
    }
    
    /**
     * Gets snapshots that are eligible for cleanup based on custom retention policy.
     */
    public List<SnapshotInfo> getSnapshotsEligibleForCleanup(TableIdentifier tableId, int retentionDays) {
        logger.debug("Finding snapshots eligible for cleanup for table: {} (retention: {} days)", 
                    tableId, retentionDays);
        
        List<SnapshotInfo> allSnapshots = listSnapshots(tableId);
        
        if (allSnapshots.size() <= MIN_SNAPSHOTS_TO_KEEP) {
            logger.debug("Table has {} snapshots, keeping all (minimum: {})", 
                        allSnapshots.size(), MIN_SNAPSHOTS_TO_KEEP);
            return new ArrayList<>();
        }
        
        Instant cutoffTime = Instant.now().minus(retentionDays, ChronoUnit.DAYS);
        
        // Sort by timestamp (oldest first) and skip the minimum number to keep
        List<SnapshotInfo> eligibleForCleanup = allSnapshots.stream()
            .sorted(Comparator.comparing(SnapshotInfo::getTimestamp))
            .filter(snapshot -> snapshot.getTimestamp().isBefore(cutoffTime))
            .collect(Collectors.toList());
        
        // Always keep at least MIN_SNAPSHOTS_TO_KEEP snapshots
        int maxToRemove = Math.max(0, allSnapshots.size() - MIN_SNAPSHOTS_TO_KEEP);
        if (eligibleForCleanup.size() > maxToRemove) {
            eligibleForCleanup = eligibleForCleanup.subList(0, maxToRemove);
        }
        
        logger.debug("Found {} snapshots eligible for cleanup", eligibleForCleanup.size());
        return eligibleForCleanup;
    }
    
    /**
     * Performs cleanup of old snapshots based on retention policy.
     * This is a dry-run method that returns what would be cleaned up.
     */
    public SnapshotCleanupResult planSnapshotCleanup(TableIdentifier tableId) {
        return planSnapshotCleanup(tableId, DEFAULT_RETENTION_DAYS);
    }
    
    /**
     * Plans snapshot cleanup with custom retention policy.
     */
    public SnapshotCleanupResult planSnapshotCleanup(TableIdentifier tableId, int retentionDays) {
        logger.info("Planning snapshot cleanup for table: {} (retention: {} days)", tableId, retentionDays);
        
        List<SnapshotInfo> allSnapshots = listSnapshots(tableId);
        List<SnapshotInfo> eligibleForCleanup = getSnapshotsEligibleForCleanup(tableId, retentionDays);
        
        long totalSizeToFree = eligibleForCleanup.stream()
            .mapToLong(SnapshotInfo::getTotalSizeBytes)
            .sum();
        
        return new SnapshotCleanupResult(
            tableId,
            allSnapshots.size(),
            eligibleForCleanup.size(),
            eligibleForCleanup,
            totalSizeToFree,
            retentionDays,
            false // This is a plan, not executed
        );
    }
    
    /**
     * Gets statistics about snapshots for a table.
     */
    public SnapshotStatistics getSnapshotStatistics(TableIdentifier tableId) {
        logger.debug("Getting snapshot statistics for table: {}", tableId);
        
        List<SnapshotInfo> snapshots = listSnapshots(tableId);
        
        if (snapshots.isEmpty()) {
            return new SnapshotStatistics(tableId, 0, 0, 0, null, null, 0);
        }
        
        long totalSize = snapshots.stream().mapToLong(SnapshotInfo::getTotalSizeBytes).sum();
        long totalRecords = snapshots.stream().mapToLong(SnapshotInfo::getTotalRecords).sum();
        
        SnapshotInfo oldest = snapshots.stream()
            .min(Comparator.comparing(SnapshotInfo::getTimestamp))
            .orElse(null);
        
        SnapshotInfo newest = snapshots.stream()
            .max(Comparator.comparing(SnapshotInfo::getTimestamp))
            .orElse(null);
        
        return new SnapshotStatistics(
            tableId,
            snapshots.size(),
            totalSize,
            totalRecords,
            oldest != null ? oldest.getTimestamp() : null,
            newest != null ? newest.getTimestamp() : null,
            getSnapshotsEligibleForCleanup(tableId).size()
        );
    }
    
    /**
     * Creates a SnapshotInfo object from an Iceberg Snapshot.
     */
    private SnapshotInfo createSnapshotInfo(Snapshot snapshot, Table table) {
        Map<String, String> summary = snapshot.summary();
        
        long totalRecords = 0;
        long totalSize = 0;
        int dataFileCount = 0;
        
        // Parse statistics from summary
        if (summary.containsKey("total-records")) {
            try {
                totalRecords = Long.parseLong(summary.get("total-records"));
            } catch (NumberFormatException e) {
                logger.debug("Could not parse total-records from snapshot summary");
            }
        }
        
        if (summary.containsKey("total-data-files")) {
            try {
                dataFileCount = Integer.parseInt(summary.get("total-data-files"));
            } catch (NumberFormatException e) {
                logger.debug("Could not parse total-data-files from snapshot summary");
            }
        }
        
        if (summary.containsKey("total-file-size-bytes")) {
            try {
                totalSize = Long.parseLong(summary.get("total-file-size-bytes"));
            } catch (NumberFormatException e) {
                logger.debug("Could not parse total-file-size-bytes from snapshot summary");
            }
        }
        
        return new SnapshotInfo(
            snapshot.snapshotId(),
            Instant.ofEpochMilli(snapshot.timestampMillis()),
            snapshot.operation(),
            summary,
            totalRecords,
            totalSize,
            dataFileCount,
            snapshot.parentId()
        );
    }
    
    /**
     * Information about a table snapshot.
     */
    public static class SnapshotInfo {
        private final long snapshotId;
        private final Instant timestamp;
        private final String operation;
        private final Map<String, String> summary;
        private final long totalRecords;
        private final long totalSizeBytes;
        private final int dataFileCount;
        private final Long parentSnapshotId;
        
        public SnapshotInfo(long snapshotId, Instant timestamp, String operation,
                          Map<String, String> summary, long totalRecords, long totalSizeBytes,
                          int dataFileCount, Long parentSnapshotId) {
            this.snapshotId = snapshotId;
            this.timestamp = timestamp;
            this.operation = operation;
            this.summary = summary;
            this.totalRecords = totalRecords;
            this.totalSizeBytes = totalSizeBytes;
            this.dataFileCount = dataFileCount;
            this.parentSnapshotId = parentSnapshotId;
        }
        
        public long getSnapshotId() { return snapshotId; }
        public Instant getTimestamp() { return timestamp; }
        public String getOperation() { return operation; }
        public Map<String, String> getSummary() { return summary; }
        public long getTotalRecords() { return totalRecords; }
        public long getTotalSizeBytes() { return totalSizeBytes; }
        public int getDataFileCount() { return dataFileCount; }
        public Long getParentSnapshotId() { return parentSnapshotId; }
        
        public double getTotalSizeMB() {
            return totalSizeBytes / 1024.0 / 1024.0;
        }
        
        public boolean isInitialSnapshot() {
            return parentSnapshotId == null;
        }
        
        @Override
        public String toString() {
            return "SnapshotInfo{" +
                    "snapshotId=" + snapshotId +
                    ", timestamp=" + timestamp +
                    ", operation='" + operation + '\'' +
                    ", records=" + totalRecords +
                    ", sizeMB=" + String.format("%.2f", getTotalSizeMB()) +
                    ", files=" + dataFileCount +
                    '}';
        }
    }
    
    /**
     * Result of snapshot cleanup planning or execution.
     */
    public static class SnapshotCleanupResult {
        private final TableIdentifier tableId;
        private final int totalSnapshots;
        private final int snapshotsToCleanup;
        private final List<SnapshotInfo> cleanupCandidates;
        private final long totalSizeToFree;
        private final int retentionDays;
        private final boolean executed;
        
        public SnapshotCleanupResult(TableIdentifier tableId, int totalSnapshots, int snapshotsToCleanup,
                                   List<SnapshotInfo> cleanupCandidates, long totalSizeToFree,
                                   int retentionDays, boolean executed) {
            this.tableId = tableId;
            this.totalSnapshots = totalSnapshots;
            this.snapshotsToCleanup = snapshotsToCleanup;
            this.cleanupCandidates = cleanupCandidates;
            this.totalSizeToFree = totalSizeToFree;
            this.retentionDays = retentionDays;
            this.executed = executed;
        }
        
        public TableIdentifier getTableId() { return tableId; }
        public int getTotalSnapshots() { return totalSnapshots; }
        public int getSnapshotsToCleanup() { return snapshotsToCleanup; }
        public List<SnapshotInfo> getCleanupCandidates() { return cleanupCandidates; }
        public long getTotalSizeToFree() { return totalSizeToFree; }
        public int getRetentionDays() { return retentionDays; }
        public boolean isExecuted() { return executed; }
        
        public double getTotalSizeToFreeMB() {
            return totalSizeToFree / 1024.0 / 1024.0;
        }
        
        public int getSnapshotsToKeep() {
            return totalSnapshots - snapshotsToCleanup;
        }
        
        @Override
        public String toString() {
            return "SnapshotCleanupResult{" +
                    "table=" + tableId +
                    ", total=" + totalSnapshots +
                    ", toCleanup=" + snapshotsToCleanup +
                    ", toKeep=" + getSnapshotsToKeep() +
                    ", sizeToFreeMB=" + String.format("%.2f", getTotalSizeToFreeMB()) +
                    ", retention=" + retentionDays + "days" +
                    ", executed=" + executed +
                    '}';
        }
    }
    
    /**
     * Statistics about snapshots for a table.
     */
    public static class SnapshotStatistics {
        private final TableIdentifier tableId;
        private final int totalSnapshots;
        private final long totalSizeBytes;
        private final long totalRecords;
        private final Instant oldestSnapshot;
        private final Instant newestSnapshot;
        private final int snapshotsEligibleForCleanup;
        
        public SnapshotStatistics(TableIdentifier tableId, int totalSnapshots, long totalSizeBytes,
                                long totalRecords, Instant oldestSnapshot, Instant newestSnapshot,
                                int snapshotsEligibleForCleanup) {
            this.tableId = tableId;
            this.totalSnapshots = totalSnapshots;
            this.totalSizeBytes = totalSizeBytes;
            this.totalRecords = totalRecords;
            this.oldestSnapshot = oldestSnapshot;
            this.newestSnapshot = newestSnapshot;
            this.snapshotsEligibleForCleanup = snapshotsEligibleForCleanup;
        }
        
        public TableIdentifier getTableId() { return tableId; }
        public int getTotalSnapshots() { return totalSnapshots; }
        public long getTotalSizeBytes() { return totalSizeBytes; }
        public long getTotalRecords() { return totalRecords; }
        public Instant getOldestSnapshot() { return oldestSnapshot; }
        public Instant getNewestSnapshot() { return newestSnapshot; }
        public int getSnapshotsEligibleForCleanup() { return snapshotsEligibleForCleanup; }
        
        public double getTotalSizeMB() {
            return totalSizeBytes / 1024.0 / 1024.0;
        }
        
        @Override
        public String toString() {
            return "SnapshotStatistics{" +
                    "table=" + tableId +
                    ", snapshots=" + totalSnapshots +
                    ", sizeMB=" + String.format("%.2f", getTotalSizeMB()) +
                    ", records=" + totalRecords +
                    ", eligibleForCleanup=" + snapshotsEligibleForCleanup +
                    '}';
        }
    }
    
    /**
     * Exception thrown when snapshot validation fails.
     */
    public static class SnapshotValidationException extends RuntimeException {
        public SnapshotValidationException(String message) {
            super(message);
        }
        
        public SnapshotValidationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}