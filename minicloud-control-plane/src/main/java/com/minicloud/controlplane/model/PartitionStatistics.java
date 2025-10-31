package com.minicloud.controlplane.model;

import java.time.Instant;
import java.util.Map;

/**
 * Represents statistics for a specific partition in an Iceberg table.
 * Used for partition pruning and query optimization.
 */
public class PartitionStatistics {
    
    private String tableName;
    private String namespace;
    private String partitionPath;
    private Map<String, String> partitionValues;
    private long rowCount;
    private long fileCount;
    private long totalSize;
    private Instant lastModified;
    private Map<String, Object> minValues;
    private Map<String, Object> maxValues;
    private Map<String, Long> nullCounts;
    private Map<String, Object> properties;
    
    public PartitionStatistics() {}
    
    public PartitionStatistics(String tableName, String namespace, String partitionPath) {
        this.tableName = tableName;
        this.namespace = namespace;
        this.partitionPath = partitionPath;
        this.lastModified = Instant.now();
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public String getNamespace() {
        return namespace;
    }
    
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
    
    public String getPartitionPath() {
        return partitionPath;
    }
    
    public void setPartitionPath(String partitionPath) {
        this.partitionPath = partitionPath;
    }
    
    public Map<String, String> getPartitionValues() {
        return partitionValues;
    }
    
    public void setPartitionValues(Map<String, String> partitionValues) {
        this.partitionValues = partitionValues;
    }
    
    public long getRowCount() {
        return rowCount;
    }
    
    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }
    
    public long getFileCount() {
        return fileCount;
    }
    
    public void setFileCount(long fileCount) {
        this.fileCount = fileCount;
    }
    
    public long getTotalSize() {
        return totalSize;
    }
    
    public void setTotalSize(long totalSize) {
        this.totalSize = totalSize;
    }
    
    public Instant getLastModified() {
        return lastModified;
    }
    
    public void setLastModified(Instant lastModified) {
        this.lastModified = lastModified;
    }
    
    public Map<String, Object> getMinValues() {
        return minValues;
    }
    
    public void setMinValues(Map<String, Object> minValues) {
        this.minValues = minValues;
    }
    
    public Map<String, Object> getMaxValues() {
        return maxValues;
    }
    
    public void setMaxValues(Map<String, Object> maxValues) {
        this.maxValues = maxValues;
    }
    
    public Map<String, Long> getNullCounts() {
        return nullCounts;
    }
    
    public void setNullCounts(Map<String, Long> nullCounts) {
        this.nullCounts = nullCounts;
    }
    
    public Map<String, Object> getProperties() {
        return properties;
    }
    
    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}