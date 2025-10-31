package com.minicloud.controlplane.model;

import java.time.Instant;
import java.util.Map;

/**
 * Represents comprehensive statistics for an Iceberg table.
 * Used for query optimization and performance monitoring.
 */
public class TableStatistics {
    
    private String tableName;
    private String namespace;
    private long totalRows;
    private long totalSize;
    private long totalFiles;
    private long totalPartitions;
    private Instant lastUpdated;
    private Map<String, ColumnStatistics> columnStatistics;
    private Map<String, Object> properties;
    
    public TableStatistics() {}
    
    public TableStatistics(String tableName, String namespace) {
        this.tableName = tableName;
        this.namespace = namespace;
        this.lastUpdated = Instant.now();
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
    
    public long getTotalRows() {
        return totalRows;
    }
    
    public void setTotalRows(long totalRows) {
        this.totalRows = totalRows;
    }
    
    public long getTotalSize() {
        return totalSize;
    }
    
    public void setTotalSize(long totalSize) {
        this.totalSize = totalSize;
    }
    
    public long getTotalFiles() {
        return totalFiles;
    }
    
    public void setTotalFiles(long totalFiles) {
        this.totalFiles = totalFiles;
    }
    
    public long getTotalPartitions() {
        return totalPartitions;
    }
    
    public void setTotalPartitions(long totalPartitions) {
        this.totalPartitions = totalPartitions;
    }
    
    public Instant getLastUpdated() {
        return lastUpdated;
    }
    
    public void setLastUpdated(Instant lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
    
    public Map<String, ColumnStatistics> getColumnStatistics() {
        return columnStatistics;
    }
    
    public void setColumnStatistics(Map<String, ColumnStatistics> columnStatistics) {
        this.columnStatistics = columnStatistics;
    }
    
    public Map<String, Object> getProperties() {
        return properties;
    }
    
    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
    
    /**
     * Represents statistics for a single column.
     */
    public static class ColumnStatistics {
        private String columnName;
        private String dataType;
        private long nullCount;
        private long distinctCount;
        private Object minValue;
        private Object maxValue;
        private double avgLength;
        private long totalLength;
        
        public ColumnStatistics() {}
        
        public ColumnStatistics(String columnName, String dataType) {
            this.columnName = columnName;
            this.dataType = dataType;
        }
        
        public String getColumnName() {
            return columnName;
        }
        
        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }
        
        public String getDataType() {
            return dataType;
        }
        
        public void setDataType(String dataType) {
            this.dataType = dataType;
        }
        
        public long getNullCount() {
            return nullCount;
        }
        
        public void setNullCount(long nullCount) {
            this.nullCount = nullCount;
        }
        
        public long getDistinctCount() {
            return distinctCount;
        }
        
        public void setDistinctCount(long distinctCount) {
            this.distinctCount = distinctCount;
        }
        
        public Object getMinValue() {
            return minValue;
        }
        
        public void setMinValue(Object minValue) {
            this.minValue = minValue;
        }
        
        public Object getMaxValue() {
            return maxValue;
        }
        
        public void setMaxValue(Object maxValue) {
            this.maxValue = maxValue;
        }
        
        public double getAvgLength() {
            return avgLength;
        }
        
        public void setAvgLength(double avgLength) {
            this.avgLength = avgLength;
        }
        
        public long getTotalLength() {
            return totalLength;
        }
        
        public void setTotalLength(long totalLength) {
            this.totalLength = totalLength;
        }
    }
}