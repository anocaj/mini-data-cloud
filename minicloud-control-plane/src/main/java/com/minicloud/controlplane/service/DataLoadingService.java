package com.minicloud.controlplane.service;

import com.minicloud.controlplane.config.IcebergConfiguration;
import com.minicloud.controlplane.controller.DataLoadingController;
import com.minicloud.controlplane.dto.TableInfo;
import com.minicloud.controlplane.model.TableMetadata;
import com.minicloud.controlplane.repository.TableMetadataRepository;
import com.minicloud.controlplane.sql.TableRegistrationService;
import com.minicloud.controlplane.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Service for loading data from various sources into the Mini Data Cloud
 */
@Service
@Transactional
public class DataLoadingService {
    
    private static final Logger logger = LoggerFactory.getLogger(DataLoadingService.class);
    
    @Autowired
    private StorageService storageService;
    
    @Autowired
    private MetadataService metadataService;
    
    @Autowired
    private TableMetadataRepository tableMetadataRepository;
    
    @Autowired
    private TableRegistrationService tableRegistrationService;
    
    @Autowired(required = false)
    private IcebergTableCreationService icebergTableCreationService;
    
    @Autowired(required = false)
    private IcebergConfiguration icebergConfiguration;
    
    @Value("${minicloud.storage.data-directory:./data}")
    private String dataDirectory;
    
    /**
     * Load CSV data into a table
     */
    public LoadResult loadCsvData(String csvFilePath, String namespaceName, String tableName, boolean hasHeader) {
        logger.info("Loading CSV data from {} into table {}.{}", csvFilePath, namespaceName, tableName);
        
        // Validate CSV file exists
        File csvFile = new File(csvFilePath);
        if (!csvFile.exists()) {
            throw new IllegalArgumentException("CSV file not found: " + csvFilePath);
        }
        
        // Check if Iceberg integration is enabled
        if (isIcebergEnabled()) {
            return loadCsvDataAsIcebergTable(csvFilePath, namespaceName, tableName, hasHeader);
        } else {
            return loadCsvDataAsParquetTable(csvFilePath, namespaceName, tableName, hasHeader);
        }
    }
    
    /**
     * Load CSV data as Iceberg table (new implementation)
     */
    private LoadResult loadCsvDataAsIcebergTable(String csvFilePath, String namespaceName, 
                                               String tableName, boolean hasHeader) {
        logger.info("Loading CSV data as Iceberg table: {}.{}", namespaceName, tableName);
        
        try {
            // Create Iceberg table from CSV
            IcebergTableCreationService.TableCreationResult result = 
                icebergTableCreationService.createTableFromCsv(csvFilePath, namespaceName, tableName, hasHeader);
            
            // Register table in metadata service for backward compatibility
            String schemaDefinition = convertIcebergSchemaToString(result.getSchema());
            TableInfo tableInfo = metadataService.registerTable(
                namespaceName, tableName, result.getTableLocation(), schemaDefinition);
            
            // Update table statistics
            metadataService.updateTableStatistics(namespaceName, tableName, 
                                                result.getRowCount(), 0L); // File size not available for Iceberg
            
            // Register table with Calcite schema for SQL queries
            tableRegistrationService.registerTable(tableName, tableInfo);
            
            logger.info("Successfully loaded {} rows into Iceberg table {}.{}", 
                       result.getRowCount(), namespaceName, tableName);
            
            return new LoadResult(
                tableName,
                result.getRowCount(),
                0L, // File size not directly available for Iceberg
                result.getDurationMs(),
                result.getTableLocation(),
                null // Arrow schema not available for Iceberg
            );
            
        } catch (Exception e) {
            logger.error("Failed to load CSV as Iceberg table", e);
            throw new RuntimeException("Failed to load CSV as Iceberg table: " + e.getMessage(), e);
        }
    }
    
    /**
     * Load CSV data as traditional Parquet table (existing implementation)
     */
    private LoadResult loadCsvDataAsParquetTable(String csvFilePath, String namespaceName, 
                                               String tableName, boolean hasHeader) {
        logger.info("Loading CSV data as Parquet table: {}.{}", namespaceName, tableName);
        
        // Create table location
        String tableLocation = Paths.get(dataDirectory, namespaceName, tableName).toString();
        
        // Load data using storage service
        StorageService.LoadResult storageResult = storageService.loadCsvData(
            csvFilePath, tableName, tableLocation, hasHeader);
        
        // Register table in metadata service
        String schemaDefinition = convertArrowSchemaToString(storageResult.getSchema());
        TableInfo tableInfo = metadataService.registerTable(namespaceName, tableName, tableLocation, schemaDefinition);
        
        // Update table statistics
        metadataService.updateTableStatistics(namespaceName, tableName, 
                                            storageResult.getRowCount(), 
                                            storageResult.getFileSizeBytes());
        
        // Register table with Calcite schema for SQL queries
        tableRegistrationService.registerTable(tableName, tableInfo);
        
        logger.info("Successfully loaded {} rows into Parquet table {}.{}", 
                   storageResult.getRowCount(), namespaceName, tableName);
        
        return new LoadResult(
            storageResult.getTableName(),
            storageResult.getRowCount(),
            storageResult.getFileSizeBytes(),
            storageResult.getDurationMs(),
            tableLocation,
            storageResult.getSchema()
        );
    }
    
    /**
     * Load the sample bank transactions data
     */
    public LoadResult loadSampleBankTransactions() {
        logger.info("Loading sample bank transactions data");
        
        // Try multiple possible paths for the CSV file
        String[] possiblePaths = {
            "sample-data/bank_transactions.csv",           // If running from project root
            "../sample-data/bank_transactions.csv",       // If running from control-plane directory
            "../../sample-data/bank_transactions.csv"     // Alternative path
        };
        
        String csvFilePath = null;
        for (String path : possiblePaths) {
            File file = new File(path);
            if (file.exists()) {
                csvFilePath = path;
                logger.info("Found CSV file at: {}", csvFilePath);
                break;
            }
        }
        
        if (csvFilePath == null) {
            throw new IllegalArgumentException("Sample CSV file not found. Tried paths: " + String.join(", ", possiblePaths));
        }
        
        return loadCsvData(csvFilePath, "default", "bank_transactions", true);
    }
    
    /**
     * Get statistics for all loaded tables
     */
    public List<DataLoadingController.TableStatsResponse> getLoadedTablesStats() {
        logger.debug("Getting statistics for all loaded tables");
        
        List<TableInfo> tables = metadataService.listAllTables();
        
        return tables.stream()
                .map(this::convertToTableStatsResponse)
                .collect(Collectors.toList());
    }
    
    /**
     * Get statistics for a specific table
     */
    public DataLoadingController.TableStatsResponse getTableStats(String namespaceName, String tableName) {
        logger.debug("Getting statistics for table: {}.{}", namespaceName, tableName);
        
        Optional<TableInfo> tableInfo = metadataService.getTable(namespaceName, tableName);
        if (tableInfo.isPresent()) {
            return convertToTableStatsResponse(tableInfo.get());
        }
        
        return null;
    }
    
    /**
     * Check if a table has data loaded
     */
    public boolean isTableLoaded(String namespaceName, String tableName) {
        return metadataService.tableExists(namespaceName, tableName);
    }
    
    /**
     * Get the physical location of a table's data
     */
    public Optional<String> getTableLocation(String namespaceName, String tableName) {
        Optional<TableInfo> tableInfo = metadataService.getTable(namespaceName, tableName);
        return tableInfo.map(TableInfo::getTableLocation);
    }
    
    /**
     * Convert Arrow schema to string representation
     */
    private String convertArrowSchemaToString(org.apache.arrow.vector.types.pojo.Schema schema) {
        if (schema == null) {
            return "UNKNOWN";
        }
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < schema.getFields().size(); i++) {
            if (i > 0) sb.append(", ");
            org.apache.arrow.vector.types.pojo.Field field = schema.getFields().get(i);
            sb.append(field.getName()).append(" ").append(field.getType().toString());
        }
        
        return sb.toString();
    }
    
    /**
     * Convert Iceberg schema to string representation
     */
    private String convertIcebergSchemaToString(org.apache.iceberg.Schema schema) {
        if (schema == null) {
            return "UNKNOWN";
        }
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < schema.columns().size(); i++) {
            if (i > 0) sb.append(", ");
            org.apache.iceberg.types.Types.NestedField field = schema.columns().get(i);
            sb.append(field.name()).append(" ").append(field.type().toString());
        }
        
        return sb.toString();
    }
    
    /**
     * Check if Iceberg integration is enabled
     */
    private boolean isIcebergEnabled() {
        return icebergConfiguration != null && 
               icebergConfiguration.isEnabled() && 
               icebergTableCreationService != null;
    }
    
    /**
     * Convert TableInfo to TableStatsResponse
     */
    private DataLoadingController.TableStatsResponse convertToTableStatsResponse(TableInfo tableInfo) {
        return new DataLoadingController.TableStatsResponse(
            tableInfo.getNamespaceName(),
            tableInfo.getTableName(),
            tableInfo.getRowCount() != null ? tableInfo.getRowCount() : 0L,
            tableInfo.getDataSizeBytes() != null ? tableInfo.getDataSizeBytes() : 0L,
            1, // Assume 1 file for now
            0, // Column count not available in TableInfo
            tableInfo.getTableLocation()
        );
    }
    
    /**
     * Result of a data loading operation
     */
    public static class LoadResult {
        private final String tableName;
        private final long rowCount;
        private final long fileSizeBytes;
        private final long durationMs;
        private final String tableLocation;
        private final org.apache.arrow.vector.types.pojo.Schema schema;
        
        public LoadResult(String tableName, long rowCount, long fileSizeBytes, 
                         long durationMs, String tableLocation, 
                         org.apache.arrow.vector.types.pojo.Schema schema) {
            this.tableName = tableName;
            this.rowCount = rowCount;
            this.fileSizeBytes = fileSizeBytes;
            this.durationMs = durationMs;
            this.tableLocation = tableLocation;
            this.schema = schema;
        }
        
        // Getters
        public String getTableName() { return tableName; }
        public long getRowCount() { return rowCount; }
        public long getFileSizeBytes() { return fileSizeBytes; }
        public long getDurationMs() { return durationMs; }
        public String getTableLocation() { return tableLocation; }
        public org.apache.arrow.vector.types.pojo.Schema getSchema() { return schema; }
        
        @Override
        public String toString() {
            return String.format("LoadResult{table='%s', rows=%d, size=%d bytes, duration=%dms}", 
                               tableName, rowCount, fileSizeBytes, durationMs);
        }
    }
}