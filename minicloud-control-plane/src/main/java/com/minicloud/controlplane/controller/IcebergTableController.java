package com.minicloud.controlplane.controller;

import com.minicloud.controlplane.service.GlobalCatalogService;
import com.minicloud.controlplane.service.IcebergTableCreationService;
import com.minicloud.controlplane.service.IcebergSchemaEvolutionService;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * REST controller for Iceberg table management operations.
 * Implements Requirements 1.5 and 4.3 for table creation, deletion, and metadata retrieval.
 */
@RestController
@RequestMapping("/api/v1/iceberg/tables")
@CrossOrigin(origins = "*")
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class IcebergTableController {
    
    private static final Logger logger = LoggerFactory.getLogger(IcebergTableController.class);
    
    private final GlobalCatalogService globalCatalogService;
    private final IcebergTableCreationService tableCreationService;
    private final IcebergSchemaEvolutionService schemaEvolutionService;
    
    @Autowired
    public IcebergTableController(GlobalCatalogService globalCatalogService,
                                IcebergTableCreationService tableCreationService,
                                IcebergSchemaEvolutionService schemaEvolutionService) {
        this.globalCatalogService = globalCatalogService;
        this.tableCreationService = tableCreationService;
        this.schemaEvolutionService = schemaEvolutionService;
    }
    
    /**
     * Create a new Iceberg table from CSV data.
     */
    @PostMapping("/namespaces/{namespace}/tables/{tableName}/create-from-csv")
    public ResponseEntity<CreateTableResponse> createTableFromCsv(
            @PathVariable String namespace,
            @PathVariable String tableName,
            @RequestBody CreateTableFromCsvRequest request) {
        
        logger.info("Creating Iceberg table {}.{} from CSV: {}", namespace, tableName, request.getCsvFilePath());
        
        try {
            IcebergTableCreationService.TableCreationResult result = tableCreationService.createTableFromCsv(
                request.getCsvFilePath(), namespace, tableName, request.isHasHeader());
            
            CreateTableResponse response = new CreateTableResponse(
                result.getTableIdentifier().toString(),
                result.getSchema().asStruct().toString(),
                result.getRowCount(),
                result.getDurationMs(),
                result.getTableLocation(),
                result.getSnapshotId()
            );
            
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
            
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid request for table creation: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            logger.error("Failed to create table {}.{}", namespace, tableName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Create a new empty Iceberg table with specified schema.
     */
    @PostMapping("/namespaces/{namespace}/tables/{tableName}")
    public ResponseEntity<TableMetadataResponse> createTable(
            @PathVariable String namespace,
            @PathVariable String tableName,
            @RequestBody CreateTableRequest request) {
        
        logger.info("Creating empty Iceberg table {}.{}", namespace, tableName);
        
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
            
            // Convert schema definition to Iceberg schema
            Schema schema = parseSchemaDefinition(request.getSchemaDefinition());
            
            // Create table with default partition spec (unpartitioned)
            Table table = globalCatalogService.createTable(tableIdentifier, schema, 
                org.apache.iceberg.PartitionSpec.unpartitioned(), request.getProperties());
            
            TableMetadataResponse response = createTableMetadataResponse(table);
            
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
            
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid request for table creation: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            logger.error("Failed to create table {}.{}", namespace, tableName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Get table metadata.
     */
    @GetMapping("/namespaces/{namespace}/tables/{tableName}")
    public ResponseEntity<TableMetadataResponse> getTable(
            @PathVariable String namespace,
            @PathVariable String tableName) {
        
        logger.debug("Getting metadata for table {}.{}", namespace, tableName);
        
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
            Table table = globalCatalogService.loadTable(tableIdentifier);
            
            TableMetadataResponse response = createTableMetadataResponse(table);
            
            return ResponseEntity.ok(response);
            
        } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
            logger.warn("Table not found: {}.{}", namespace, tableName);
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            logger.error("Failed to get table metadata for {}.{}", namespace, tableName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * List all tables in a namespace.
     */
    @GetMapping("/namespaces/{namespace}/tables")
    public ResponseEntity<List<String>> listTables(@PathVariable String namespace) {
        logger.debug("Listing tables in namespace: {}", namespace);
        
        try {
            Namespace ns = Namespace.of(namespace);
            List<TableIdentifier> tables = globalCatalogService.listTables(ns);
            
            List<String> tableNames = tables.stream()
                .map(TableIdentifier::name)
                .collect(Collectors.toList());
            
            return ResponseEntity.ok(tableNames);
            
        } catch (Exception e) {
            logger.error("Failed to list tables in namespace: {}", namespace, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Delete an Iceberg table.
     */
    @DeleteMapping("/namespaces/{namespace}/tables/{tableName}")
    public ResponseEntity<Void> deleteTable(
            @PathVariable String namespace,
            @PathVariable String tableName,
            @RequestParam(defaultValue = "false") boolean purge) {
        
        logger.info("Deleting table {}.{} (purge={})", namespace, tableName, purge);
        
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
            boolean deleted = globalCatalogService.dropTable(tableIdentifier, purge);
            
            if (deleted) {
                return ResponseEntity.ok().build();
            } else {
                return ResponseEntity.notFound().build();
            }
            
        } catch (Exception e) {
            logger.error("Failed to delete table {}.{}", namespace, tableName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Get table schema.
     */
    @GetMapping("/namespaces/{namespace}/tables/{tableName}/schema")
    public ResponseEntity<SchemaResponse> getTableSchema(
            @PathVariable String namespace,
            @PathVariable String tableName) {
        
        logger.debug("Getting schema for table {}.{}", namespace, tableName);
        
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
            Table table = globalCatalogService.loadTable(tableIdentifier);
            
            SchemaResponse response = new SchemaResponse(
                table.schema().schemaId(),
                table.schema().asStruct().toString(),
                table.schema().columns().stream()
                    .map(field -> new ColumnInfo(
                        field.fieldId(),
                        field.name(),
                        field.type().toString(),
                        field.isOptional()
                    ))
                    .collect(Collectors.toList())
            );
            
            return ResponseEntity.ok(response);
            
        } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
            logger.warn("Table not found: {}.{}", namespace, tableName);
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            logger.error("Failed to get schema for table {}.{}", namespace, tableName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Add a column to the table schema.
     */
    @PostMapping("/namespaces/{namespace}/tables/{tableName}/schema/columns")
    public ResponseEntity<SchemaEvolutionResponse> addColumn(
            @PathVariable String namespace,
            @PathVariable String tableName,
            @RequestBody AddColumnRequest request) {
        
        logger.info("Adding column '{}' to table {}.{}", request.getColumnName(), namespace, tableName);
        
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
            Type columnType = parseTypeDefinition(request.getColumnType());
            
            IcebergSchemaEvolutionService.SchemaEvolutionResult result = 
                schemaEvolutionService.addColumn(tableIdentifier, request.getColumnName(), 
                    columnType, request.isOptional(), request.getDefaultValue());
            
            SchemaEvolutionResponse response = new SchemaEvolutionResponse(
                result.getOperationType().toString(),
                result.getTableIdentifier().toString(),
                result.getMessage(),
                result.isSuccess(),
                result.getNewSchema().asStruct().toString()
            );
            
            return ResponseEntity.ok(response);
            
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid request for adding column: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            logger.error("Failed to add column to table {}.{}", namespace, tableName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Rename a column in the table schema.
     */
    @PutMapping("/namespaces/{namespace}/tables/{tableName}/schema/columns/{columnName}/rename")
    public ResponseEntity<SchemaEvolutionResponse> renameColumn(
            @PathVariable String namespace,
            @PathVariable String tableName,
            @PathVariable String columnName,
            @RequestBody RenameColumnRequest request) {
        
        logger.info("Renaming column '{}' to '{}' in table {}.{}", 
                   columnName, request.getNewColumnName(), namespace, tableName);
        
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
            
            IcebergSchemaEvolutionService.SchemaEvolutionResult result = 
                schemaEvolutionService.renameColumn(tableIdentifier, columnName, request.getNewColumnName());
            
            SchemaEvolutionResponse response = new SchemaEvolutionResponse(
                result.getOperationType().toString(),
                result.getTableIdentifier().toString(),
                result.getMessage(),
                result.isSuccess(),
                result.getNewSchema().asStruct().toString()
            );
            
            return ResponseEntity.ok(response);
            
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid request for renaming column: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            logger.error("Failed to rename column in table {}.{}", namespace, tableName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Update column type in the table schema.
     */
    @PutMapping("/namespaces/{namespace}/tables/{tableName}/schema/columns/{columnName}/type")
    public ResponseEntity<SchemaEvolutionResponse> updateColumnType(
            @PathVariable String namespace,
            @PathVariable String tableName,
            @PathVariable String columnName,
            @RequestBody UpdateColumnTypeRequest request) {
        
        logger.info("Updating type of column '{}' in table {}.{} to {}", 
                   columnName, namespace, tableName, request.getNewType());
        
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
            Type newType = parseTypeDefinition(request.getNewType());
            
            IcebergSchemaEvolutionService.SchemaEvolutionResult result = 
                schemaEvolutionService.updateColumnType(tableIdentifier, columnName, newType);
            
            SchemaEvolutionResponse response = new SchemaEvolutionResponse(
                result.getOperationType().toString(),
                result.getTableIdentifier().toString(),
                result.getMessage(),
                result.isSuccess(),
                result.getNewSchema().asStruct().toString()
            );
            
            return ResponseEntity.ok(response);
            
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid request for updating column type: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            logger.error("Failed to update column type in table {}.{}", namespace, tableName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Get schema evolution history for a table.
     */
    @GetMapping("/namespaces/{namespace}/tables/{tableName}/schema/history")
    public ResponseEntity<List<SchemaHistoryEntry>> getSchemaHistory(
            @PathVariable String namespace,
            @PathVariable String tableName) {
        
        logger.debug("Getting schema history for table {}.{}", namespace, tableName);
        
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
            List<IcebergSchemaEvolutionService.SchemaEvolutionHistory> history = 
                schemaEvolutionService.getSchemaHistory(tableIdentifier);
            
            List<SchemaHistoryEntry> response = history.stream()
                .map(entry -> new SchemaHistoryEntry(
                    entry.getSnapshotId(),
                    entry.getTimestampMillis(),
                    entry.getSchema().schemaId(),
                    entry.getSchema().columns().size(),
                    entry.getSummary()
                ))
                .collect(Collectors.toList());
            
            return ResponseEntity.ok(response);
            
        } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
            logger.warn("Table not found: {}.{}", namespace, tableName);
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            logger.error("Failed to get schema history for table {}.{}", namespace, tableName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    // Helper methods
    
    private TableMetadataResponse createTableMetadataResponse(Table table) {
        return new TableMetadataResponse(
            table.name(),
            table.location(),
            table.schema().schemaId(),
            table.schema().asStruct().toString(),
            table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : null,
            table.currentSnapshot() != null ? table.currentSnapshot().timestampMillis() : null,
            table.properties()
        );
    }
    
    private Schema parseSchemaDefinition(String schemaDefinition) {
        // Simplified schema parsing - in production, you'd want more robust parsing
        // For now, assume JSON-like format or use a proper schema parser
        throw new UnsupportedOperationException("Schema parsing not yet implemented");
    }
    
    private Type parseTypeDefinition(String typeDefinition) {
        // Parse type definition string to Iceberg Type
        switch (typeDefinition.toLowerCase()) {
            case "string":
                return Types.StringType.get();
            case "int":
            case "integer":
                return Types.IntegerType.get();
            case "long":
                return Types.LongType.get();
            case "double":
                return Types.DoubleType.get();
            case "boolean":
                return Types.BooleanType.get();
            case "date":
                return Types.DateType.get();
            case "timestamp":
                return Types.TimestampType.withoutZone();
            default:
                throw new IllegalArgumentException("Unsupported type: " + typeDefinition);
        }
    }
    
    // Request/Response DTOs
    
    public static class CreateTableFromCsvRequest {
        private String csvFilePath;
        private boolean hasHeader = true;
        
        public String getCsvFilePath() { return csvFilePath; }
        public void setCsvFilePath(String csvFilePath) { this.csvFilePath = csvFilePath; }
        public boolean isHasHeader() { return hasHeader; }
        public void setHasHeader(boolean hasHeader) { this.hasHeader = hasHeader; }
    }
    
    public static class CreateTableRequest {
        private String schemaDefinition;
        private Map<String, String> properties;
        
        public String getSchemaDefinition() { return schemaDefinition; }
        public void setSchemaDefinition(String schemaDefinition) { this.schemaDefinition = schemaDefinition; }
        public Map<String, String> getProperties() { return properties; }
        public void setProperties(Map<String, String> properties) { this.properties = properties; }
    }
    
    public static class CreateTableResponse {
        private final String tableIdentifier;
        private final String schema;
        private final long rowCount;
        private final long durationMs;
        private final String location;
        private final Long snapshotId;
        
        public CreateTableResponse(String tableIdentifier, String schema, long rowCount, 
                                 long durationMs, String location, Long snapshotId) {
            this.tableIdentifier = tableIdentifier;
            this.schema = schema;
            this.rowCount = rowCount;
            this.durationMs = durationMs;
            this.location = location;
            this.snapshotId = snapshotId;
        }
        
        public String getTableIdentifier() { return tableIdentifier; }
        public String getSchema() { return schema; }
        public long getRowCount() { return rowCount; }
        public long getDurationMs() { return durationMs; }
        public String getLocation() { return location; }
        public Long getSnapshotId() { return snapshotId; }
    }
    
    public static class TableMetadataResponse {
        private final String name;
        private final String location;
        private final int schemaId;
        private final String schema;
        private final Long currentSnapshotId;
        private final Long lastModified;
        private final Map<String, String> properties;
        
        public TableMetadataResponse(String name, String location, int schemaId, String schema,
                                   Long currentSnapshotId, Long lastModified, Map<String, String> properties) {
            this.name = name;
            this.location = location;
            this.schemaId = schemaId;
            this.schema = schema;
            this.currentSnapshotId = currentSnapshotId;
            this.lastModified = lastModified;
            this.properties = properties;
        }
        
        public String getName() { return name; }
        public String getLocation() { return location; }
        public int getSchemaId() { return schemaId; }
        public String getSchema() { return schema; }
        public Long getCurrentSnapshotId() { return currentSnapshotId; }
        public Long getLastModified() { return lastModified; }
        public Map<String, String> getProperties() { return properties; }
    }
    
    public static class SchemaResponse {
        private final int schemaId;
        private final String schemaDefinition;
        private final List<ColumnInfo> columns;
        
        public SchemaResponse(int schemaId, String schemaDefinition, List<ColumnInfo> columns) {
            this.schemaId = schemaId;
            this.schemaDefinition = schemaDefinition;
            this.columns = columns;
        }
        
        public int getSchemaId() { return schemaId; }
        public String getSchemaDefinition() { return schemaDefinition; }
        public List<ColumnInfo> getColumns() { return columns; }
    }
    
    public static class ColumnInfo {
        private final int fieldId;
        private final String name;
        private final String type;
        private final boolean optional;
        
        public ColumnInfo(int fieldId, String name, String type, boolean optional) {
            this.fieldId = fieldId;
            this.name = name;
            this.type = type;
            this.optional = optional;
        }
        
        public int getFieldId() { return fieldId; }
        public String getName() { return name; }
        public String getType() { return type; }
        public boolean isOptional() { return optional; }
    }
    
    public static class AddColumnRequest {
        private String columnName;
        private String columnType;
        private boolean optional = true;
        private Object defaultValue;
        
        public String getColumnName() { return columnName; }
        public void setColumnName(String columnName) { this.columnName = columnName; }
        public String getColumnType() { return columnType; }
        public void setColumnType(String columnType) { this.columnType = columnType; }
        public boolean isOptional() { return optional; }
        public void setOptional(boolean optional) { this.optional = optional; }
        public Object getDefaultValue() { return defaultValue; }
        public void setDefaultValue(Object defaultValue) { this.defaultValue = defaultValue; }
    }
    
    public static class RenameColumnRequest {
        private String newColumnName;
        
        public String getNewColumnName() { return newColumnName; }
        public void setNewColumnName(String newColumnName) { this.newColumnName = newColumnName; }
    }
    
    public static class UpdateColumnTypeRequest {
        private String newType;
        
        public String getNewType() { return newType; }
        public void setNewType(String newType) { this.newType = newType; }
    }
    
    public static class SchemaEvolutionResponse {
        private final String operationType;
        private final String tableIdentifier;
        private final String message;
        private final boolean success;
        private final String newSchema;
        
        public SchemaEvolutionResponse(String operationType, String tableIdentifier, 
                                     String message, boolean success, String newSchema) {
            this.operationType = operationType;
            this.tableIdentifier = tableIdentifier;
            this.message = message;
            this.success = success;
            this.newSchema = newSchema;
        }
        
        public String getOperationType() { return operationType; }
        public String getTableIdentifier() { return tableIdentifier; }
        public String getMessage() { return message; }
        public boolean isSuccess() { return success; }
        public String getNewSchema() { return newSchema; }
    }
    
    public static class SchemaHistoryEntry {
        private final long snapshotId;
        private final long timestampMillis;
        private final int schemaId;
        private final int columnCount;
        private final Map<String, String> summary;
        
        public SchemaHistoryEntry(long snapshotId, long timestampMillis, int schemaId, 
                                int columnCount, Map<String, String> summary) {
            this.snapshotId = snapshotId;
            this.timestampMillis = timestampMillis;
            this.schemaId = schemaId;
            this.columnCount = columnCount;
            this.summary = summary;
        }
        
        public long getSnapshotId() { return snapshotId; }
        public long getTimestampMillis() { return timestampMillis; }
        public int getSchemaId() { return schemaId; }
        public int getColumnCount() { return columnCount; }
        public Map<String, String> getSummary() { return summary; }
    }
}