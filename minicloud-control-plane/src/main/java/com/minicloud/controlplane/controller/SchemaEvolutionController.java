package com.minicloud.controlplane.controller;

import com.minicloud.controlplane.service.IcebergSchemaEvolutionService;
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
import java.util.stream.Collectors;

/**
 * REST controller for Iceberg table schema evolution operations.
 * Provides endpoints for managing table schema changes with backward compatibility.
 */
@RestController
@RequestMapping("/api/v1/schema")
@CrossOrigin(origins = "*") // For development - should be restricted in production
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class SchemaEvolutionController {
    
    private static final Logger logger = LoggerFactory.getLogger(SchemaEvolutionController.class);
    
    @Autowired
    private IcebergSchemaEvolutionService schemaEvolutionService;
    
    /**
     * Add a new column to a table.
     */
    @PostMapping("/{namespaceName}/{tableName}/columns")
    public ResponseEntity<SchemaEvolutionResponse> addColumn(
            @PathVariable String namespaceName,
            @PathVariable String tableName,
            @RequestBody AddColumnRequest request) {
        
        logger.info("Adding column '{}' to table {}.{}", request.getColumnName(), namespaceName, tableName);
        
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(namespaceName, tableName);
            Type columnType = parseColumnType(request.getColumnType());
            
            IcebergSchemaEvolutionService.SchemaEvolutionResult result = 
                schemaEvolutionService.addColumn(
                    tableIdentifier, 
                    request.getColumnName(), 
                    columnType, 
                    request.isOptional(), 
                    request.getDefaultValue()
                );
            
            SchemaEvolutionResponse response = new SchemaEvolutionResponse(
                result.getOperationType().toString(),
                result.getTableIdentifier().toString(),
                result.getMessage(),
                result.isSuccess(),
                null
            );
            
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
            
        } catch (Exception e) {
            logger.error("Failed to add column to table {}.{}", namespaceName, tableName, e);
            SchemaEvolutionResponse errorResponse = new SchemaEvolutionResponse(
                "ADD_COLUMN",
                namespaceName + "." + tableName,
                null,
                false,
                e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }
    }
    
    /**
     * Rename a column in a table.
     */
    @PutMapping("/{namespaceName}/{tableName}/columns/{columnName}/rename")
    public ResponseEntity<SchemaEvolutionResponse> renameColumn(
            @PathVariable String namespaceName,
            @PathVariable String tableName,
            @PathVariable String columnName,
            @RequestBody RenameColumnRequest request) {
        
        logger.info("Renaming column '{}' to '{}' in table {}.{}", 
                   columnName, request.getNewColumnName(), namespaceName, tableName);
        
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(namespaceName, tableName);
            
            IcebergSchemaEvolutionService.SchemaEvolutionResult result = 
                schemaEvolutionService.renameColumn(tableIdentifier, columnName, request.getNewColumnName());
            
            SchemaEvolutionResponse response = new SchemaEvolutionResponse(
                result.getOperationType().toString(),
                result.getTableIdentifier().toString(),
                result.getMessage(),
                result.isSuccess(),
                null
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to rename column in table {}.{}", namespaceName, tableName, e);
            SchemaEvolutionResponse errorResponse = new SchemaEvolutionResponse(
                "RENAME_COLUMN",
                namespaceName + "." + tableName,
                null,
                false,
                e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }
    }
    
    /**
     * Update the type of a column in a table.
     */
    @PutMapping("/{namespaceName}/{tableName}/columns/{columnName}/type")
    public ResponseEntity<SchemaEvolutionResponse> updateColumnType(
            @PathVariable String namespaceName,
            @PathVariable String tableName,
            @PathVariable String columnName,
            @RequestBody UpdateColumnTypeRequest request) {
        
        logger.info("Updating type of column '{}' in table {}.{} to {}", 
                   columnName, namespaceName, tableName, request.getNewType());
        
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(namespaceName, tableName);
            Type newType = parseColumnType(request.getNewType());
            
            IcebergSchemaEvolutionService.SchemaEvolutionResult result = 
                schemaEvolutionService.updateColumnType(tableIdentifier, columnName, newType);
            
            SchemaEvolutionResponse response = new SchemaEvolutionResponse(
                result.getOperationType().toString(),
                result.getTableIdentifier().toString(),
                result.getMessage(),
                result.isSuccess(),
                null
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to update column type in table {}.{}", namespaceName, tableName, e);
            SchemaEvolutionResponse errorResponse = new SchemaEvolutionResponse(
                "UPDATE_COLUMN_TYPE",
                namespaceName + "." + tableName,
                null,
                false,
                e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }
    }
    
    /**
     * Make a column required.
     */
    @PutMapping("/{namespaceName}/{tableName}/columns/{columnName}/required")
    public ResponseEntity<SchemaEvolutionResponse> makeColumnRequired(
            @PathVariable String namespaceName,
            @PathVariable String tableName,
            @PathVariable String columnName,
            @RequestBody MakeColumnRequiredRequest request) {
        
        logger.info("Making column '{}' required in table {}.{}", columnName, namespaceName, tableName);
        
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(namespaceName, tableName);
            
            IcebergSchemaEvolutionService.SchemaEvolutionResult result = 
                schemaEvolutionService.makeColumnRequired(tableIdentifier, columnName, request.getDefaultValue());
            
            SchemaEvolutionResponse response = new SchemaEvolutionResponse(
                result.getOperationType().toString(),
                result.getTableIdentifier().toString(),
                result.getMessage(),
                result.isSuccess(),
                null
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to make column required in table {}.{}", namespaceName, tableName, e);
            SchemaEvolutionResponse errorResponse = new SchemaEvolutionResponse(
                "MAKE_COLUMN_REQUIRED",
                namespaceName + "." + tableName,
                null,
                false,
                e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }
    }
    
    /**
     * Make a column optional.
     */
    @PutMapping("/{namespaceName}/{tableName}/columns/{columnName}/optional")
    public ResponseEntity<SchemaEvolutionResponse> makeColumnOptional(
            @PathVariable String namespaceName,
            @PathVariable String tableName,
            @PathVariable String columnName) {
        
        logger.info("Making column '{}' optional in table {}.{}", columnName, namespaceName, tableName);
        
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(namespaceName, tableName);
            
            IcebergSchemaEvolutionService.SchemaEvolutionResult result = 
                schemaEvolutionService.makeColumnOptional(tableIdentifier, columnName);
            
            SchemaEvolutionResponse response = new SchemaEvolutionResponse(
                result.getOperationType().toString(),
                result.getTableIdentifier().toString(),
                result.getMessage(),
                result.isSuccess(),
                null
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to make column optional in table {}.{}", namespaceName, tableName, e);
            SchemaEvolutionResponse errorResponse = new SchemaEvolutionResponse(
                "MAKE_COLUMN_OPTIONAL",
                namespaceName + "." + tableName,
                null,
                false,
                e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }
    }
    
    /**
     * Get schema evolution history for a table.
     */
    @GetMapping("/{namespaceName}/{tableName}/history")
    public ResponseEntity<List<SchemaHistoryResponse>> getSchemaHistory(
            @PathVariable String namespaceName,
            @PathVariable String tableName) {
        
        logger.info("Getting schema history for table {}.{}", namespaceName, tableName);
        
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(namespaceName, tableName);
            
            List<IcebergSchemaEvolutionService.SchemaEvolutionHistory> history = 
                schemaEvolutionService.getSchemaHistory(tableIdentifier);
            
            List<SchemaHistoryResponse> response = history.stream()
                .map(entry -> new SchemaHistoryResponse(
                    entry.getSnapshotId(),
                    entry.getTimestampMillis(),
                    entry.getSchema().columns().size(),
                    entry.getSchema().columns().stream()
                        .map(field -> field.name() + ":" + field.type().toString())
                        .collect(Collectors.toList()),
                    entry.getSummary()
                ))
                .collect(Collectors.toList());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to get schema history for table {}.{}", namespaceName, tableName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Parse column type string to Iceberg Type.
     */
    private Type parseColumnType(String typeString) {
        if (typeString == null) {
            throw new IllegalArgumentException("Column type cannot be null");
        }
        
        String lowerType = typeString.toLowerCase().trim();
        
        switch (lowerType) {
            case "boolean":
            case "bool":
                return Types.BooleanType.get();
            case "integer":
            case "int":
                return Types.IntegerType.get();
            case "long":
            case "bigint":
                return Types.LongType.get();
            case "float":
                return Types.FloatType.get();
            case "double":
                return Types.DoubleType.get();
            case "string":
            case "varchar":
            case "text":
                return Types.StringType.get();
            case "date":
                return Types.DateType.get();
            case "timestamp":
                return Types.TimestampType.withoutZone();
            default:
                // Handle decimal types
                if (lowerType.startsWith("decimal(")) {
                    return parseDecimalType(lowerType);
                }
                throw new IllegalArgumentException("Unsupported column type: " + typeString);
        }
    }
    
    /**
     * Parse decimal type with precision and scale.
     */
    private Type parseDecimalType(String typeString) {
        try {
            // Extract precision and scale from "decimal(precision,scale)"
            String params = typeString.substring(8, typeString.length() - 1); // Remove "decimal(" and ")"
            String[] parts = params.split(",");
            
            int precision = Integer.parseInt(parts[0].trim());
            int scale = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
            
            return Types.DecimalType.of(precision, scale);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid decimal type format: " + typeString + ". Expected format: decimal(precision,scale)");
        }
    }
    
    // Request/Response DTOs
    
    public static class AddColumnRequest {
        private String columnName;
        private String columnType;
        private boolean optional = true;
        private Object defaultValue;
        
        // Getters and setters
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
    
    public static class MakeColumnRequiredRequest {
        private Object defaultValue;
        
        public Object getDefaultValue() { return defaultValue; }
        public void setDefaultValue(Object defaultValue) { this.defaultValue = defaultValue; }
    }
    
    public static class SchemaEvolutionResponse {
        private final String operation;
        private final String tableIdentifier;
        private final String message;
        private final boolean success;
        private final String error;
        
        public SchemaEvolutionResponse(String operation, String tableIdentifier, String message, boolean success, String error) {
            this.operation = operation;
            this.tableIdentifier = tableIdentifier;
            this.message = message;
            this.success = success;
            this.error = error;
        }
        
        // Getters
        public String getOperation() { return operation; }
        public String getTableIdentifier() { return tableIdentifier; }
        public String getMessage() { return message; }
        public boolean isSuccess() { return success; }
        public String getError() { return error; }
    }
    
    public static class SchemaHistoryResponse {
        private final long snapshotId;
        private final long timestampMillis;
        private final int columnCount;
        private final List<String> columns;
        private final java.util.Map<String, String> summary;
        
        public SchemaHistoryResponse(long snapshotId, long timestampMillis, int columnCount, 
                                   List<String> columns, java.util.Map<String, String> summary) {
            this.snapshotId = snapshotId;
            this.timestampMillis = timestampMillis;
            this.columnCount = columnCount;
            this.columns = columns;
            this.summary = summary;
        }
        
        // Getters
        public long getSnapshotId() { return snapshotId; }
        public long getTimestampMillis() { return timestampMillis; }
        public int getColumnCount() { return columnCount; }
        public List<String> getColumns() { return columns; }
        public java.util.Map<String, String> getSummary() { return summary; }
    }
}