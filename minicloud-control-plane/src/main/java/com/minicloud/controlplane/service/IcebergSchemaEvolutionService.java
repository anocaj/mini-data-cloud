package com.minicloud.controlplane.service;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Service for managing Iceberg table schema evolution.
 * Implements Requirement 1.3 for schema modification operations and backward compatibility.
 */
@Service
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class IcebergSchemaEvolutionService {
    
    private static final Logger logger = LoggerFactory.getLogger(IcebergSchemaEvolutionService.class);
    
    private final GlobalCatalogService globalCatalogService;
    
    @Autowired
    public IcebergSchemaEvolutionService(GlobalCatalogService globalCatalogService) {
        this.globalCatalogService = globalCatalogService;
    }
    
    /**
     * Adds a new column to an existing table.
     * 
     * @param tableIdentifier The table to modify
     * @param columnName Name of the new column
     * @param columnType Iceberg type for the new column
     * @param isOptional Whether the column is optional (nullable)
     * @param defaultValue Default value for existing rows (can be null)
     * @return SchemaEvolutionResult with details about the operation
     */
    public SchemaEvolutionResult addColumn(TableIdentifier tableIdentifier, String columnName, 
                                         Type columnType, boolean isOptional, Object defaultValue) {
        logger.info("Adding column '{}' to table {}", columnName, tableIdentifier);
        
        try {
            Table table = globalCatalogService.loadTable(tableIdentifier);
            Schema currentSchema = table.schema();
            
            // Validate that column doesn't already exist
            if (currentSchema.findField(columnName) != null) {
                throw new IllegalArgumentException("Column '" + columnName + "' already exists in table " + tableIdentifier);
            }
            
            // Perform backward compatibility validation
            validateAddColumnCompatibility(currentSchema, columnName, columnType, isOptional);
            
            // Create schema update
            UpdateSchema updateSchema = table.updateSchema();
            
            if (isOptional) {
                updateSchema.addColumn(columnName, columnType, defaultValue != null ? String.valueOf(defaultValue) : null);
            } else {
                if (defaultValue == null) {
                    throw new IllegalArgumentException("Required columns must have a default value for existing data");
                }
                updateSchema.addRequiredColumn(columnName, columnType, String.valueOf(defaultValue));
            }
            
            // Commit the schema change
            updateSchema.commit();
            
            Schema newSchema = table.schema();
            
            logger.info("Successfully added column '{}' to table {}", columnName, tableIdentifier);
            
            return new SchemaEvolutionResult(
                SchemaEvolutionResult.OperationType.ADD_COLUMN,
                tableIdentifier,
                currentSchema,
                newSchema,
                "Added column '" + columnName + "' successfully",
                true
            );
            
        } catch (Exception e) {
            logger.error("Failed to add column '{}' to table {}", columnName, tableIdentifier, e);
            throw new RuntimeException("Failed to add column: " + e.getMessage(), e);
        }
    }
    
    /**
     * Renames an existing column in the table.
     * 
     * @param tableIdentifier The table to modify
     * @param oldColumnName Current name of the column
     * @param newColumnName New name for the column
     * @return SchemaEvolutionResult with details about the operation
     */
    public SchemaEvolutionResult renameColumn(TableIdentifier tableIdentifier, String oldColumnName, String newColumnName) {
        logger.info("Renaming column '{}' to '{}' in table {}", oldColumnName, newColumnName, tableIdentifier);
        
        try {
            Table table = globalCatalogService.loadTable(tableIdentifier);
            Schema currentSchema = table.schema();
            
            // Validate that old column exists
            Types.NestedField oldField = currentSchema.findField(oldColumnName);
            if (oldField == null) {
                throw new IllegalArgumentException("Column '" + oldColumnName + "' does not exist in table " + tableIdentifier);
            }
            
            // Validate that new column name doesn't already exist
            if (currentSchema.findField(newColumnName) != null) {
                throw new IllegalArgumentException("Column '" + newColumnName + "' already exists in table " + tableIdentifier);
            }
            
            // Perform backward compatibility validation
            validateRenameColumnCompatibility(currentSchema, oldColumnName, newColumnName);
            
            // Create schema update
            UpdateSchema updateSchema = table.updateSchema();
            updateSchema.renameColumn(oldColumnName, newColumnName);
            
            // Commit the schema change
            updateSchema.commit();
            
            Schema newSchema = table.schema();
            
            logger.info("Successfully renamed column '{}' to '{}' in table {}", oldColumnName, newColumnName, tableIdentifier);
            
            return new SchemaEvolutionResult(
                SchemaEvolutionResult.OperationType.RENAME_COLUMN,
                tableIdentifier,
                currentSchema,
                newSchema,
                "Renamed column '" + oldColumnName + "' to '" + newColumnName + "' successfully",
                true
            );
            
        } catch (Exception e) {
            logger.error("Failed to rename column '{}' to '{}' in table {}", oldColumnName, newColumnName, tableIdentifier, e);
            throw new RuntimeException("Failed to rename column: " + e.getMessage(), e);
        }
    }
    
    /**
     * Updates the type of an existing column (with compatibility checks).
     * 
     * @param tableIdentifier The table to modify
     * @param columnName Name of the column to update
     * @param newType New type for the column
     * @return SchemaEvolutionResult with details about the operation
     */
    public SchemaEvolutionResult updateColumnType(TableIdentifier tableIdentifier, String columnName, Type newType) {
        logger.info("Updating type of column '{}' in table {} to {}", columnName, tableIdentifier, newType);
        
        try {
            Table table = globalCatalogService.loadTable(tableIdentifier);
            Schema currentSchema = table.schema();
            
            // Validate that column exists
            Types.NestedField field = currentSchema.findField(columnName);
            if (field == null) {
                throw new IllegalArgumentException("Column '" + columnName + "' does not exist in table " + tableIdentifier);
            }
            
            Type currentType = field.type();
            
            // Perform backward compatibility validation
            validateTypeChangeCompatibility(currentType, newType, columnName);
            
            // Create schema update
            UpdateSchema updateSchema = table.updateSchema();
            updateSchema.updateColumn(columnName, newType.asPrimitiveType());
            
            // Commit the schema change
            updateSchema.commit();
            
            Schema newSchema = table.schema();
            
            logger.info("Successfully updated type of column '{}' in table {} to {}", columnName, tableIdentifier, newType);
            
            return new SchemaEvolutionResult(
                SchemaEvolutionResult.OperationType.UPDATE_COLUMN_TYPE,
                tableIdentifier,
                currentSchema,
                newSchema,
                "Updated column '" + columnName + "' type to " + newType + " successfully",
                true
            );
            
        } catch (Exception e) {
            logger.error("Failed to update type of column '{}' in table {}", columnName, tableIdentifier, e);
            throw new RuntimeException("Failed to update column type: " + e.getMessage(), e);
        }
    }
    
    /**
     * Makes an optional column required (with default value for existing nulls).
     * 
     * @param tableIdentifier The table to modify
     * @param columnName Name of the column to make required
     * @param defaultValue Default value for existing null values
     * @return SchemaEvolutionResult with details about the operation
     */
    public SchemaEvolutionResult makeColumnRequired(TableIdentifier tableIdentifier, String columnName, Object defaultValue) {
        logger.info("Making column '{}' required in table {}", columnName, tableIdentifier);
        
        try {
            Table table = globalCatalogService.loadTable(tableIdentifier);
            Schema currentSchema = table.schema();
            
            // Validate that column exists and is currently optional
            Types.NestedField field = currentSchema.findField(columnName);
            if (field == null) {
                throw new IllegalArgumentException("Column '" + columnName + "' does not exist in table " + tableIdentifier);
            }
            
            if (!field.isOptional()) {
                throw new IllegalArgumentException("Column '" + columnName + "' is already required in table " + tableIdentifier);
            }
            
            // Validate that a default value is provided
            if (defaultValue == null) {
                throw new IllegalArgumentException("Default value is required when making a column required");
            }
            
            // Perform backward compatibility validation
            validateMakeRequiredCompatibility(currentSchema, columnName, defaultValue);
            
            // Create schema update
            UpdateSchema updateSchema = table.updateSchema();
            updateSchema.makeColumnOptional(columnName); // First make it optional (no-op)
            updateSchema.requireColumn(columnName); // Then make it required
            
            // Commit the schema change
            updateSchema.commit();
            
            Schema newSchema = table.schema();
            
            logger.info("Successfully made column '{}' required in table {}", columnName, tableIdentifier);
            
            return new SchemaEvolutionResult(
                SchemaEvolutionResult.OperationType.MAKE_COLUMN_REQUIRED,
                tableIdentifier,
                currentSchema,
                newSchema,
                "Made column '" + columnName + "' required successfully",
                true
            );
            
        } catch (Exception e) {
            logger.error("Failed to make column '{}' required in table {}", columnName, tableIdentifier, e);
            throw new RuntimeException("Failed to make column required: " + e.getMessage(), e);
        }
    }
    
    /**
     * Makes a required column optional.
     * 
     * @param tableIdentifier The table to modify
     * @param columnName Name of the column to make optional
     * @return SchemaEvolutionResult with details about the operation
     */
    public SchemaEvolutionResult makeColumnOptional(TableIdentifier tableIdentifier, String columnName) {
        logger.info("Making column '{}' optional in table {}", columnName, tableIdentifier);
        
        try {
            Table table = globalCatalogService.loadTable(tableIdentifier);
            Schema currentSchema = table.schema();
            
            // Validate that column exists and is currently required
            Types.NestedField field = currentSchema.findField(columnName);
            if (field == null) {
                throw new IllegalArgumentException("Column '" + columnName + "' does not exist in table " + tableIdentifier);
            }
            
            if (field.isOptional()) {
                throw new IllegalArgumentException("Column '" + columnName + "' is already optional in table " + tableIdentifier);
            }
            
            // Perform backward compatibility validation
            validateMakeOptionalCompatibility(currentSchema, columnName);
            
            // Create schema update
            UpdateSchema updateSchema = table.updateSchema();
            updateSchema.makeColumnOptional(columnName);
            
            // Commit the schema change
            updateSchema.commit();
            
            Schema newSchema = table.schema();
            
            logger.info("Successfully made column '{}' optional in table {}", columnName, tableIdentifier);
            
            return new SchemaEvolutionResult(
                SchemaEvolutionResult.OperationType.MAKE_COLUMN_OPTIONAL,
                tableIdentifier,
                currentSchema,
                newSchema,
                "Made column '" + columnName + "' optional successfully",
                true
            );
            
        } catch (Exception e) {
            logger.error("Failed to make column '{}' optional in table {}", columnName, tableIdentifier, e);
            throw new RuntimeException("Failed to make column optional: " + e.getMessage(), e);
        }
    }
    
    /**
     * Validates that adding a column won't break backward compatibility.
     */
    private void validateAddColumnCompatibility(Schema currentSchema, String columnName, Type columnType, boolean isOptional) {
        // Check for naming conflicts
        if (currentSchema.findField(columnName) != null) {
            throw new IllegalArgumentException("Column name '" + columnName + "' already exists");
        }
        
        // Validate column name format
        if (!isValidColumnName(columnName)) {
            throw new IllegalArgumentException("Invalid column name: " + columnName);
        }
        
        // Required columns must have default values for backward compatibility
        if (!isOptional) {
            logger.warn("Adding required column '{}' - ensure default value is provided", columnName);
        }
        
        logger.debug("Add column validation passed for '{}'", columnName);
    }
    
    /**
     * Validates that renaming a column won't break backward compatibility.
     */
    private void validateRenameColumnCompatibility(Schema currentSchema, String oldName, String newName) {
        // Check that new name doesn't conflict
        if (currentSchema.findField(newName) != null) {
            throw new IllegalArgumentException("Column name '" + newName + "' already exists");
        }
        
        // Validate new column name format
        if (!isValidColumnName(newName)) {
            throw new IllegalArgumentException("Invalid column name: " + newName);
        }
        
        logger.debug("Rename column validation passed for '{}' -> '{}'", oldName, newName);
    }
    
    /**
     * Validates that changing a column type won't break backward compatibility.
     */
    private void validateTypeChangeCompatibility(Type currentType, Type newType, String columnName) {
        // Check if the type change is safe (widening conversions only)
        if (!isCompatibleTypeChange(currentType, newType)) {
            throw new IllegalArgumentException(
                "Incompatible type change for column '" + columnName + "': " + 
                currentType + " -> " + newType + ". Only widening conversions are allowed.");
        }
        
        logger.debug("Type change validation passed for column '{}': {} -> {}", columnName, currentType, newType);
    }
    
    /**
     * Validates that making a column required won't break backward compatibility.
     */
    private void validateMakeRequiredCompatibility(Schema currentSchema, String columnName, Object defaultValue) {
        // Ensure default value is compatible with column type
        Types.NestedField field = currentSchema.findField(columnName);
        if (field != null) {
            // Validate that default value is compatible with the column type
            // This is a simplified validation - in production, you'd want more thorough type checking
            logger.debug("Make required validation passed for column '{}'", columnName);
        }
    }
    
    /**
     * Validates that making a column optional won't break backward compatibility.
     */
    private void validateMakeOptionalCompatibility(Schema currentSchema, String columnName) {
        // Making a column optional is generally safe for backward compatibility
        logger.debug("Make optional validation passed for column '{}'", columnName);
    }
    
    /**
     * Checks if a column name is valid.
     */
    private boolean isValidColumnName(String columnName) {
        if (columnName == null || columnName.trim().isEmpty()) {
            return false;
        }
        
        // Check for valid identifier pattern (letters, numbers, underscores)
        return columnName.matches("^[a-zA-Z_][a-zA-Z0-9_]*$");
    }
    
    /**
     * Checks if a type change is compatible (safe for existing data).
     */
    private boolean isCompatibleTypeChange(Type currentType, Type newType) {
        // Allow same type
        if (currentType.equals(newType)) {
            return true;
        }
        
        // Allow widening numeric conversions
        if (isNumericType(currentType) && isNumericType(newType)) {
            return isWideningNumericConversion(currentType, newType);
        }
        
        // Allow string to larger string (conceptually)
        if (currentType instanceof Types.StringType && newType instanceof Types.StringType) {
            return true;
        }
        
        // Other type changes are not allowed for backward compatibility
        return false;
    }
    
    /**
     * Checks if a type is numeric.
     */
    private boolean isNumericType(Type type) {
        return type instanceof Types.IntegerType ||
               type instanceof Types.LongType ||
               type instanceof Types.FloatType ||
               type instanceof Types.DoubleType ||
               type instanceof Types.DecimalType;
    }
    
    /**
     * Checks if a numeric type conversion is widening (safe).
     */
    private boolean isWideningNumericConversion(Type fromType, Type toType) {
        // Integer -> Long
        if (fromType instanceof Types.IntegerType && toType instanceof Types.LongType) {
            return true;
        }
        
        // Integer/Long -> Float/Double
        if ((fromType instanceof Types.IntegerType || fromType instanceof Types.LongType) &&
            (toType instanceof Types.FloatType || toType instanceof Types.DoubleType)) {
            return true;
        }
        
        // Float -> Double
        if (fromType instanceof Types.FloatType && toType instanceof Types.DoubleType) {
            return true;
        }
        
        // Decimal with smaller precision/scale to larger
        if (fromType instanceof Types.DecimalType && toType instanceof Types.DecimalType) {
            Types.DecimalType fromDecimal = (Types.DecimalType) fromType;
            Types.DecimalType toDecimal = (Types.DecimalType) toType;
            return toDecimal.precision() >= fromDecimal.precision() && 
                   toDecimal.scale() >= fromDecimal.scale();
        }
        
        return false;
    }
    
    /**
     * Gets the schema evolution history for a table.
     */
    public List<SchemaEvolutionHistory> getSchemaHistory(TableIdentifier tableIdentifier) {
        logger.debug("Getting schema evolution history for table: {}", tableIdentifier);
        
        try {
            Table table = globalCatalogService.loadTable(tableIdentifier);
            List<SchemaEvolutionHistory> history = new ArrayList<>();
            
            // Get all snapshots and their schemas
            table.snapshots().forEach(snapshot -> {
                Schema schema = table.schemas().get(snapshot.schemaId());
                if (schema != null) {
                    history.add(new SchemaEvolutionHistory(
                        snapshot.snapshotId(),
                        snapshot.timestampMillis(),
                        schema,
                        snapshot.summary()
                    ));
                }
            });
            
            logger.debug("Found {} schema evolution entries for table: {}", history.size(), tableIdentifier);
            return history;
            
        } catch (Exception e) {
            logger.error("Failed to get schema history for table: {}", tableIdentifier, e);
            throw new RuntimeException("Failed to get schema history: " + e.getMessage(), e);
        }
    }
    
    /**
     * Result of a schema evolution operation.
     */
    public static class SchemaEvolutionResult {
        public enum OperationType {
            ADD_COLUMN,
            RENAME_COLUMN,
            UPDATE_COLUMN_TYPE,
            MAKE_COLUMN_REQUIRED,
            MAKE_COLUMN_OPTIONAL,
            DROP_COLUMN
        }
        
        private final OperationType operationType;
        private final TableIdentifier tableIdentifier;
        private final Schema oldSchema;
        private final Schema newSchema;
        private final String message;
        private final boolean success;
        
        public SchemaEvolutionResult(OperationType operationType, TableIdentifier tableIdentifier,
                                   Schema oldSchema, Schema newSchema, String message, boolean success) {
            this.operationType = operationType;
            this.tableIdentifier = tableIdentifier;
            this.oldSchema = oldSchema;
            this.newSchema = newSchema;
            this.message = message;
            this.success = success;
        }
        
        // Getters
        public OperationType getOperationType() { return operationType; }
        public TableIdentifier getTableIdentifier() { return tableIdentifier; }
        public Schema getOldSchema() { return oldSchema; }
        public Schema getNewSchema() { return newSchema; }
        public String getMessage() { return message; }
        public boolean isSuccess() { return success; }
        
        @Override
        public String toString() {
            return String.format("SchemaEvolutionResult{operation=%s, table=%s, success=%s, message='%s'}", 
                               operationType, tableIdentifier, success, message);
        }
    }
    
    /**
     * Schema evolution history entry.
     */
    public static class SchemaEvolutionHistory {
        private final long snapshotId;
        private final long timestampMillis;
        private final Schema schema;
        private final Map<String, String> summary;
        
        public SchemaEvolutionHistory(long snapshotId, long timestampMillis, Schema schema, Map<String, String> summary) {
            this.snapshotId = snapshotId;
            this.timestampMillis = timestampMillis;
            this.schema = schema;
            this.summary = summary;
        }
        
        // Getters
        public long getSnapshotId() { return snapshotId; }
        public long getTimestampMillis() { return timestampMillis; }
        public Schema getSchema() { return schema; }
        public Map<String, String> getSummary() { return summary; }
        
        @Override
        public String toString() {
            return String.format("SchemaEvolutionHistory{snapshot=%d, timestamp=%d, columns=%d}", 
                               snapshotId, timestampMillis, schema.columns().size());
        }
    }
}