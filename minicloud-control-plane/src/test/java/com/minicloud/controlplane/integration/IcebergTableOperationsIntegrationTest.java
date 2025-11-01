package com.minicloud.controlplane.integration;

import com.minicloud.controlplane.service.IcebergTableCreationService;
import com.minicloud.controlplane.service.IcebergSchemaEvolutionService;
import com.minicloud.controlplane.service.IcebergTransactionService;
import com.minicloud.controlplane.service.GlobalCatalogService;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Iceberg table operations.
 * Tests end-to-end table creation, schema evolution, and ACID transactions.
 * 
 * Note: These tests require Iceberg to be enabled and properly configured.
 */
@SpringBootTest
@ActiveProfiles("test")
@EnabledIfSystemProperty(named = "minicloud.iceberg.enabled", matches = "true")
public class IcebergTableOperationsIntegrationTest {
    
    private static final Logger logger = LoggerFactory.getLogger(IcebergTableOperationsIntegrationTest.class);
    
    @Autowired(required = false)
    private IcebergTableCreationService tableCreationService;
    
    @Autowired(required = false)
    private IcebergSchemaEvolutionService schemaEvolutionService;
    
    @Autowired(required = false)
    private IcebergTransactionService transactionService;
    
    @Autowired(required = false)
    private GlobalCatalogService globalCatalogService;
    
    /**
     * Test end-to-end table creation from CSV.
     * Requirement: 1.1, 1.2
     */
    @Test
    public void testTableCreationFromCsv() throws IOException {
        // Skip test if Iceberg is not enabled
        if (tableCreationService == null) {
            logger.info("Skipping Iceberg table creation test - Iceberg not enabled");
            return;
        }
        
        logger.info("Testing Iceberg table creation from CSV");
        
        // Create a temporary CSV file
        File tempCsv = createTempCsvFile();
        
        try {
            // Create table from CSV
            IcebergTableCreationService.TableCreationResult result = 
                tableCreationService.createTableFromCsv(
                    tempCsv.getAbsolutePath(), 
                    "test", 
                    "integration_test_table", 
                    true
                );
            
            // Verify table creation
            assertNotNull(result);
            assertEquals("test.integration_test_table", result.getTableIdentifier().toString());
            assertTrue(result.getRowCount() > 0);
            assertNotNull(result.getSchema());
            
            logger.info("Table creation test passed: {}", result);
            
        } finally {
            // Clean up
            tempCsv.delete();
            cleanupTable(TableIdentifier.of("test", "integration_test_table"));
        }
    }
    
    /**
     * Test schema evolution scenarios.
     * Requirement: 1.3
     */
    @Test
    public void testSchemaEvolution() throws IOException {
        // Skip test if Iceberg is not enabled
        if (tableCreationService == null || schemaEvolutionService == null) {
            logger.info("Skipping schema evolution test - Iceberg not enabled");
            return;
        }
        
        logger.info("Testing Iceberg schema evolution");
        
        // Create a test table first
        File tempCsv = createTempCsvFile();
        TableIdentifier tableId = TableIdentifier.of("test", "schema_evolution_test");
        
        try {
            // Create initial table
            tableCreationService.createTableFromCsv(
                tempCsv.getAbsolutePath(), 
                "test", 
                "schema_evolution_test", 
                true
            );
            
            // Test adding a column
            IcebergSchemaEvolutionService.SchemaEvolutionResult addResult = 
                schemaEvolutionService.addColumn(
                    tableId, 
                    "new_column", 
                    Types.StringType.get(), 
                    true, 
                    "default_value"
                );
            
            assertTrue(addResult.isSuccess());
            assertEquals(IcebergSchemaEvolutionService.SchemaEvolutionResult.OperationType.ADD_COLUMN, 
                        addResult.getOperationType());
            
            // Test renaming a column
            IcebergSchemaEvolutionService.SchemaEvolutionResult renameResult = 
                schemaEvolutionService.renameColumn(tableId, "new_column", "renamed_column");
            
            assertTrue(renameResult.isSuccess());
            assertEquals(IcebergSchemaEvolutionService.SchemaEvolutionResult.OperationType.RENAME_COLUMN, 
                        renameResult.getOperationType());
            
            logger.info("Schema evolution test passed");
            
        } finally {
            // Clean up
            tempCsv.delete();
            cleanupTable(tableId);
        }
    }
    
    /**
     * Test ACID transaction properties.
     * Requirement: 5.1
     */
    @Test
    public void testAcidTransactions() throws IOException {
        // Skip test if Iceberg is not enabled
        if (tableCreationService == null || transactionService == null) {
            logger.info("Skipping ACID transaction test - Iceberg not enabled");
            return;
        }
        
        logger.info("Testing ACID transactions");
        
        // Create a test table first
        File tempCsv = createTempCsvFile();
        TableIdentifier tableId = TableIdentifier.of("test", "transaction_test");
        
        try {
            // Create initial table
            tableCreationService.createTableFromCsv(
                tempCsv.getAbsolutePath(), 
                "test", 
                "transaction_test", 
                true
            );
            
            // Test transaction lifecycle
            IcebergTransactionService.TransactionContext context = 
                transactionService.beginTransaction(tableId);
            
            assertNotNull(context);
            assertNotNull(context.getTransactionId());
            assertEquals(IcebergTransactionService.TransactionStatus.ACTIVE, context.getStatus());
            
            // Test transaction commit
            IcebergTransactionService.TransactionResult commitResult = 
                transactionService.commitTransaction(context.getTransactionId());
            
            assertTrue(commitResult.getMessage().contains("successfully"));
            assertEquals(IcebergTransactionService.TransactionStatus.COMMITTED, commitResult.getStatus());
            
            // Test transaction rollback
            IcebergTransactionService.TransactionContext rollbackContext = 
                transactionService.beginTransaction(tableId);
            
            IcebergTransactionService.TransactionResult rollbackResult = 
                transactionService.rollbackTransaction(rollbackContext.getTransactionId());
            
            assertEquals(IcebergTransactionService.TransactionStatus.ROLLED_BACK, rollbackResult.getStatus());
            
            logger.info("ACID transaction test passed");
            
        } finally {
            // Clean up
            tempCsv.delete();
            cleanupTable(tableId);
        }
    }
    
    /**
     * Creates a temporary CSV file for testing.
     */
    private File createTempCsvFile() throws IOException {
        File tempFile = File.createTempFile("iceberg_test", ".csv");
        
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("id,name,age,email\n");
            writer.write("1,John Doe,30,john@example.com\n");
            writer.write("2,Jane Smith,25,jane@example.com\n");
            writer.write("3,Bob Johnson,35,bob@example.com\n");
        }
        
        return tempFile;
    }
    
    /**
     * Cleans up a test table.
     */
    private void cleanupTable(TableIdentifier tableId) {
        try {
            if (globalCatalogService != null && globalCatalogService.tableExists(tableId)) {
                globalCatalogService.dropTable(tableId, true);
                logger.debug("Cleaned up test table: {}", tableId);
            }
        } catch (Exception e) {
            logger.warn("Failed to cleanup test table: {}", tableId, e);
        }
    }
}