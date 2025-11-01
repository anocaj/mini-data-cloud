package com.minicloud.controlplane.controller;

import com.minicloud.controlplane.service.IcebergTransactionService;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

/**
 * REST controller for managing ACID transactions on Iceberg tables.
 * Provides endpoints for transaction lifecycle management and operations.
 */
@RestController
@RequestMapping("/api/v1/transactions")
@CrossOrigin(origins = "*") // For development - should be restricted in production
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class TransactionController {
    
    private static final Logger logger = LoggerFactory.getLogger(TransactionController.class);
    
    @Autowired
    private IcebergTransactionService transactionService;
    
    /**
     * Begin a new transaction on a table.
     */
    @PostMapping("/begin")
    public ResponseEntity<TransactionResponse> beginTransaction(@RequestBody BeginTransactionRequest request) {
        logger.info("Beginning transaction on table: {}.{}", request.getNamespaceName(), request.getTableName());
        
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(request.getNamespaceName(), request.getTableName());
            
            IcebergTransactionService.TransactionContext context = 
                transactionService.beginTransaction(tableIdentifier);
            
            TransactionResponse response = new TransactionResponse(
                context.getTransactionId(),
                context.getTableIdentifier().toString(),
                context.getStatus().toString(),
                context.getStartTime().toEpochMilli(),
                null,
                0,
                "Transaction started successfully",
                null
            );
            
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
            
        } catch (Exception e) {
            logger.error("Failed to begin transaction on table {}.{}", request.getNamespaceName(), request.getTableName(), e);
            TransactionResponse errorResponse = new TransactionResponse(
                null,
                request.getNamespaceName() + "." + request.getTableName(),
                "FAILED",
                System.currentTimeMillis(),
                null,
                0,
                null,
                e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }
    }
    
    /**
     * Commit a transaction.
     */
    @PostMapping("/{transactionId}/commit")
    public ResponseEntity<TransactionResponse> commitTransaction(@PathVariable String transactionId) {
        logger.info("Committing transaction: {}", transactionId);
        
        try {
            IcebergTransactionService.TransactionResult result = 
                transactionService.commitTransaction(transactionId);
            
            TransactionResponse response = new TransactionResponse(
                result.getTransactionId(),
                result.getTableIdentifier().toString(),
                result.getStatus().toString(),
                System.currentTimeMillis() - result.getDurationMs(),
                System.currentTimeMillis(),
                result.getOperationCount(),
                result.getMessage(),
                result.getError()
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to commit transaction: {}", transactionId, e);
            TransactionResponse errorResponse = new TransactionResponse(
                transactionId,
                null,
                "FAILED",
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                0,
                null,
                e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }
    }
    
    /**
     * Rollback a transaction.
     */
    @PostMapping("/{transactionId}/rollback")
    public ResponseEntity<TransactionResponse> rollbackTransaction(@PathVariable String transactionId) {
        logger.info("Rolling back transaction: {}", transactionId);
        
        try {
            IcebergTransactionService.TransactionResult result = 
                transactionService.rollbackTransaction(transactionId);
            
            TransactionResponse response = new TransactionResponse(
                result.getTransactionId(),
                result.getTableIdentifier().toString(),
                result.getStatus().toString(),
                System.currentTimeMillis() - result.getDurationMs(),
                System.currentTimeMillis(),
                result.getOperationCount(),
                result.getMessage(),
                result.getError()
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to rollback transaction: {}", transactionId, e);
            TransactionResponse errorResponse = new TransactionResponse(
                transactionId,
                null,
                "FAILED",
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                0,
                null,
                e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }
    }
    
    /**
     * Get transaction status.
     */
    @GetMapping("/{transactionId}/status")
    public ResponseEntity<TransactionStatusResponse> getTransactionStatus(@PathVariable String transactionId) {
        logger.debug("Getting status for transaction: {}", transactionId);
        
        try {
            IcebergTransactionService.TransactionStatus status = 
                transactionService.getTransactionStatus(transactionId);
            
            if (status == IcebergTransactionService.TransactionStatus.NOT_FOUND) {
                return ResponseEntity.notFound().build();
            }
            
            IcebergTransactionService.TransactionContext context = 
                transactionService.getTransactionDetails(transactionId);
            
            TransactionStatusResponse response = new TransactionStatusResponse(
                transactionId,
                status.toString(),
                context != null ? context.getTableIdentifier().toString() : null,
                context != null ? context.getStartTime().toEpochMilli() : null,
                context != null ? (context.getEndTime() != null ? context.getEndTime().toEpochMilli() : null) : null,
                context != null ? context.getOperations().size() : 0
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to get transaction status: {}", transactionId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Get transaction details.
     */
    @GetMapping("/{transactionId}")
    public ResponseEntity<TransactionDetailsResponse> getTransactionDetails(@PathVariable String transactionId) {
        logger.debug("Getting details for transaction: {}", transactionId);
        
        try {
            IcebergTransactionService.TransactionContext context = 
                transactionService.getTransactionDetails(transactionId);
            
            if (context == null) {
                return ResponseEntity.notFound().build();
            }
            
            List<TransactionOperationResponse> operations = context.getOperations().stream()
                .map(op -> new TransactionOperationResponse(
                    op.getOperationType().toString(),
                    op.getRecordCount(),
                    op.getDescription(),
                    op.getTimestamp().toEpochMilli()
                ))
                .collect(Collectors.toList());
            
            TransactionDetailsResponse response = new TransactionDetailsResponse(
                context.getTransactionId(),
                context.getStatus().toString(),
                context.getTableIdentifier().toString(),
                context.getStartTime().toEpochMilli(),
                context.getEndTime() != null ? context.getEndTime().toEpochMilli() : null,
                operations
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to get transaction details: {}", transactionId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * List all active transactions.
     */
    @GetMapping("/active")
    public ResponseEntity<List<TransactionSummaryResponse>> getActiveTransactions() {
        logger.debug("Getting all active transactions");
        
        try {
            List<IcebergTransactionService.TransactionContext> activeTransactions = 
                transactionService.getActiveTransactions();
            
            List<TransactionSummaryResponse> response = activeTransactions.stream()
                .map(context -> new TransactionSummaryResponse(
                    context.getTransactionId(),
                    context.getStatus().toString(),
                    context.getTableIdentifier().toString(),
                    context.getStartTime().toEpochMilli(),
                    context.getOperations().size()
                ))
                .collect(Collectors.toList());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to get active transactions", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Clean up expired transactions.
     */
    @PostMapping("/cleanup")
    public ResponseEntity<CleanupResponse> cleanupExpiredTransactions(
            @RequestParam(defaultValue = "60") long maxAgeMinutes) {
        logger.info("Cleaning up transactions older than {} minutes", maxAgeMinutes);
        
        try {
            int cleanedUp = transactionService.cleanupExpiredTransactions(maxAgeMinutes);
            
            CleanupResponse response = new CleanupResponse(
                cleanedUp,
                "Cleaned up " + cleanedUp + " expired transactions"
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to cleanup expired transactions", e);
            CleanupResponse errorResponse = new CleanupResponse(
                0,
                "Failed to cleanup transactions: " + e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
    
    // Request/Response DTOs
    
    public static class BeginTransactionRequest {
        private String namespaceName;
        private String tableName;
        
        // Getters and setters
        public String getNamespaceName() { return namespaceName; }
        public void setNamespaceName(String namespaceName) { this.namespaceName = namespaceName; }
        
        public String getTableName() { return tableName; }
        public void setTableName(String tableName) { this.tableName = tableName; }
    }
    
    public static class TransactionResponse {
        private final String transactionId;
        private final String tableIdentifier;
        private final String status;
        private final Long startTime;
        private final Long endTime;
        private final int operationCount;
        private final String message;
        private final String error;
        
        public TransactionResponse(String transactionId, String tableIdentifier, String status,
                                 Long startTime, Long endTime, int operationCount, String message, String error) {
            this.transactionId = transactionId;
            this.tableIdentifier = tableIdentifier;
            this.status = status;
            this.startTime = startTime;
            this.endTime = endTime;
            this.operationCount = operationCount;
            this.message = message;
            this.error = error;
        }
        
        // Getters
        public String getTransactionId() { return transactionId; }
        public String getTableIdentifier() { return tableIdentifier; }
        public String getStatus() { return status; }
        public Long getStartTime() { return startTime; }
        public Long getEndTime() { return endTime; }
        public int getOperationCount() { return operationCount; }
        public String getMessage() { return message; }
        public String getError() { return error; }
    }
    
    public static class TransactionStatusResponse {
        private final String transactionId;
        private final String status;
        private final String tableIdentifier;
        private final Long startTime;
        private final Long endTime;
        private final int operationCount;
        
        public TransactionStatusResponse(String transactionId, String status, String tableIdentifier,
                                       Long startTime, Long endTime, int operationCount) {
            this.transactionId = transactionId;
            this.status = status;
            this.tableIdentifier = tableIdentifier;
            this.startTime = startTime;
            this.endTime = endTime;
            this.operationCount = operationCount;
        }
        
        // Getters
        public String getTransactionId() { return transactionId; }
        public String getStatus() { return status; }
        public String getTableIdentifier() { return tableIdentifier; }
        public Long getStartTime() { return startTime; }
        public Long getEndTime() { return endTime; }
        public int getOperationCount() { return operationCount; }
    }
    
    public static class TransactionDetailsResponse {
        private final String transactionId;
        private final String status;
        private final String tableIdentifier;
        private final Long startTime;
        private final Long endTime;
        private final List<TransactionOperationResponse> operations;
        
        public TransactionDetailsResponse(String transactionId, String status, String tableIdentifier,
                                        Long startTime, Long endTime, List<TransactionOperationResponse> operations) {
            this.transactionId = transactionId;
            this.status = status;
            this.tableIdentifier = tableIdentifier;
            this.startTime = startTime;
            this.endTime = endTime;
            this.operations = operations;
        }
        
        // Getters
        public String getTransactionId() { return transactionId; }
        public String getStatus() { return status; }
        public String getTableIdentifier() { return tableIdentifier; }
        public Long getStartTime() { return startTime; }
        public Long getEndTime() { return endTime; }
        public List<TransactionOperationResponse> getOperations() { return operations; }
    }
    
    public static class TransactionOperationResponse {
        private final String operationType;
        private final long recordCount;
        private final String description;
        private final long timestamp;
        
        public TransactionOperationResponse(String operationType, long recordCount, String description, long timestamp) {
            this.operationType = operationType;
            this.recordCount = recordCount;
            this.description = description;
            this.timestamp = timestamp;
        }
        
        // Getters
        public String getOperationType() { return operationType; }
        public long getRecordCount() { return recordCount; }
        public String getDescription() { return description; }
        public long getTimestamp() { return timestamp; }
    }
    
    public static class TransactionSummaryResponse {
        private final String transactionId;
        private final String status;
        private final String tableIdentifier;
        private final long startTime;
        private final int operationCount;
        
        public TransactionSummaryResponse(String transactionId, String status, String tableIdentifier,
                                        long startTime, int operationCount) {
            this.transactionId = transactionId;
            this.status = status;
            this.tableIdentifier = tableIdentifier;
            this.startTime = startTime;
            this.operationCount = operationCount;
        }
        
        // Getters
        public String getTransactionId() { return transactionId; }
        public String getStatus() { return status; }
        public String getTableIdentifier() { return tableIdentifier; }
        public long getStartTime() { return startTime; }
        public int getOperationCount() { return operationCount; }
    }
    
    public static class CleanupResponse {
        private final int cleanedUpCount;
        private final String message;
        
        public CleanupResponse(int cleanedUpCount, String message) {
            this.cleanedUpCount = cleanedUpCount;
            this.message = message;
        }
        
        // Getters
        public int getCleanedUpCount() { return cleanedUpCount; }
        public String getMessage() { return message; }
    }
}