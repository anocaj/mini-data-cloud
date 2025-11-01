package com.minicloud.controlplane.controller;

import com.minicloud.controlplane.service.DataLoadingService;
import com.minicloud.controlplane.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;

/**
 * REST controller for data loading operations
 */
@RestController
@RequestMapping("/api/v1/data")
@CrossOrigin(origins = "*") // For development - should be restricted in production
public class DataLoadingController {
    
    private static final Logger logger = LoggerFactory.getLogger(DataLoadingController.class);
    
    @Autowired
    private DataLoadingService dataLoadingService;
    
    /**
     * Upload and load CSV file
     */
    @PostMapping(value = "/upload", consumes = "multipart/form-data")
    public ResponseEntity<UploadResponse> uploadCsvFile(@RequestParam("file") MultipartFile file) {
        logger.info("Received file upload request: {}", file.getOriginalFilename());
        
        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body(new UploadResponse(
                false, null, "File is empty", null
            ));
        }
        
        // Validate file type
        String originalFilename = file.getOriginalFilename();
        if (originalFilename == null || !originalFilename.toLowerCase().endsWith(".csv")) {
            return ResponseEntity.badRequest().body(new UploadResponse(
                false, null, "Only CSV files are allowed", null
            ));
        }
        
        try {
            // Create temp directory if it doesn't exist
            Path tempDir = Paths.get("./temp");
            if (!Files.exists(tempDir)) {
                Files.createDirectories(tempDir);
            }
            
            // Save uploaded file to temp directory
            String filename = System.currentTimeMillis() + "_" + originalFilename;
            Path tempFile = tempDir.resolve(filename);
            Files.copy(file.getInputStream(), tempFile, StandardCopyOption.REPLACE_EXISTING);
            
            // Extract table name from filename (remove extension)
            String tableName = originalFilename.substring(0, originalFilename.lastIndexOf('.'));
            // Clean table name (replace spaces and special chars with underscores)
            tableName = tableName.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
            
            // Load the CSV data
            DataLoadingService.LoadResult result = dataLoadingService.loadCsvData(
                tempFile.toString(),
                "default", // Use default namespace
                tableName,
                true // Assume header is present
            );
            
            // Clean up temp file
            Files.deleteIfExists(tempFile);
            
            UploadResponse response = new UploadResponse(
                true,
                result.getTableName(),
                "File uploaded and processed successfully",
                null
            );
            
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
            
        } catch (IOException e) {
            logger.error("Error handling file upload", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new UploadResponse(
                false, null, "Error processing file: " + e.getMessage(), null
            ));
        } catch (Exception e) {
            logger.error("Error loading CSV data from uploaded file", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new UploadResponse(
                false, null, "Error loading data: " + e.getMessage(), null
            ));
        }
    }

    /**
     * Load CSV data into a table (automatically chooses Iceberg or Parquet based on configuration)
     */
    @PostMapping("/load/csv")
    public ResponseEntity<LoadDataResponse> loadCsvData(@RequestBody LoadCsvRequest request) {
        logger.info("Received CSV data loading request for table: {}", request.getTableName());
        
        try {
            DataLoadingService.LoadResult result = dataLoadingService.loadCsvData(
                request.getCsvFilePath(),
                request.getNamespaceName(),
                request.getTableName(),
                request.isHasHeader()
            );
            
            LoadDataResponse response = new LoadDataResponse(
                result.getTableName(),
                result.getRowCount(),
                result.getFileSizeBytes(),
                result.getDurationMs(),
                "SUCCESS",
                null
            );
            
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        } catch (Exception e) {
            logger.error("Error loading CSV data", e);
            LoadDataResponse errorResponse = new LoadDataResponse(
                request.getTableName(),
                0L,
                0L,
                0L,
                "FAILED",
                e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
    
    /**
     * Load the sample bank transactions data
     */
    @PostMapping("/load/sample/bank-transactions")
    public ResponseEntity<LoadDataResponse> loadSampleBankTransactions() {
        logger.info("Loading sample bank transactions data");
        
        try {
            DataLoadingService.LoadResult result = dataLoadingService.loadSampleBankTransactions();
            
            LoadDataResponse response = new LoadDataResponse(
                result.getTableName(),
                result.getRowCount(),
                result.getFileSizeBytes(),
                result.getDurationMs(),
                "SUCCESS",
                null
            );
            
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        } catch (Exception e) {
            logger.error("Error loading sample bank transactions", e);
            LoadDataResponse errorResponse = new LoadDataResponse(
                "bank_transactions",
                0L,
                0L,
                0L,
                "FAILED",
                e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
    
    /**
     * List all loaded tables with their statistics
     */
    @GetMapping("/tables")
    public ResponseEntity<List<TableStatsResponse>> getLoadedTables() {
        logger.debug("Getting loaded tables statistics");
        
        try {
            List<TableStatsResponse> tables = dataLoadingService.getLoadedTablesStats();
            return ResponseEntity.ok(tables);
        } catch (Exception e) {
            logger.error("Error getting loaded tables", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Get statistics for a specific table
     */
    @GetMapping("/tables/{namespaceName}/{tableName}/stats")
    public ResponseEntity<TableStatsResponse> getTableStats(@PathVariable String namespaceName,
                                                           @PathVariable String tableName) {
        logger.debug("Getting statistics for table: {}.{}", namespaceName, tableName);
        
        try {
            TableStatsResponse stats = dataLoadingService.getTableStats(namespaceName, tableName);
            if (stats != null) {
                return ResponseEntity.ok(stats);
            } else {
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            logger.error("Error getting table statistics: {}.{}", namespaceName, tableName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Request DTO for loading CSV data
     */
    public static class LoadCsvRequest {
        private String csvFilePath;
        private String namespaceName = "default";
        private String tableName;
        private boolean hasHeader = true;
        
        // Getters and setters
        public String getCsvFilePath() { return csvFilePath; }
        public void setCsvFilePath(String csvFilePath) { this.csvFilePath = csvFilePath; }
        
        public String getNamespaceName() { return namespaceName; }
        public void setNamespaceName(String namespaceName) { this.namespaceName = namespaceName; }
        
        public String getTableName() { return tableName; }
        public void setTableName(String tableName) { this.tableName = tableName; }
        
        public boolean isHasHeader() { return hasHeader; }
        public void setHasHeader(boolean hasHeader) { this.hasHeader = hasHeader; }
    }
    
    /**
     * Response DTO for data loading operations
     */
    public static class LoadDataResponse {
        private final String tableName;
        private final long rowCount;
        private final long fileSizeBytes;
        private final long durationMs;
        private final String status;
        private final String errorMessage;
        
        public LoadDataResponse(String tableName, long rowCount, long fileSizeBytes, 
                              long durationMs, String status, String errorMessage) {
            this.tableName = tableName;
            this.rowCount = rowCount;
            this.fileSizeBytes = fileSizeBytes;
            this.durationMs = durationMs;
            this.status = status;
            this.errorMessage = errorMessage;
        }
        
        // Getters
        public String getTableName() { return tableName; }
        public long getRowCount() { return rowCount; }
        public long getFileSizeBytes() { return fileSizeBytes; }
        public long getDurationMs() { return durationMs; }
        public String getStatus() { return status; }
        public String getErrorMessage() { return errorMessage; }
    }
    
    /**
     * Response DTO for table statistics
     */
    public static class TableStatsResponse {
        private final String namespaceName;
        private final String tableName;
        private final long rowCount;
        private final long sizeBytes;
        private final int fileCount;
        private final int columnCount;
        private final String tableLocation;
        
        public TableStatsResponse(String namespaceName, String tableName, long rowCount, 
                                long sizeBytes, int fileCount, int columnCount, String tableLocation) {
            this.namespaceName = namespaceName;
            this.tableName = tableName;
            this.rowCount = rowCount;
            this.sizeBytes = sizeBytes;
            this.fileCount = fileCount;
            this.columnCount = columnCount;
            this.tableLocation = tableLocation;
        }
        
        // Getters
        public String getNamespaceName() { return namespaceName; }
        public String getTableName() { return tableName; }
        public long getRowCount() { return rowCount; }
        public long getSizeBytes() { return sizeBytes; }
        public int getFileCount() { return fileCount; }
        public int getColumnCount() { return columnCount; }
        public String getTableLocation() { return tableLocation; }
    }
    
    /**
     * Response DTO for file upload operations
     */
    public static class UploadResponse {
        private final boolean success;
        private final String tableName;
        private final String message;
        private final String error;
        
        public UploadResponse(boolean success, String tableName, String message, String error) {
            this.success = success;
            this.tableName = tableName;
            this.message = message;
            this.error = error;
        }
        
        // Getters
        public boolean isSuccess() { return success; }
        public String getTableName() { return tableName; }
        public String getMessage() { return message; }
        public String getError() { return error; }
    }
}