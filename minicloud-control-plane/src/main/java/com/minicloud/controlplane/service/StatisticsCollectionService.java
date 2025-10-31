package com.minicloud.controlplane.service;

import com.minicloud.controlplane.config.IcebergConfiguration;
import com.minicloud.controlplane.model.TableStatistics;
import com.minicloud.controlplane.model.PartitionStatistics;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Statistics Collection Service for automatic table and column statistics collection.
 * Provides statistics-based query optimization hooks and partition statistics management.
 */
@Service
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class StatisticsCollectionService {
    
    private static final Logger logger = LoggerFactory.getLogger(StatisticsCollectionService.class);
    
    private final IcebergConfiguration icebergConfig;
    private final GlobalCatalogService globalCatalogService;
    private final MetadataCacheManager cacheManager;
    
    // Statistics collection executor
    private ScheduledExecutorService statisticsExecutor;
    private ExecutorService asyncExecutor;
    
    // Statistics storage (in production, this would be persisted)
    private final Map<String, TableStatistics> tableStatisticsStore = new ConcurrentHashMap<>();
    private final Map<String, List<PartitionStatistics>> partitionStatisticsStore = new ConcurrentHashMap<>();
    
    // Collection tracking
    private final Map<String, Instant> lastCollectionTime = new ConcurrentHashMap<>();
    private final Set<String> collectingTables = ConcurrentHashMap.newKeySet();
    
    @Autowired
    public StatisticsCollectionService(
            IcebergConfiguration icebergConfig,
            GlobalCatalogService globalCatalogService,
            MetadataCacheManager cacheManager) {
        this.icebergConfig = icebergConfig;
        this.globalCatalogService = globalCatalogService;
        this.cacheManager = cacheManager;
    }
    
    @PostConstruct
    public void initialize() {
        if (!icebergConfig.isEnabled()) {
            logger.info("Statistics collection is disabled (Iceberg not enabled)");
            return;
        }
        
        logger.info("Initializing statistics collection service");
        
        try {
            // Initialize executors
            this.statisticsExecutor = Executors.newScheduledThreadPool(2);
            this.asyncExecutor = Executors.newFixedThreadPool(4);
            
            // Schedule periodic statistics collection
            schedulePeriodicCollection();
            
            logger.info("Statistics collection service initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize statistics collection service", e);
            throw new RuntimeException("Statistics collection service initialization failed", e);
        }
    }
    
    private void schedulePeriodicCollection() {
        // Schedule statistics collection every 30 minutes
        statisticsExecutor.scheduleAtFixedRate(
            this::collectAllTableStatistics,
            5, // Initial delay
            30, // Period
            TimeUnit.MINUTES
        );
        
        // Schedule partition statistics collection every hour
        statisticsExecutor.scheduleAtFixedRate(
            this::collectAllPartitionStatistics,
            10, // Initial delay
            60, // Period
            TimeUnit.MINUTES
        );
        
        logger.info("Scheduled periodic statistics collection");
    }
    
    // Table Statistics Collection
    
    /**
     * Collects comprehensive statistics for a specific table.
     */
    public CompletableFuture<TableStatistics> collectTableStatistics(TableIdentifier identifier) {
        return CompletableFuture.supplyAsync(() -> {
            String tableKey = tableKey(identifier);
            
            if (collectingTables.contains(tableKey)) {
                logger.debug("Statistics collection already in progress for table: {}", identifier);
                return getTableStatistics(identifier).orElse(null);
            }
            
            try {
                collectingTables.add(tableKey);
                logger.info("Collecting statistics for table: {}", identifier);
                
                Table table = globalCatalogService.loadTable(identifier);
                TableStatistics statistics = analyzeTable(identifier, table);
                
                // Store statistics
                tableStatisticsStore.put(tableKey, statistics);
                lastCollectionTime.put(tableKey, Instant.now());
                
                // Cache statistics
                cacheManager.cacheStatistics(identifier, statistics);
                
                logger.info("Successfully collected statistics for table: {} (rows: {}, size: {} bytes)", 
                    identifier, statistics.getTotalRows(), statistics.getTotalSize());
                
                return statistics;
                
            } catch (Exception e) {
                logger.error("Failed to collect statistics for table: {}", identifier, e);
                throw new RuntimeException("Statistics collection failed for table: " + identifier, e);
            } finally {
                collectingTables.remove(tableKey);
            }
        }, asyncExecutor);
    }
    
    /**
     * Gets cached table statistics or triggers collection if not available.
     */
    public Optional<TableStatistics> getTableStatistics(TableIdentifier identifier) {
        String tableKey = tableKey(identifier);
        
        // Check cache first
        Optional<TableStatistics> cached = cacheManager.getCachedStatistics(identifier);
        if (cached.isPresent()) {
            return cached;
        }
        
        // Check local store
        TableStatistics stored = tableStatisticsStore.get(tableKey);
        if (stored != null) {
            // Cache for future use
            cacheManager.cacheStatistics(identifier, stored);
            return Optional.of(stored);
        }
        
        // Trigger async collection if not in progress
        if (!collectingTables.contains(tableKey)) {
            collectTableStatistics(identifier);
        }
        
        return Optional.empty();
    }
    
    private TableStatistics analyzeTable(TableIdentifier identifier, Table table) {
        TableStatistics statistics = new TableStatistics(
            identifier.name(), 
            identifier.namespace().toString()
        );
        
        try {
            // Get current snapshot
            Snapshot currentSnapshot = table.currentSnapshot();
            if (currentSnapshot == null) {
                logger.warn("No current snapshot for table: {}", identifier);
                return statistics;
            }
            
            // Analyze data files
            TableScan scan = table.newScan();
            long totalRows = 0;
            long totalSize = 0;
            long totalFiles = 0;
            
            try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
                for (FileScanTask task : tasks) {
                    DataFile file = task.file();
                    totalRows += file.recordCount();
                    totalSize += file.fileSizeInBytes();
                    totalFiles++;
                }
            }
            
            statistics.setTotalRows(totalRows);
            statistics.setTotalSize(totalSize);
            statistics.setTotalFiles(totalFiles);
            
            // Analyze schema and collect column statistics
            Schema schema = table.schema();
            Map<String, TableStatistics.ColumnStatistics> columnStats = analyzeColumns(schema, table);
            statistics.setColumnStatistics(columnStats);
            
            // Count partitions
            long partitionCount = countPartitions(table);
            statistics.setTotalPartitions(partitionCount);
            
            // Set properties
            Map<String, Object> properties = new HashMap<>();
            properties.put("snapshot_id", currentSnapshot.snapshotId());
            properties.put("schema_id", schema.schemaId());
            properties.put("collection_time", Instant.now().toString());
            statistics.setProperties(properties);
            
        } catch (Exception e) {
            logger.error("Error analyzing table: {}", identifier, e);
            throw new RuntimeException("Table analysis failed", e);
        }
        
        return statistics;
    }
    
    private Map<String, TableStatistics.ColumnStatistics> analyzeColumns(Schema schema, Table table) {
        Map<String, TableStatistics.ColumnStatistics> columnStats = new HashMap<>();
        
        for (Types.NestedField field : schema.columns()) {
            TableStatistics.ColumnStatistics colStats = new TableStatistics.ColumnStatistics(
                field.name(),
                field.type().toString()
            );
            
            // For now, we'll set basic statistics
            // In a full implementation, this would scan data files to compute actual statistics
            colStats.setNullCount(0); // Would be computed from data
            colStats.setDistinctCount(-1); // Would be computed from data
            
            columnStats.put(field.name(), colStats);
        }
        
        return columnStats;
    }
    
    private long countPartitions(Table table) {
        try {
            // Count unique partition values
            Set<String> partitions = new HashSet<>();
            
            TableScan scan = table.newScan();
            try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
                for (FileScanTask task : tasks) {
                    // Get partition data from file path or metadata
                    String partitionPath = task.file().path().toString();
                    // Extract partition information (simplified)
                    partitions.add(partitionPath);
                }
            }
            
            return partitions.size();
        } catch (Exception e) {
            logger.warn("Could not count partitions for table", e);
            return 0;
        }
    }
    
    // Partition Statistics Collection
    
    /**
     * Collects statistics for all partitions of a table.
     */
    public CompletableFuture<List<PartitionStatistics>> collectPartitionStatistics(TableIdentifier identifier) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                logger.info("Collecting partition statistics for table: {}", identifier);
                
                Table table = globalCatalogService.loadTable(identifier);
                List<PartitionStatistics> partitionStats = analyzePartitions(identifier, table);
                
                // Store partition statistics
                String tableKey = tableKey(identifier);
                partitionStatisticsStore.put(tableKey, partitionStats);
                
                // Cache partition statistics
                cacheManager.cachePartitionStatistics(identifier, partitionStats);
                
                logger.info("Successfully collected partition statistics for table: {} ({} partitions)", 
                    identifier, partitionStats.size());
                
                return partitionStats;
                
            } catch (Exception e) {
                logger.error("Failed to collect partition statistics for table: {}", identifier, e);
                throw new RuntimeException("Partition statistics collection failed for table: " + identifier, e);
            }
        }, asyncExecutor);
    }
    
    /**
     * Gets cached partition statistics.
     */
    public Optional<List<PartitionStatistics>> getPartitionStatistics(TableIdentifier identifier) {
        // Check cache first
        Optional<List<PartitionStatistics>> cached = cacheManager.getCachedPartitionStatistics(identifier);
        if (cached.isPresent()) {
            return cached;
        }
        
        // Check local store
        String tableKey = tableKey(identifier);
        List<PartitionStatistics> stored = partitionStatisticsStore.get(tableKey);
        if (stored != null) {
            cacheManager.cachePartitionStatistics(identifier, stored);
            return Optional.of(stored);
        }
        
        return Optional.empty();
    }
    
    private List<PartitionStatistics> analyzePartitions(TableIdentifier identifier, Table table) {
        List<PartitionStatistics> partitionStats = new ArrayList<>();
        
        try {
            // Group files by partition
            Map<String, List<DataFile>> partitionFiles = new HashMap<>();
            
            TableScan scan = table.newScan();
            try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
                for (FileScanTask task : tasks) {
                    DataFile file = task.file();
                    String partitionPath = extractPartitionPath(file);
                    
                    partitionFiles.computeIfAbsent(partitionPath, k -> new ArrayList<>()).add(file);
                }
            }
            
            // Analyze each partition
            for (Map.Entry<String, List<DataFile>> entry : partitionFiles.entrySet()) {
                String partitionPath = entry.getKey();
                List<DataFile> files = entry.getValue();
                
                PartitionStatistics partStats = new PartitionStatistics(
                    identifier.name(),
                    identifier.namespace().toString(),
                    partitionPath
                );
                
                // Aggregate file statistics
                long totalRows = files.stream().mapToLong(DataFile::recordCount).sum();
                long totalSize = files.stream().mapToLong(DataFile::fileSizeInBytes).sum();
                
                partStats.setRowCount(totalRows);
                partStats.setFileCount(files.size());
                partStats.setTotalSize(totalSize);
                
                // Extract partition values (simplified)
                Map<String, String> partitionValues = extractPartitionValues(partitionPath);
                partStats.setPartitionValues(partitionValues);
                
                partitionStats.add(partStats);
            }
            
        } catch (Exception e) {
            logger.error("Error analyzing partitions for table: {}", identifier, e);
            throw new RuntimeException("Partition analysis failed", e);
        }
        
        return partitionStats;
    }
    
    private String extractPartitionPath(DataFile file) {
        // Extract partition path from file path
        String path = file.path().toString();
        // Simplified extraction - in reality, this would parse the actual partition structure
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash > 0) {
            int secondLastSlash = path.lastIndexOf('/', lastSlash - 1);
            if (secondLastSlash > 0) {
                return path.substring(secondLastSlash + 1, lastSlash);
            }
        }
        return "default_partition";
    }
    
    private Map<String, String> extractPartitionValues(String partitionPath) {
        Map<String, String> values = new HashMap<>();
        
        // Parse partition path like "year=2024/month=01"
        String[] parts = partitionPath.split("/");
        for (String part : parts) {
            if (part.contains("=")) {
                String[] keyValue = part.split("=", 2);
                if (keyValue.length == 2) {
                    values.put(keyValue[0], keyValue[1]);
                }
            }
        }
        
        return values;
    }
    
    // Batch Collection Operations
    
    /**
     * Collects statistics for all tables in all namespaces.
     */
    public void collectAllTableStatistics() {
        try {
            logger.info("Starting batch collection of table statistics");
            
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            
            // Get all namespaces
            List<org.apache.iceberg.catalog.Namespace> namespaces = globalCatalogService.listNamespaces();
            
            for (org.apache.iceberg.catalog.Namespace namespace : namespaces) {
                // Get all tables in namespace
                List<TableIdentifier> tables = globalCatalogService.listTables(namespace);
                
                for (TableIdentifier table : tables) {
                    // Skip if recently collected
                    String tableKey = tableKey(table);
                    Instant lastCollection = lastCollectionTime.get(tableKey);
                    if (lastCollection != null && 
                        lastCollection.isAfter(Instant.now().minusSeconds(1800))) { // 30 minutes
                        continue;
                    }
                    
                    CompletableFuture<Void> future = collectTableStatistics(table)
                        .thenAccept(stats -> {
                            if (stats != null) {
                                logger.debug("Collected statistics for table: {}", table);
                            }
                        })
                        .exceptionally(throwable -> {
                            logger.warn("Failed to collect statistics for table: {}", table, throwable);
                            return null;
                        });
                    
                    futures.add(future);
                }
            }
            
            // Wait for all collections to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(10, TimeUnit.MINUTES);
            
            logger.info("Completed batch collection of table statistics ({} tables)", futures.size());
            
        } catch (Exception e) {
            logger.error("Error during batch statistics collection", e);
        }
    }
    
    /**
     * Collects partition statistics for all tables.
     */
    public void collectAllPartitionStatistics() {
        try {
            logger.info("Starting batch collection of partition statistics");
            
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            
            // Get all namespaces
            List<org.apache.iceberg.catalog.Namespace> namespaces = globalCatalogService.listNamespaces();
            
            for (org.apache.iceberg.catalog.Namespace namespace : namespaces) {
                List<TableIdentifier> tables = globalCatalogService.listTables(namespace);
                
                for (TableIdentifier table : tables) {
                    CompletableFuture<Void> future = collectPartitionStatistics(table)
                        .thenAccept(partitionStats -> {
                            if (partitionStats != null) {
                                logger.debug("Collected partition statistics for table: {} ({} partitions)", 
                                    table, partitionStats.size());
                            }
                        })
                        .exceptionally(throwable -> {
                            logger.warn("Failed to collect partition statistics for table: {}", table, throwable);
                            return null;
                        });
                    
                    futures.add(future);
                }
            }
            
            // Wait for all collections to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(15, TimeUnit.MINUTES);
            
            logger.info("Completed batch collection of partition statistics ({} tables)", futures.size());
            
        } catch (Exception e) {
            logger.error("Error during batch partition statistics collection", e);
        }
    }
    
    // Query Optimization Hooks
    
    /**
     * Provides statistics-based query optimization recommendations.
     */
    public QueryOptimizationHints getOptimizationHints(TableIdentifier identifier) {
        Optional<TableStatistics> tableStats = getTableStatistics(identifier);
        Optional<List<PartitionStatistics>> partitionStats = getPartitionStatistics(identifier);
        
        return new QueryOptimizationHints(tableStats.orElse(null), partitionStats.orElse(null));
    }
    
    // Utility Methods
    
    private String tableKey(TableIdentifier identifier) {
        return identifier.namespace().toString() + "." + identifier.name();
    }
    
    /**
     * Invalidates statistics for a table (called when table is modified).
     */
    public void invalidateStatistics(TableIdentifier identifier) {
        String tableKey = tableKey(identifier);
        
        logger.info("Invalidating statistics for table: {}", identifier);
        
        tableStatisticsStore.remove(tableKey);
        partitionStatisticsStore.remove(tableKey);
        lastCollectionTime.remove(tableKey);
        
        // Invalidate cache
        cacheManager.invalidateTable(identifier);
    }
    
    @PreDestroy
    public void cleanup() {
        if (statisticsExecutor != null) {
            statisticsExecutor.shutdown();
            try {
                if (!statisticsExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    statisticsExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                statisticsExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        if (asyncExecutor != null) {
            asyncExecutor.shutdown();
            try {
                if (!asyncExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    asyncExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                asyncExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        logger.info("Statistics collection service cleaned up successfully");
    }
    
    /**
     * Query optimization hints based on collected statistics.
     */
    public static class QueryOptimizationHints {
        private final TableStatistics tableStatistics;
        private final List<PartitionStatistics> partitionStatistics;
        
        public QueryOptimizationHints(TableStatistics tableStatistics, List<PartitionStatistics> partitionStatistics) {
            this.tableStatistics = tableStatistics;
            this.partitionStatistics = partitionStatistics;
        }
        
        public TableStatistics getTableStatistics() {
            return tableStatistics;
        }
        
        public List<PartitionStatistics> getPartitionStatistics() {
            return partitionStatistics;
        }
        
        public boolean hasStatistics() {
            return tableStatistics != null;
        }
        
        public boolean hasPartitionStatistics() {
            return partitionStatistics != null && !partitionStatistics.isEmpty();
        }
        
        public long getEstimatedRowCount() {
            return tableStatistics != null ? tableStatistics.getTotalRows() : -1;
        }
        
        public long getEstimatedSize() {
            return tableStatistics != null ? tableStatistics.getTotalSize() : -1;
        }
        
        public List<String> getPartitionColumns() {
            if (partitionStatistics == null || partitionStatistics.isEmpty()) {
                return Collections.emptyList();
            }
            
            return partitionStatistics.get(0).getPartitionValues().keySet()
                .stream().collect(Collectors.toList());
        }
    }
}