package com.minicloud.controlplane.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics service for Iceberg catalog operations.
 * Tracks table creation, deletion, schema evolution, and catalog performance.
 */
@Service
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class IcebergCatalogMetricsService {
    
    private static final Logger logger = LoggerFactory.getLogger(IcebergCatalogMetricsService.class);
    
    private final MeterRegistry meterRegistry;
    
    // Micrometer metrics
    private Counter tablesCreatedTotal;
    private Counter tablesDroppedTotal;
    private Counter schemaEvolutionsTotal;
    private Counter catalogOperationsTotal;
    private Timer tableCreationTime;
    private Timer schemaEvolutionTime;
    private Timer catalogLookupTime;
    private Counter catalogCacheHits;
    private Counter catalogCacheMisses;
    
    // Internal tracking
    private final AtomicLong activeTables = new AtomicLong(0);
    private final ConcurrentHashMap<String, Instant> tableCreationTimes = new ConcurrentHashMap<>();
    
    @Autowired
    public IcebergCatalogMetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }
    
    @PostConstruct
    public void initialize() {
        logger.info("Initializing Iceberg catalog metrics service");
        
        // Table lifecycle metrics
        tablesCreatedTotal = Counter.builder("iceberg.catalog.tables.created.total")
            .description("Total number of Iceberg tables created")
            .register(meterRegistry);
        
        tablesDroppedTotal = Counter.builder("iceberg.catalog.tables.dropped.total")
            .description("Total number of Iceberg tables dropped")
            .register(meterRegistry);
        
        schemaEvolutionsTotal = Counter.builder("iceberg.catalog.schema.evolutions.total")
            .description("Total number of schema evolution operations")
            .register(meterRegistry);
        
        catalogOperationsTotal = Counter.builder("iceberg.catalog.operations.total")
            .description("Total number of catalog operations")
            .tag("operation", "all")
            .register(meterRegistry);
        
        // Timing metrics
        tableCreationTime = Timer.builder("iceberg.catalog.table.creation.time")
            .description("Time taken to create Iceberg tables")
            .register(meterRegistry);
        
        schemaEvolutionTime = Timer.builder("iceberg.catalog.schema.evolution.time")
            .description("Time taken for schema evolution operations")
            .register(meterRegistry);
        
        catalogLookupTime = Timer.builder("iceberg.catalog.lookup.time")
            .description("Time taken for catalog lookup operations")
            .register(meterRegistry);
        
        // Cache metrics
        catalogCacheHits = Counter.builder("iceberg.catalog.cache.hits.total")
            .description("Total number of catalog cache hits")
            .register(meterRegistry);
        
        catalogCacheMisses = Counter.builder("iceberg.catalog.cache.misses.total")
            .description("Total number of catalog cache misses")
            .register(meterRegistry);
        
        // Gauge metrics
        meterRegistry.gauge("iceberg.catalog.tables.active", activeTables, AtomicLong::doubleValue);
        
        logger.info("Iceberg catalog metrics service initialized successfully");
    }
    
    /**
     * Records the start of a table creation operation.
     */
    public void recordTableCreationStart(TableIdentifier tableId) {
        String tableKey = tableId.toString();
        tableCreationTimes.put(tableKey, Instant.now());
        
        logger.debug("Recording table creation start for: {}", tableId);
    }
    
    /**
     * Records the completion of a table creation operation.
     */
    public void recordTableCreationComplete(TableIdentifier tableId, boolean success) {
        String tableKey = tableId.toString();
        Instant startTime = tableCreationTimes.remove(tableKey);
        
        if (startTime != null) {
            Duration creationTime = Duration.between(startTime, Instant.now());
            tableCreationTime.record(creationTime);
            
            logger.info("Table creation completed for {}: {} ms, success: {}", 
                tableId, creationTime.toMillis(), success);
        }
        
        if (success) {
            tablesCreatedTotal.increment();
            activeTables.incrementAndGet();
        }
        
        catalogOperationsTotal.increment();
    }
    
    /**
     * Records a table drop operation.
     */
    public void recordTableDrop(TableIdentifier tableId, boolean success) {
        logger.info("Recording table drop for: {}, success: {}", tableId, success);
        
        if (success) {
            tablesDroppedTotal.increment();
            activeTables.decrementAndGet();
        }
        
        catalogOperationsTotal.increment();
    }
    
    /**
     * Records the start of a schema evolution operation.
     */
    public void recordSchemaEvolutionStart(TableIdentifier tableId, String evolutionType) {
        String operationKey = tableId + "_" + evolutionType;
        tableCreationTimes.put(operationKey, Instant.now());
        
        logger.debug("Recording schema evolution start for: {} ({})", tableId, evolutionType);
    }
    
    /**
     * Records the completion of a schema evolution operation.
     */
    public void recordSchemaEvolutionComplete(TableIdentifier tableId, String evolutionType, boolean success) {
        String operationKey = tableId + "_" + evolutionType;
        Instant startTime = tableCreationTimes.remove(operationKey);
        
        if (startTime != null) {
            Duration evolutionTime = Duration.between(startTime, Instant.now());
            schemaEvolutionTime.record(evolutionTime);
            
            logger.info("Schema evolution completed for {} ({}): {} ms, success: {}", 
                tableId, evolutionType, evolutionTime.toMillis(), success);
        }
        
        if (success) {
            schemaEvolutionsTotal.increment();
        }
        
        catalogOperationsTotal.increment();
    }
    
    /**
     * Records a catalog lookup operation.
     */
    public void recordCatalogLookup(TableIdentifier tableId, Duration lookupTime, boolean found) {
        catalogLookupTime.record(lookupTime);
        catalogOperationsTotal.increment();
        
        logger.debug("Catalog lookup for {}: {} ms, found: {}", 
            tableId, lookupTime.toMillis(), found);
    }
    
    /**
     * Records a catalog cache hit.
     */
    public void recordCacheHit(String cacheKey) {
        catalogCacheHits.increment();
        logger.debug("Catalog cache hit for: {}", cacheKey);
    }
    
    /**
     * Records a catalog cache miss.
     */
    public void recordCacheMiss(String cacheKey) {
        catalogCacheMisses.increment();
        logger.debug("Catalog cache miss for: {}", cacheKey);
    }
    
    /**
     * Gets catalog performance metrics.
     */
    public CatalogPerformanceMetrics getCatalogPerformanceMetrics() {
        long totalTablesCreated = (long) tablesCreatedTotal.count();
        long totalTablesDropped = (long) tablesDroppedTotal.count();
        long totalSchemaEvolutions = (long) schemaEvolutionsTotal.count();
        long totalCatalogOperations = (long) catalogOperationsTotal.count();
        
        double averageTableCreationTime = tableCreationTime.mean(java.util.concurrent.TimeUnit.MILLISECONDS);
        double averageSchemaEvolutionTime = schemaEvolutionTime.mean(java.util.concurrent.TimeUnit.MILLISECONDS);
        double averageCatalogLookupTime = catalogLookupTime.mean(java.util.concurrent.TimeUnit.MILLISECONDS);
        
        long totalCacheHits = (long) catalogCacheHits.count();
        long totalCacheMisses = (long) catalogCacheMisses.count();
        double cacheHitRate = (totalCacheHits + totalCacheMisses) > 0 ? 
            (double) totalCacheHits / (totalCacheHits + totalCacheMisses) : 0.0;
        
        return new CatalogPerformanceMetrics(
            totalTablesCreated,
            totalTablesDropped,
            totalSchemaEvolutions,
            totalCatalogOperations,
            activeTables.get(),
            averageTableCreationTime,
            averageSchemaEvolutionTime,
            averageCatalogLookupTime,
            cacheHitRate,
            totalCacheHits,
            totalCacheMisses
        );
    }
    
    /**
     * Data class for catalog performance metrics.
     */
    public static class CatalogPerformanceMetrics {
        private final long totalTablesCreated;
        private final long totalTablesDropped;
        private final long totalSchemaEvolutions;
        private final long totalCatalogOperations;
        private final long activeTables;
        private final double averageTableCreationTimeMs;
        private final double averageSchemaEvolutionTimeMs;
        private final double averageCatalogLookupTimeMs;
        private final double cacheHitRate;
        private final long totalCacheHits;
        private final long totalCacheMisses;
        
        public CatalogPerformanceMetrics(long totalTablesCreated, long totalTablesDropped,
                                       long totalSchemaEvolutions, long totalCatalogOperations,
                                       long activeTables, double averageTableCreationTimeMs,
                                       double averageSchemaEvolutionTimeMs, double averageCatalogLookupTimeMs,
                                       double cacheHitRate, long totalCacheHits, long totalCacheMisses) {
            this.totalTablesCreated = totalTablesCreated;
            this.totalTablesDropped = totalTablesDropped;
            this.totalSchemaEvolutions = totalSchemaEvolutions;
            this.totalCatalogOperations = totalCatalogOperations;
            this.activeTables = activeTables;
            this.averageTableCreationTimeMs = averageTableCreationTimeMs;
            this.averageSchemaEvolutionTimeMs = averageSchemaEvolutionTimeMs;
            this.averageCatalogLookupTimeMs = averageCatalogLookupTimeMs;
            this.cacheHitRate = cacheHitRate;
            this.totalCacheHits = totalCacheHits;
            this.totalCacheMisses = totalCacheMisses;
        }
        
        // Getters
        public long getTotalTablesCreated() { return totalTablesCreated; }
        public long getTotalTablesDropped() { return totalTablesDropped; }
        public long getTotalSchemaEvolutions() { return totalSchemaEvolutions; }
        public long getTotalCatalogOperations() { return totalCatalogOperations; }
        public long getActiveTables() { return activeTables; }
        public double getAverageTableCreationTimeMs() { return averageTableCreationTimeMs; }
        public double getAverageSchemaEvolutionTimeMs() { return averageSchemaEvolutionTimeMs; }
        public double getAverageCatalogLookupTimeMs() { return averageCatalogLookupTimeMs; }
        public double getCacheHitRate() { return cacheHitRate; }
        public long getTotalCacheHits() { return totalCacheHits; }
        public long getTotalCacheMisses() { return totalCacheMisses; }
    }
}