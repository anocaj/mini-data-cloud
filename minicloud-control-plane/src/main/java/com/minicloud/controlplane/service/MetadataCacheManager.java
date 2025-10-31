package com.minicloud.controlplane.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.minicloud.controlplane.config.IcebergConfiguration;
import com.minicloud.controlplane.model.TableStatistics;
import com.minicloud.controlplane.model.PartitionStatistics;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Multi-level metadata caching system for Iceberg tables.
 * Implements L1 (in-memory), L2 (local disk), and L3 (distributed) caching layers.
 */
@Service
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class MetadataCacheManager {
    
    private static final Logger logger = LoggerFactory.getLogger(MetadataCacheManager.class);
    
    private final IcebergConfiguration icebergConfig;
    
    // L1 Cache - In-memory with Caffeine
    private Cache<String, CachedTableMetadata> l1TableCache;
    private Cache<String, TableStatistics> l1StatsCache;
    private Cache<String, List<PartitionStatistics>> l1PartitionCache;
    
    // L2 Cache - Local disk simulation (using in-memory for now)
    private final Map<String, CachedTableMetadata> l2TableCache = new ConcurrentHashMap<>();
    private final Map<String, TableStatistics> l2StatsCache = new ConcurrentHashMap<>();
    private final Map<String, List<PartitionStatistics>> l2PartitionCache = new ConcurrentHashMap<>();
    
    // L3 Cache - Distributed cache coordination (simulated)
    private final Map<String, CacheEntry> l3CacheRegistry = new ConcurrentHashMap<>();
    
    // Cache invalidation tracking
    private final Map<String, Instant> invalidationTimestamps = new ConcurrentHashMap<>();
    
    // Background cleanup scheduler
    private ScheduledExecutorService cleanupScheduler;
    
    @Autowired
    public MetadataCacheManager(IcebergConfiguration icebergConfig) {
        this.icebergConfig = icebergConfig;
    }
    
    @PostConstruct
    public void initialize() {
        if (!icebergConfig.isEnabled()) {
            logger.info("Metadata caching is disabled (Iceberg not enabled)");
            return;
        }
        
        logger.info("Initializing multi-level metadata cache manager");
        
        try {
            initializeL1Cache();
            initializeL2Cache();
            initializeL3Cache();
            startCleanupScheduler();
            
            logger.info("Metadata cache manager initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize metadata cache manager", e);
            throw new RuntimeException("Metadata cache manager initialization failed", e);
        }
    }
    
    private void initializeL1Cache() {
        IcebergConfiguration.CacheConfig cacheConfig = icebergConfig.getCache();
        Duration l1Ttl = parseDuration(cacheConfig.getL1Ttl());
        
        logger.info("Initializing L1 cache with size: {} and TTL: {}", cacheConfig.getL1Size(), l1Ttl);
        
        RemovalListener<String, Object> removalListener = (key, value, cause) -> {
            logger.debug("L1 cache entry removed: {} (cause: {})", key, cause);
            // Promote to L2 cache if evicted due to size
            if (cause.wasEvicted()) {
                promoteToL2Cache(key, value);
            }
        };
        
        this.l1TableCache = Caffeine.newBuilder()
            .maximumSize(cacheConfig.getL1Size())
            .expireAfterWrite(l1Ttl)
            .removalListener(removalListener)
            .recordStats()
            .build();
            
        this.l1StatsCache = Caffeine.newBuilder()
            .maximumSize(cacheConfig.getL1Size())
            .expireAfterWrite(l1Ttl)
            .removalListener(removalListener)
            .recordStats()
            .build();
            
        this.l1PartitionCache = Caffeine.newBuilder()
            .maximumSize(cacheConfig.getL1Size())
            .expireAfterWrite(l1Ttl)
            .removalListener(removalListener)
            .recordStats()
            .build();
        
        logger.info("L1 cache initialized successfully");
    }
    
    private void initializeL2Cache() {
        IcebergConfiguration.CacheConfig cacheConfig = icebergConfig.getCache();
        logger.info("Initializing L2 cache with size: {} and TTL: {}", 
            cacheConfig.getL2Size(), cacheConfig.getL2Ttl());
        
        // L2 cache is implemented as a bounded concurrent map for simplicity
        // In a production system, this would use local disk storage
        
        logger.info("L2 cache initialized successfully");
    }
    
    private void initializeL3Cache() {
        IcebergConfiguration.CacheConfig cacheConfig = icebergConfig.getCache();
        
        if (!cacheConfig.isL3Enabled()) {
            logger.info("L3 distributed cache is disabled");
            return;
        }
        
        logger.info("Initializing L3 distributed cache coordination");
        
        // L3 cache coordination would integrate with Redis, Hazelcast, or similar
        // For now, we simulate with a registry of cache entries across nodes
        
        logger.info("L3 cache coordination initialized successfully");
    }
    
    private void startCleanupScheduler() {
        this.cleanupScheduler = Executors.newScheduledThreadPool(1);
        
        // Schedule cleanup every 5 minutes
        cleanupScheduler.scheduleAtFixedRate(this::performCleanup, 5, 5, TimeUnit.MINUTES);
        
        logger.info("Cache cleanup scheduler started");
    }
    
    // Table Metadata Caching
    
    /**
     * Gets cached table metadata, checking L1, L2, and L3 caches in order.
     */
    public Optional<Table> getCachedTable(TableIdentifier identifier) {
        String key = tableKey(identifier);
        
        // Check L1 cache first
        CachedTableMetadata cached = l1TableCache.getIfPresent(key);
        if (cached != null && !isExpired(cached)) {
            logger.debug("L1 cache hit for table: {}", identifier);
            return Optional.of(cached.getTable());
        }
        
        // Check L2 cache
        cached = l2TableCache.get(key);
        if (cached != null && !isExpired(cached)) {
            logger.debug("L2 cache hit for table: {}", identifier);
            // Promote to L1
            l1TableCache.put(key, cached);
            return Optional.of(cached.getTable());
        }
        
        // Check L3 cache if enabled
        if (icebergConfig.getCache().isL3Enabled()) {
            CacheEntry entry = l3CacheRegistry.get(key);
            if (entry != null && !isExpired(entry)) {
                logger.debug("L3 cache hit for table: {}", identifier);
                // Would fetch from distributed cache in real implementation
                return Optional.empty(); // Placeholder
            }
        }
        
        logger.debug("Cache miss for table: {}", identifier);
        return Optional.empty();
    }
    
    /**
     * Caches table metadata in all appropriate cache levels.
     */
    public void cacheTable(TableIdentifier identifier, Table table) {
        String key = tableKey(identifier);
        CachedTableMetadata cached = new CachedTableMetadata(table, Instant.now());
        
        logger.debug("Caching table metadata: {}", identifier);
        
        // Store in L1 cache
        l1TableCache.put(key, cached);
        
        // Store in L2 cache if L1 is near capacity
        if (l1TableCache.estimatedSize() > icebergConfig.getCache().getL1Size() * 0.8) {
            l2TableCache.put(key, cached);
        }
        
        // Register in L3 if enabled
        if (icebergConfig.getCache().isL3Enabled()) {
            l3CacheRegistry.put(key, new CacheEntry(key, Instant.now()));
        }
    }
    
    // Statistics Caching
    
    /**
     * Gets cached table statistics.
     */
    public Optional<TableStatistics> getCachedStatistics(TableIdentifier identifier) {
        String key = tableKey(identifier);
        
        // Check L1 cache
        TableStatistics stats = l1StatsCache.getIfPresent(key);
        if (stats != null) {
            logger.debug("L1 cache hit for statistics: {}", identifier);
            return Optional.of(stats);
        }
        
        // Check L2 cache
        stats = l2StatsCache.get(key);
        if (stats != null) {
            logger.debug("L2 cache hit for statistics: {}", identifier);
            l1StatsCache.put(key, stats);
            return Optional.of(stats);
        }
        
        logger.debug("Cache miss for statistics: {}", identifier);
        return Optional.empty();
    }
    
    /**
     * Caches table statistics.
     */
    public void cacheStatistics(TableIdentifier identifier, TableStatistics statistics) {
        String key = tableKey(identifier);
        
        logger.debug("Caching table statistics: {}", identifier);
        
        l1StatsCache.put(key, statistics);
        
        // Store in L2 if L1 is near capacity
        if (l1StatsCache.estimatedSize() > icebergConfig.getCache().getL1Size() * 0.8) {
            l2StatsCache.put(key, statistics);
        }
    }
    
    // Partition Statistics Caching
    
    /**
     * Gets cached partition statistics.
     */
    public Optional<List<PartitionStatistics>> getCachedPartitionStatistics(TableIdentifier identifier) {
        String key = tableKey(identifier);
        
        // Check L1 cache
        List<PartitionStatistics> partitionStats = l1PartitionCache.getIfPresent(key);
        if (partitionStats != null) {
            logger.debug("L1 cache hit for partition statistics: {}", identifier);
            return Optional.of(partitionStats);
        }
        
        // Check L2 cache
        partitionStats = l2PartitionCache.get(key);
        if (partitionStats != null) {
            logger.debug("L2 cache hit for partition statistics: {}", identifier);
            l1PartitionCache.put(key, partitionStats);
            return Optional.of(partitionStats);
        }
        
        logger.debug("Cache miss for partition statistics: {}", identifier);
        return Optional.empty();
    }
    
    /**
     * Caches partition statistics.
     */
    public void cachePartitionStatistics(TableIdentifier identifier, List<PartitionStatistics> partitionStats) {
        String key = tableKey(identifier);
        
        logger.debug("Caching partition statistics: {}", identifier);
        
        l1PartitionCache.put(key, partitionStats);
        
        // Store in L2 if L1 is near capacity
        if (l1PartitionCache.estimatedSize() > icebergConfig.getCache().getL1Size() * 0.8) {
            l2PartitionCache.put(key, partitionStats);
        }
    }
    
    // Cache Invalidation
    
    /**
     * Invalidates all cached data for a specific table.
     */
    public void invalidateTable(TableIdentifier identifier) {
        String key = tableKey(identifier);
        
        logger.info("Invalidating cache for table: {}", identifier);
        
        // Invalidate from all cache levels
        l1TableCache.invalidate(key);
        l1StatsCache.invalidate(key);
        l1PartitionCache.invalidate(key);
        
        l2TableCache.remove(key);
        l2StatsCache.remove(key);
        l2PartitionCache.remove(key);
        
        l3CacheRegistry.remove(key);
        
        // Track invalidation timestamp
        invalidationTimestamps.put(key, Instant.now());
    }
    
    /**
     * Invalidates all cached data for a namespace.
     */
    public void invalidateNamespace(String namespace) {
        logger.info("Invalidating cache for namespace: {}", namespace);
        
        String namespacePrefix = namespace + ".";
        
        // Invalidate L1 caches
        l1TableCache.asMap().keySet().removeIf(key -> key.startsWith(namespacePrefix));
        l1StatsCache.asMap().keySet().removeIf(key -> key.startsWith(namespacePrefix));
        l1PartitionCache.asMap().keySet().removeIf(key -> key.startsWith(namespacePrefix));
        
        // Invalidate L2 caches
        l2TableCache.keySet().removeIf(key -> key.startsWith(namespacePrefix));
        l2StatsCache.keySet().removeIf(key -> key.startsWith(namespacePrefix));
        l2PartitionCache.keySet().removeIf(key -> key.startsWith(namespacePrefix));
        
        // Invalidate L3 registry
        l3CacheRegistry.keySet().removeIf(key -> key.startsWith(namespacePrefix));
    }
    
    /**
     * Clears all caches.
     */
    public void clearAll() {
        logger.info("Clearing all metadata caches");
        
        l1TableCache.invalidateAll();
        l1StatsCache.invalidateAll();
        l1PartitionCache.invalidateAll();
        
        l2TableCache.clear();
        l2StatsCache.clear();
        l2PartitionCache.clear();
        
        l3CacheRegistry.clear();
        invalidationTimestamps.clear();
    }
    
    // Cache Statistics and Monitoring
    
    /**
     * Gets cache statistics for monitoring.
     */
    public CacheStatistics getCacheStatistics() {
        return new CacheStatistics(
            l1TableCache.stats(),
            l1StatsCache.stats(),
            l1PartitionCache.stats(),
            l2TableCache.size(),
            l2StatsCache.size(),
            l2PartitionCache.size(),
            l3CacheRegistry.size()
        );
    }
    
    // Utility Methods
    
    private String tableKey(TableIdentifier identifier) {
        return identifier.namespace().toString() + "." + identifier.name();
    }
    
    private Duration parseDuration(String duration) {
        // Simple duration parser for formats like "300s", "5m", "1h"
        if (duration.endsWith("s")) {
            return Duration.ofSeconds(Long.parseLong(duration.substring(0, duration.length() - 1)));
        } else if (duration.endsWith("m")) {
            return Duration.ofMinutes(Long.parseLong(duration.substring(0, duration.length() - 1)));
        } else if (duration.endsWith("h")) {
            return Duration.ofHours(Long.parseLong(duration.substring(0, duration.length() - 1)));
        } else {
            return Duration.ofSeconds(Long.parseLong(duration));
        }
    }
    
    private boolean isExpired(CachedTableMetadata cached) {
        Duration l2Ttl = parseDuration(icebergConfig.getCache().getL2Ttl());
        return cached.getCachedAt().plus(l2Ttl).isBefore(Instant.now());
    }
    
    private boolean isExpired(CacheEntry entry) {
        Duration l2Ttl = parseDuration(icebergConfig.getCache().getL2Ttl());
        return entry.getCachedAt().plus(l2Ttl).isBefore(Instant.now());
    }
    
    private void promoteToL2Cache(String key, Object value) {
        if (value instanceof CachedTableMetadata) {
            l2TableCache.put(key, (CachedTableMetadata) value);
        } else if (value instanceof TableStatistics) {
            l2StatsCache.put(key, (TableStatistics) value);
        } else if (value instanceof List) {
            l2PartitionCache.put(key, (List<PartitionStatistics>) value);
        }
    }
    
    private void performCleanup() {
        logger.debug("Performing cache cleanup");
        
        Instant now = Instant.now();
        Duration l2Ttl = parseDuration(icebergConfig.getCache().getL2Ttl());
        
        // Clean expired L2 entries
        l2TableCache.entrySet().removeIf(entry -> 
            entry.getValue().getCachedAt().plus(l2Ttl).isBefore(now));
        l2StatsCache.entrySet().removeIf(entry -> 
            entry.getValue().getLastUpdated().plus(l2Ttl).isBefore(now));
        
        // Clean expired L3 entries
        l3CacheRegistry.entrySet().removeIf(entry -> 
            entry.getValue().getCachedAt().plus(l2Ttl).isBefore(now));
        
        logger.debug("Cache cleanup completed");
    }
    
    @PreDestroy
    public void cleanup() {
        if (cleanupScheduler != null) {
            cleanupScheduler.shutdown();
            try {
                if (!cleanupScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    cleanupScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleanupScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        clearAll();
        logger.info("Metadata cache manager cleaned up successfully");
    }
    
    // Inner Classes
    
    /**
     * Wrapper for cached table metadata with timestamp.
     */
    public static class CachedTableMetadata {
        private final Table table;
        private final Instant cachedAt;
        
        public CachedTableMetadata(Table table, Instant cachedAt) {
            this.table = table;
            this.cachedAt = cachedAt;
        }
        
        public Table getTable() {
            return table;
        }
        
        public Instant getCachedAt() {
            return cachedAt;
        }
    }
    
    /**
     * Cache entry for L3 distributed cache coordination.
     */
    public static class CacheEntry {
        private final String key;
        private final Instant cachedAt;
        
        public CacheEntry(String key, Instant cachedAt) {
            this.key = key;
            this.cachedAt = cachedAt;
        }
        
        public String getKey() {
            return key;
        }
        
        public Instant getCachedAt() {
            return cachedAt;
        }
    }
    
    /**
     * Cache statistics for monitoring and observability.
     */
    public static class CacheStatistics {
        private final com.github.benmanes.caffeine.cache.stats.CacheStats l1TableStats;
        private final com.github.benmanes.caffeine.cache.stats.CacheStats l1StatsStats;
        private final com.github.benmanes.caffeine.cache.stats.CacheStats l1PartitionStats;
        private final long l2TableSize;
        private final long l2StatsSize;
        private final long l2PartitionSize;
        private final long l3Size;
        
        public CacheStatistics(
            com.github.benmanes.caffeine.cache.stats.CacheStats l1TableStats,
            com.github.benmanes.caffeine.cache.stats.CacheStats l1StatsStats,
            com.github.benmanes.caffeine.cache.stats.CacheStats l1PartitionStats,
            long l2TableSize,
            long l2StatsSize,
            long l2PartitionSize,
            long l3Size) {
            this.l1TableStats = l1TableStats;
            this.l1StatsStats = l1StatsStats;
            this.l1PartitionStats = l1PartitionStats;
            this.l2TableSize = l2TableSize;
            this.l2StatsSize = l2StatsSize;
            this.l2PartitionSize = l2PartitionSize;
            this.l3Size = l3Size;
        }
        
        public com.github.benmanes.caffeine.cache.stats.CacheStats getL1TableStats() {
            return l1TableStats;
        }
        
        public com.github.benmanes.caffeine.cache.stats.CacheStats getL1StatsStats() {
            return l1StatsStats;
        }
        
        public com.github.benmanes.caffeine.cache.stats.CacheStats getL1PartitionStats() {
            return l1PartitionStats;
        }
        
        public long getL2TableSize() {
            return l2TableSize;
        }
        
        public long getL2StatsSize() {
            return l2StatsSize;
        }
        
        public long getL2PartitionSize() {
            return l2PartitionSize;
        }
        
        public long getL3Size() {
            return l3Size;
        }
    }
}