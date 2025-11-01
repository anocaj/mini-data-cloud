package com.minicloud.controlplane.service;

import com.minicloud.controlplane.config.IcebergConfiguration;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Iceberg-specific query performance monitoring service.
 * Tracks metrics for Iceberg table operations, time travel queries, and transaction performance.
 */
@Service
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class IcebergQueryMetricsService {
    
    private static final Logger logger = LoggerFactory.getLogger(IcebergQueryMetricsService.class);
    
    private final MeterRegistry meterRegistry;
    private final IcebergConfiguration icebergConfig;
    
    // Metrics storage
    private final Map<String, QueryMetrics> activeQueries = new ConcurrentHashMap<>();
    private final Map<String, TableMetrics> tableMetrics = new ConcurrentHashMap<>();
    private final Map<String, TimeTravelMetrics> timeTravelMetrics = new ConcurrentHashMap<>();
    private final Map<String, TransactionMetrics> transactionMetrics = new ConcurrentHashMap<>();
    
    // Micrometer metrics
    private Counter icebergQueriesTotal;
    private Counter timeTravelQueriesTotal;
    private Counter transactionsTotal;
    private Timer queryExecutionTime;
    private Timer timeTravelQueryTime;
    private Timer transactionTime;
    private Timer partitionPruningTime;
    private Counter filesScannedTotal;
    private Counter filesPrunedTotal;

    
    @Autowired
    public IcebergQueryMetricsService(MeterRegistry meterRegistry, IcebergConfiguration icebergConfig) {
        this.meterRegistry = meterRegistry;
        this.icebergConfig = icebergConfig;
    }
    
    @PostConstruct
    public void initialize() {
        if (!icebergConfig.isEnabled()) {
            logger.info("Iceberg query metrics service is disabled");
            return;
        }
        
        logger.info("Initializing Iceberg query metrics service");
        
        // Initialize Micrometer metrics
        initializeMetrics();
        
        logger.info("Iceberg query metrics service initialized successfully");
    }
    
    private void initializeMetrics() {
        // Query metrics
        icebergQueriesTotal = Counter.builder("iceberg.queries.total")
            .description("Total number of Iceberg queries executed")
            .tag("type", "iceberg")
            .register(meterRegistry);
        
        timeTravelQueriesTotal = Counter.builder("iceberg.time_travel.queries.total")
            .description("Total number of time travel queries executed")
            .register(meterRegistry);
        
        transactionsTotal = Counter.builder("iceberg.transactions.total")
            .description("Total number of Iceberg transactions")
            .register(meterRegistry);
        
        // Timing metrics
        queryExecutionTime = Timer.builder("iceberg.query.execution.time")
            .description("Iceberg query execution time")
            .register(meterRegistry);
        
        timeTravelQueryTime = Timer.builder("iceberg.time_travel.execution.time")
            .description("Time travel query execution time")
            .register(meterRegistry);
        
        transactionTime = Timer.builder("iceberg.transaction.time")
            .description("Iceberg transaction execution time")
            .register(meterRegistry);
        
        partitionPruningTime = Timer.builder("iceberg.partition.pruning.time")
            .description("Partition pruning execution time")
            .register(meterRegistry);
        
        // File metrics
        filesScannedTotal = Counter.builder("iceberg.files.scanned.total")
            .description("Total number of files scanned")
            .register(meterRegistry);
        
        filesPrunedTotal = Counter.builder("iceberg.files.pruned.total")
            .description("Total number of files pruned")
            .register(meterRegistry);
        
        // Gauge metrics - register with lambda functions
        meterRegistry.gauge("iceberg.queries.active", this, service -> (double) service.getActiveQueryCount());
        meterRegistry.gauge("iceberg.transactions.active", this, service -> (double) service.getActiveTransactionCount());
    }
    
    // Query Metrics
    
    /**
     * Records the start of an Iceberg query execution.
     */
    public void recordQueryStart(String queryId, TableIdentifier tableId, String queryType) {
        logger.debug("Recording query start: {} for table: {}", queryId, tableId);
        
        QueryMetrics metrics = new QueryMetrics(queryId, tableId, queryType);
        activeQueries.put(queryId, metrics);
        
        // Increment counter
        icebergQueriesTotal.increment();
        
        // Update table metrics
        updateTableMetrics(tableId, "query_started");
    }
    
    /**
     * Records the completion of an Iceberg query execution.
     */
    public void recordQueryCompletion(String queryId, long rowsReturned, long bytesScanned, 
                                    int filesScanned, int filesPruned) {
        QueryMetrics metrics = activeQueries.remove(queryId);
        if (metrics == null) {
            logger.warn("No metrics found for query: {}", queryId);
            return;
        }
        
        metrics.complete(rowsReturned, bytesScanned, filesScanned, filesPruned);
        
        logger.info("Query {} completed: {} rows, {} bytes, {} files scanned, {} files pruned", 
            queryId, rowsReturned, bytesScanned, filesScanned, filesPruned);
        
        // Record timing
        Duration executionTime = metrics.getExecutionTime();
        queryExecutionTime.record(executionTime);
        
        // Record file metrics
        filesScannedTotal.increment(filesScanned);
        filesPrunedTotal.increment(filesPruned);
        
        // Update table metrics
        updateTableMetrics(metrics.getTableId(), "query_completed");
    }
    
    /**
     * Records a query failure.
     */
    public void recordQueryFailure(String queryId, String errorMessage) {
        QueryMetrics metrics = activeQueries.remove(queryId);
        if (metrics == null) {
            logger.warn("No metrics found for failed query: {}", queryId);
            return;
        }
        
        metrics.fail(errorMessage);
        
        logger.warn("Query {} failed: {}", queryId, errorMessage);
        
        // Update table metrics
        updateTableMetrics(metrics.getTableId(), "query_failed");
    }
    
    // Time Travel Metrics
    
    /**
     * Records the start of a time travel query.
     */
    public void recordTimeTravelQueryStart(String queryId, TableIdentifier tableId, 
                                         Instant targetTimestamp, Long snapshotId) {
        logger.debug("Recording time travel query start: {} for table: {} at {}", 
            queryId, tableId, targetTimestamp);
        
        TimeTravelMetrics metrics = new TimeTravelMetrics(queryId, tableId, targetTimestamp, snapshotId);
        timeTravelMetrics.put(queryId, metrics);
        
        // Increment counter
        timeTravelQueriesTotal.increment();
    }
    
    /**
     * Records the completion of a time travel query.
     */
    public void recordTimeTravelQueryCompletion(String queryId, long rowsReturned, 
                                              int snapshotsEvaluated, boolean snapshotFound) {
        TimeTravelMetrics metrics = timeTravelMetrics.remove(queryId);
        if (metrics == null) {
            logger.warn("No time travel metrics found for query: {}", queryId);
            return;
        }
        
        metrics.complete(rowsReturned, snapshotsEvaluated, snapshotFound);
        
        logger.info("Time travel query {} completed: {} rows, {} snapshots evaluated, snapshot found: {}", 
            queryId, rowsReturned, snapshotsEvaluated, snapshotFound);
        
        // Record timing
        Duration executionTime = metrics.getExecutionTime();
        timeTravelQueryTime.record(executionTime);
    }
    
    // Transaction Metrics
    
    /**
     * Records the start of an Iceberg transaction.
     */
    public void recordTransactionStart(String transactionId, TableIdentifier tableId, String operationType) {
        logger.debug("Recording transaction start: {} for table: {} ({})", 
            transactionId, tableId, operationType);
        
        TransactionMetrics metrics = new TransactionMetrics(transactionId, tableId, operationType);
        transactionMetrics.put(transactionId, metrics);
        
        // Increment counter
        transactionsTotal.increment();
    }
    
    /**
     * Records the completion of an Iceberg transaction.
     */
    public void recordTransactionCompletion(String transactionId, long rowsAffected, 
                                          int filesAdded, int filesDeleted, boolean committed) {
        TransactionMetrics metrics = transactionMetrics.remove(transactionId);
        if (metrics == null) {
            logger.warn("No transaction metrics found for: {}", transactionId);
            return;
        }
        
        metrics.complete(rowsAffected, filesAdded, filesDeleted, committed);
        
        logger.info("Transaction {} completed: {} rows affected, {} files added, {} files deleted, committed: {}", 
            transactionId, rowsAffected, filesAdded, filesDeleted, committed);
        
        // Record timing
        Duration executionTime = metrics.getExecutionTime();
        transactionTime.record(executionTime);
    }
    
    // Partition Pruning Metrics
    
    /**
     * Records partition pruning performance.
     */
    public void recordPartitionPruning(String queryId, int totalFiles, int prunedFiles, 
                                     Duration pruningTime) {
        logger.debug("Recording partition pruning for query {}: {} total files, {} pruned, {} ms", 
            queryId, totalFiles, prunedFiles, pruningTime.toMillis());
        
        // Record timing
        partitionPruningTime.record(pruningTime);
        
        // Calculate pruning efficiency
        double pruningEfficiency = totalFiles > 0 ? (double) prunedFiles / totalFiles : 0.0;
        
        // Record custom metric for pruning efficiency
        meterRegistry.gauge("iceberg.partition.pruning.efficiency", pruningEfficiency);
        
        logger.info("Partition pruning efficiency for query {}: {:.2%}", queryId, pruningEfficiency);
    }
    
    // Table-specific Metrics
    
    private void updateTableMetrics(TableIdentifier tableId, String event) {
        String tableKey = tableId.toString();
        TableMetrics metrics = tableMetrics.computeIfAbsent(tableKey, 
            k -> new TableMetrics(tableId));
        
        metrics.recordEvent(event);
    }
    
    /**
     * Gets performance metrics for a specific table.
     */
    public TablePerformanceMetrics getTablePerformanceMetrics(TableIdentifier tableId) {
        String tableKey = tableId.toString();
        TableMetrics metrics = tableMetrics.get(tableKey);
        
        if (metrics == null) {
            return new TablePerformanceMetrics(tableId, 0, 0, 0, Duration.ZERO, Duration.ZERO);
        }
        
        return new TablePerformanceMetrics(
            tableId,
            metrics.getQueryCount(),
            metrics.getSuccessfulQueries(),
            metrics.getFailedQueries(),
            metrics.getAverageQueryTime(),
            metrics.getTotalQueryTime()
        );
    }
    
    // Aggregate Metrics
    
    /**
     * Gets overall Iceberg performance metrics.
     */
    public IcebergPerformanceMetrics getOverallPerformanceMetrics() {
        long totalQueries = (long) icebergQueriesTotal.count();
        long totalTimeTravelQueries = (long) timeTravelQueriesTotal.count();
        long totalTransactions = (long) transactionsTotal.count();
        
        double averageQueryTime = queryExecutionTime.mean(java.util.concurrent.TimeUnit.MILLISECONDS);
        double averageTimeTravelTime = timeTravelQueryTime.mean(java.util.concurrent.TimeUnit.MILLISECONDS);
        double averageTransactionTime = transactionTime.mean(java.util.concurrent.TimeUnit.MILLISECONDS);
        
        long totalFilesScanned = (long) filesScannedTotal.count();
        long totalFilesPruned = (long) filesPrunedTotal.count();
        
        return new IcebergPerformanceMetrics(
            totalQueries,
            totalTimeTravelQueries,
            totalTransactions,
            averageQueryTime,
            averageTimeTravelTime,
            averageTransactionTime,
            totalFilesScanned,
            totalFilesPruned,
            getActiveQueryCount(),
            getActiveTransactionCount()
        );
    }
    
    private int getActiveQueryCount() {
        return activeQueries.size();
    }
    
    private int getActiveTransactionCount() {
        return transactionMetrics.size();
    }
    
    // Metrics Data Classes
    
    private static class QueryMetrics {
        private final String queryId;
        private final TableIdentifier tableId;
        private final String queryType;
        private final Instant startTime;
        private volatile Instant endTime;
        private volatile long rowsReturned;
        private volatile long bytesScanned;
        private volatile int filesScanned;
        private volatile int filesPruned;
        private volatile String errorMessage;
        
        public QueryMetrics(String queryId, TableIdentifier tableId, String queryType) {
            this.queryId = queryId;
            this.tableId = tableId;
            this.queryType = queryType;
            this.startTime = Instant.now();
        }
        
        public void complete(long rowsReturned, long bytesScanned, int filesScanned, int filesPruned) {
            this.endTime = Instant.now();
            this.rowsReturned = rowsReturned;
            this.bytesScanned = bytesScanned;
            this.filesScanned = filesScanned;
            this.filesPruned = filesPruned;
        }
        
        public void fail(String errorMessage) {
            this.endTime = Instant.now();
            this.errorMessage = errorMessage;
        }
        
        public Duration getExecutionTime() {
            Instant end = endTime != null ? endTime : Instant.now();
            return Duration.between(startTime, end);
        }
        
        public TableIdentifier getTableId() { return tableId; }
        public boolean isCompleted() { return endTime != null; }
        public boolean isFailed() { return errorMessage != null; }
    }
    
    private static class TimeTravelMetrics {
        private final String queryId;
        private final TableIdentifier tableId;
        private final Instant targetTimestamp;
        private final Long snapshotId;
        private final Instant startTime;
        private volatile Instant endTime;
        private volatile long rowsReturned;
        private volatile int snapshotsEvaluated;
        private volatile boolean snapshotFound;
        
        public TimeTravelMetrics(String queryId, TableIdentifier tableId, 
                               Instant targetTimestamp, Long snapshotId) {
            this.queryId = queryId;
            this.tableId = tableId;
            this.targetTimestamp = targetTimestamp;
            this.snapshotId = snapshotId;
            this.startTime = Instant.now();
        }
        
        public void complete(long rowsReturned, int snapshotsEvaluated, boolean snapshotFound) {
            this.endTime = Instant.now();
            this.rowsReturned = rowsReturned;
            this.snapshotsEvaluated = snapshotsEvaluated;
            this.snapshotFound = snapshotFound;
        }
        
        public Duration getExecutionTime() {
            Instant end = endTime != null ? endTime : Instant.now();
            return Duration.between(startTime, end);
        }
    }
    
    private static class TransactionMetrics {
        private final String transactionId;
        private final TableIdentifier tableId;
        private final String operationType;
        private final Instant startTime;
        private volatile Instant endTime;
        private volatile long rowsAffected;
        private volatile int filesAdded;
        private volatile int filesDeleted;
        private volatile boolean committed;
        
        public TransactionMetrics(String transactionId, TableIdentifier tableId, String operationType) {
            this.transactionId = transactionId;
            this.tableId = tableId;
            this.operationType = operationType;
            this.startTime = Instant.now();
        }
        
        public void complete(long rowsAffected, int filesAdded, int filesDeleted, boolean committed) {
            this.endTime = Instant.now();
            this.rowsAffected = rowsAffected;
            this.filesAdded = filesAdded;
            this.filesDeleted = filesDeleted;
            this.committed = committed;
        }
        
        public Duration getExecutionTime() {
            Instant end = endTime != null ? endTime : Instant.now();
            return Duration.between(startTime, end);
        }
    }
    
    private static class TableMetrics {
        private final TableIdentifier tableId;
        private final AtomicLong queryCount = new AtomicLong(0);
        private final AtomicLong successfulQueries = new AtomicLong(0);
        private final AtomicLong failedQueries = new AtomicLong(0);
        private final AtomicLong totalQueryTimeMs = new AtomicLong(0);
        private final AtomicReference<Instant> lastAccessed = new AtomicReference<>(Instant.now());
        
        public TableMetrics(TableIdentifier tableId) {
            this.tableId = tableId;
        }
        
        public void recordEvent(String event) {
            lastAccessed.set(Instant.now());
            
            switch (event) {
                case "query_started":
                    queryCount.incrementAndGet();
                    break;
                case "query_completed":
                    successfulQueries.incrementAndGet();
                    break;
                case "query_failed":
                    failedQueries.incrementAndGet();
                    break;
            }
        }
        
        public long getQueryCount() { return queryCount.get(); }
        public long getSuccessfulQueries() { return successfulQueries.get(); }
        public long getFailedQueries() { return failedQueries.get(); }
        
        public Duration getAverageQueryTime() {
            long count = queryCount.get();
            if (count == 0) return Duration.ZERO;
            return Duration.ofMillis(totalQueryTimeMs.get() / count);
        }
        
        public Duration getTotalQueryTime() {
            return Duration.ofMillis(totalQueryTimeMs.get());
        }
    }
    
    // Public Data Classes
    
    public static class TablePerformanceMetrics {
        private final TableIdentifier tableId;
        private final long queryCount;
        private final long successfulQueries;
        private final long failedQueries;
        private final Duration averageQueryTime;
        private final Duration totalQueryTime;
        
        public TablePerformanceMetrics(TableIdentifier tableId, long queryCount, 
                                     long successfulQueries, long failedQueries,
                                     Duration averageQueryTime, Duration totalQueryTime) {
            this.tableId = tableId;
            this.queryCount = queryCount;
            this.successfulQueries = successfulQueries;
            this.failedQueries = failedQueries;
            this.averageQueryTime = averageQueryTime;
            this.totalQueryTime = totalQueryTime;
        }
        
        // Getters
        public TableIdentifier getTableId() { return tableId; }
        public long getQueryCount() { return queryCount; }
        public long getSuccessfulQueries() { return successfulQueries; }
        public long getFailedQueries() { return failedQueries; }
        public Duration getAverageQueryTime() { return averageQueryTime; }
        public Duration getTotalQueryTime() { return totalQueryTime; }
        public double getSuccessRate() { 
            return queryCount > 0 ? (double) successfulQueries / queryCount : 0.0; 
        }
    }
    
    public static class IcebergPerformanceMetrics {
        private final long totalQueries;
        private final long totalTimeTravelQueries;
        private final long totalTransactions;
        private final double averageQueryTimeMs;
        private final double averageTimeTravelTimeMs;
        private final double averageTransactionTimeMs;
        private final long totalFilesScanned;
        private final long totalFilesPruned;
        private final int activeQueries;
        private final int activeTransactions;
        
        public IcebergPerformanceMetrics(long totalQueries, long totalTimeTravelQueries, 
                                       long totalTransactions, double averageQueryTimeMs,
                                       double averageTimeTravelTimeMs, double averageTransactionTimeMs,
                                       long totalFilesScanned, long totalFilesPruned,
                                       int activeQueries, int activeTransactions) {
            this.totalQueries = totalQueries;
            this.totalTimeTravelQueries = totalTimeTravelQueries;
            this.totalTransactions = totalTransactions;
            this.averageQueryTimeMs = averageQueryTimeMs;
            this.averageTimeTravelTimeMs = averageTimeTravelTimeMs;
            this.averageTransactionTimeMs = averageTransactionTimeMs;
            this.totalFilesScanned = totalFilesScanned;
            this.totalFilesPruned = totalFilesPruned;
            this.activeQueries = activeQueries;
            this.activeTransactions = activeTransactions;
        }
        
        // Getters
        public long getTotalQueries() { return totalQueries; }
        public long getTotalTimeTravelQueries() { return totalTimeTravelQueries; }
        public long getTotalTransactions() { return totalTransactions; }
        public double getAverageQueryTimeMs() { return averageQueryTimeMs; }
        public double getAverageTimeTravelTimeMs() { return averageTimeTravelTimeMs; }
        public double getAverageTransactionTimeMs() { return averageTransactionTimeMs; }
        public long getTotalFilesScanned() { return totalFilesScanned; }
        public long getTotalFilesPruned() { return totalFilesPruned; }
        public int getActiveQueries() { return activeQueries; }
        public int getActiveTransactions() { return activeTransactions; }
        
        public double getFilePruningEfficiency() {
            return totalFilesScanned > 0 ? (double) totalFilesPruned / totalFilesScanned : 0.0;
        }
    }
}