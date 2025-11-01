package com.minicloud.controlplane.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Metrics service for NYC dataset analytics and performance monitoring.
 * Tracks dataset usage, query patterns, and performance characteristics.
 */
@Service
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class NYCDatasetMetricsService {
    
    private static final Logger logger = LoggerFactory.getLogger(NYCDatasetMetricsService.class);
    
    private final MeterRegistry meterRegistry;
    
    // Dataset metrics
    private final ConcurrentHashMap<String, AtomicLong> datasetQueryCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> datasetRowsProcessed = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> datasetBytesScanned = new ConcurrentHashMap<>();
    
    // Micrometer metrics
    private Counter nycQueriesTotal;
    private Counter nycRowsProcessedTotal;
    private Counter nycBytesScannedTotal;
    private Timer nycQueryExecutionTime;
    private Timer nycDataLoadingTime;
    private Counter nycPartitionsPrunedTotal;
    private Counter nycFilesSkippedTotal;
    
    // Dataset-specific counters
    private Counter taxiYellowQueries;
    private Counter taxiGreenQueries;
    private Counter fhvQueries;
    private Counter serviceRequestQueries;
    private Counter weatherQueries;
    
    @Autowired
    public NYCDatasetMetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }
    
    @PostConstruct
    public void initialize() {
        logger.info("Initializing NYC dataset metrics service");
        
        // General NYC metrics
        nycQueriesTotal = Counter.builder("nyc.queries.total")
            .description("Total number of queries on NYC datasets")
            .register(meterRegistry);
        
        nycRowsProcessedTotal = Counter.builder("nyc.rows.processed.total")
            .description("Total number of rows processed from NYC datasets")
            .register(meterRegistry);
        
        nycBytesScannedTotal = Counter.builder("nyc.bytes.scanned.total")
            .description("Total bytes scanned from NYC datasets")
            .register(meterRegistry);
        
        nycQueryExecutionTime = Timer.builder("nyc.query.execution.time")
            .description("NYC dataset query execution time")
            .register(meterRegistry);
        
        nycDataLoadingTime = Timer.builder("nyc.data.loading.time")
            .description("NYC dataset loading time")
            .register(meterRegistry);
        
        nycPartitionsPrunedTotal = Counter.builder("nyc.partitions.pruned.total")
            .description("Total number of partitions pruned in NYC queries")
            .register(meterRegistry);
        
        nycFilesSkippedTotal = Counter.builder("nyc.files.skipped.total")
            .description("Total number of files skipped due to partition pruning")
            .register(meterRegistry);
        
        // Dataset-specific metrics
        taxiYellowQueries = Counter.builder("nyc.dataset.queries.total")
            .description("Queries on NYC Yellow Taxi dataset")
            .tag("dataset", "taxi_yellow")
            .register(meterRegistry);
        
        taxiGreenQueries = Counter.builder("nyc.dataset.queries.total")
            .description("Queries on NYC Green Taxi dataset")
            .tag("dataset", "taxi_green")
            .register(meterRegistry);
        
        fhvQueries = Counter.builder("nyc.dataset.queries.total")
            .description("Queries on NYC For-Hire Vehicle dataset")
            .tag("dataset", "fhv")
            .register(meterRegistry);
        
        serviceRequestQueries = Counter.builder("nyc.dataset.queries.total")
            .description("Queries on NYC 311 Service Requests dataset")
            .tag("dataset", "service_requests")
            .register(meterRegistry);
        
        weatherQueries = Counter.builder("nyc.dataset.queries.total")
            .description("Queries on NYC Weather dataset")
            .tag("dataset", "weather")
            .register(meterRegistry);
        
        // Initialize dataset tracking
        initializeDatasetTracking();
        
        logger.info("NYC dataset metrics service initialized successfully");
    }
    
    private void initializeDatasetTracking() {
        String[] datasets = {"taxi_yellow", "taxi_green", "fhv", "service_requests", "weather"};
        
        for (String dataset : datasets) {
            datasetQueryCounts.put(dataset, new AtomicLong(0));
            datasetRowsProcessed.put(dataset, new AtomicLong(0));
            datasetBytesScanned.put(dataset, new AtomicLong(0));
            
            // Register gauge metrics for each dataset
            meterRegistry.gauge("nyc.dataset.queries.active", 
                datasetQueryCounts.get(dataset), 
                counter -> counter.doubleValue());
        }
    }
    
    /**
     * Records the start of a NYC dataset query.
     */
    public void recordQueryStart(String dataset, String queryId, String queryType) {
        logger.debug("Recording NYC query start: {} on dataset: {} ({})", queryId, dataset, queryType);
        
        nycQueriesTotal.increment();
        
        // Increment dataset-specific counter
        AtomicLong datasetCounter = datasetQueryCounts.get(dataset);
        if (datasetCounter != null) {
            datasetCounter.incrementAndGet();
        }
        
        // Increment specific dataset metrics
        switch (dataset.toLowerCase()) {
            case "taxi_yellow":
                taxiYellowQueries.increment();
                break;
            case "taxi_green":
                taxiGreenQueries.increment();
                break;
            case "fhv":
                fhvQueries.increment();
                break;
            case "service_requests":
                serviceRequestQueries.increment();
                break;
            case "weather":
                weatherQueries.increment();
                break;
        }
    }
    
    /**
     * Records the completion of a NYC dataset query.
     */
    public void recordQueryCompletion(String dataset, String queryId, Duration executionTime,
                                    long rowsProcessed, long bytesScanned, 
                                    int partitionsPruned, int filesSkipped) {
        logger.info("NYC query {} completed on {}: {} rows, {} bytes, {} ms", 
            queryId, dataset, rowsProcessed, bytesScanned, executionTime.toMillis());
        
        // Record timing
        nycQueryExecutionTime.record(executionTime);
        
        // Record data processing metrics
        nycRowsProcessedTotal.increment(rowsProcessed);
        nycBytesScannedTotal.increment(bytesScanned);
        nycPartitionsPrunedTotal.increment(partitionsPruned);
        nycFilesSkippedTotal.increment(filesSkipped);
        
        // Update dataset-specific metrics
        AtomicLong rowsCounter = datasetRowsProcessed.get(dataset);
        if (rowsCounter != null) {
            rowsCounter.addAndGet(rowsProcessed);
        }
        
        AtomicLong bytesCounter = datasetBytesScanned.get(dataset);
        if (bytesCounter != null) {
            bytesCounter.addAndGet(bytesScanned);
        }
    }
    
    /**
     * Records NYC dataset loading performance.
     */
    public void recordDataLoading(String dataset, Duration loadingTime, long rowsLoaded, 
                                long filesCreated, String partitionStrategy) {
        logger.info("NYC dataset {} loaded: {} rows, {} files, {} ms, partition strategy: {}", 
            dataset, rowsLoaded, filesCreated, loadingTime.toMillis(), partitionStrategy);
        
        nycDataLoadingTime.record(loadingTime);
        
        // Record custom metrics for data loading
        meterRegistry.counter("nyc.data.loading.rows.total", "dataset", dataset)
            .increment(rowsLoaded);
        
        meterRegistry.counter("nyc.data.loading.files.total", "dataset", dataset)
            .increment(filesCreated);
    }
    
    /**
     * Records partition pruning effectiveness for NYC queries.
     */
    public void recordPartitionPruning(String dataset, String queryId, 
                                     int totalPartitions, int prunedPartitions,
                                     int totalFiles, int skippedFiles) {
        double pruningEfficiency = totalPartitions > 0 ? 
            (double) prunedPartitions / totalPartitions : 0.0;
        
        double fileSkipEfficiency = totalFiles > 0 ? 
            (double) skippedFiles / totalFiles : 0.0;
        
        logger.debug("Partition pruning for {} query {}: {:.2%} partitions pruned, {:.2%} files skipped", 
            dataset, queryId, pruningEfficiency, fileSkipEfficiency);
        
        // Record efficiency metrics using AtomicReference for gauge values
        AtomicReference<Double> pruningEfficiencyRef = new AtomicReference<>(pruningEfficiency);
        AtomicReference<Double> fileSkipEfficiencyRef = new AtomicReference<>(fileSkipEfficiency);
        
        Gauge.builder("nyc.partition.pruning.efficiency", pruningEfficiencyRef, ref -> ref.get())
            .description("Partition pruning efficiency for NYC datasets")
            .tag("dataset", dataset)
            .register(meterRegistry);
        
        Gauge.builder("nyc.file.skip.efficiency", fileSkipEfficiencyRef, ref -> ref.get())
            .description("File skip efficiency for NYC datasets")
            .tag("dataset", dataset)
            .register(meterRegistry);
    }
    
    /**
     * Records popular query patterns on NYC datasets.
     */
    public void recordQueryPattern(String dataset, String queryPattern, String timeRange) {
        logger.debug("Recording query pattern on {}: {} ({})", dataset, queryPattern, timeRange);
        
        meterRegistry.counter("nyc.query.patterns.total", 
            "dataset", dataset, 
            "pattern", queryPattern,
            "time_range", timeRange)
            .increment();
    }
    
    /**
     * Gets NYC dataset performance metrics.
     */
    public NYCDatasetPerformanceMetrics getPerformanceMetrics() {
        long totalQueries = (long) nycQueriesTotal.count();
        long totalRowsProcessed = (long) nycRowsProcessedTotal.count();
        long totalBytesScanned = (long) nycBytesScannedTotal.count();
        
        double averageQueryTime = nycQueryExecutionTime.mean(java.util.concurrent.TimeUnit.MILLISECONDS);
        double averageDataLoadingTime = nycDataLoadingTime.mean(java.util.concurrent.TimeUnit.MILLISECONDS);
        
        long totalPartitionsPruned = (long) nycPartitionsPrunedTotal.count();
        long totalFilesSkipped = (long) nycFilesSkippedTotal.count();
        
        // Dataset-specific metrics
        long taxiYellowQueryCount = (long) taxiYellowQueries.count();
        long taxiGreenQueryCount = (long) taxiGreenQueries.count();
        long fhvQueryCount = (long) fhvQueries.count();
        long serviceRequestQueryCount = (long) serviceRequestQueries.count();
        long weatherQueryCount = (long) weatherQueries.count();
        
        return new NYCDatasetPerformanceMetrics(
            totalQueries,
            totalRowsProcessed,
            totalBytesScanned,
            averageQueryTime,
            averageDataLoadingTime,
            totalPartitionsPruned,
            totalFilesSkipped,
            taxiYellowQueryCount,
            taxiGreenQueryCount,
            fhvQueryCount,
            serviceRequestQueryCount,
            weatherQueryCount
        );
    }
    
    /**
     * Gets metrics for a specific NYC dataset.
     */
    public DatasetMetrics getDatasetMetrics(String dataset) {
        AtomicLong queryCount = datasetQueryCounts.get(dataset);
        AtomicLong rowsProcessed = datasetRowsProcessed.get(dataset);
        AtomicLong bytesScanned = datasetBytesScanned.get(dataset);
        
        return new DatasetMetrics(
            dataset,
            queryCount != null ? queryCount.get() : 0,
            rowsProcessed != null ? rowsProcessed.get() : 0,
            bytesScanned != null ? bytesScanned.get() : 0
        );
    }
    
    /**
     * Data class for overall NYC dataset performance metrics.
     */
    public static class NYCDatasetPerformanceMetrics {
        private final long totalQueries;
        private final long totalRowsProcessed;
        private final long totalBytesScanned;
        private final double averageQueryTimeMs;
        private final double averageDataLoadingTimeMs;
        private final long totalPartitionsPruned;
        private final long totalFilesSkipped;
        private final long taxiYellowQueries;
        private final long taxiGreenQueries;
        private final long fhvQueries;
        private final long serviceRequestQueries;
        private final long weatherQueries;
        
        public NYCDatasetPerformanceMetrics(long totalQueries, long totalRowsProcessed,
                                          long totalBytesScanned, double averageQueryTimeMs,
                                          double averageDataLoadingTimeMs, long totalPartitionsPruned,
                                          long totalFilesSkipped, long taxiYellowQueries,
                                          long taxiGreenQueries, long fhvQueries,
                                          long serviceRequestQueries, long weatherQueries) {
            this.totalQueries = totalQueries;
            this.totalRowsProcessed = totalRowsProcessed;
            this.totalBytesScanned = totalBytesScanned;
            this.averageQueryTimeMs = averageQueryTimeMs;
            this.averageDataLoadingTimeMs = averageDataLoadingTimeMs;
            this.totalPartitionsPruned = totalPartitionsPruned;
            this.totalFilesSkipped = totalFilesSkipped;
            this.taxiYellowQueries = taxiYellowQueries;
            this.taxiGreenQueries = taxiGreenQueries;
            this.fhvQueries = fhvQueries;
            this.serviceRequestQueries = serviceRequestQueries;
            this.weatherQueries = weatherQueries;
        }
        
        // Getters
        public long getTotalQueries() { return totalQueries; }
        public long getTotalRowsProcessed() { return totalRowsProcessed; }
        public long getTotalBytesScanned() { return totalBytesScanned; }
        public double getAverageQueryTimeMs() { return averageQueryTimeMs; }
        public double getAverageDataLoadingTimeMs() { return averageDataLoadingTimeMs; }
        public long getTotalPartitionsPruned() { return totalPartitionsPruned; }
        public long getTotalFilesSkipped() { return totalFilesSkipped; }
        public long getTaxiYellowQueries() { return taxiYellowQueries; }
        public long getTaxiGreenQueries() { return taxiGreenQueries; }
        public long getFhvQueries() { return fhvQueries; }
        public long getServiceRequestQueries() { return serviceRequestQueries; }
        public long getWeatherQueries() { return weatherQueries; }
        
        public double getPartitionPruningEfficiency() {
            return totalPartitionsPruned > 0 ? 
                (double) totalPartitionsPruned / (totalPartitionsPruned + totalFilesSkipped) : 0.0;
        }
    }
    
    /**
     * Data class for individual dataset metrics.
     */
    public static class DatasetMetrics {
        private final String dataset;
        private final long queryCount;
        private final long rowsProcessed;
        private final long bytesScanned;
        
        public DatasetMetrics(String dataset, long queryCount, long rowsProcessed, long bytesScanned) {
            this.dataset = dataset;
            this.queryCount = queryCount;
            this.rowsProcessed = rowsProcessed;
            this.bytesScanned = bytesScanned;
        }
        
        // Getters
        public String getDataset() { return dataset; }
        public long getQueryCount() { return queryCount; }
        public long getRowsProcessed() { return rowsProcessed; }
        public long getBytesScanned() { return bytesScanned; }
    }
}