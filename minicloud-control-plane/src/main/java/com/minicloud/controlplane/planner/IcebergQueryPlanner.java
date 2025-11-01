package com.minicloud.controlplane.planner;

import com.minicloud.controlplane.service.GlobalCatalogService;
import com.minicloud.controlplane.service.StatisticsCollectionService;
import com.minicloud.controlplane.sql.ParsedQuery;
import com.minicloud.proto.execution.QueryExecutionProto.*;
import com.minicloud.proto.common.CommonProto;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.RelVisitor;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;

import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Iceberg-aware query planner that leverages Iceberg metadata for query optimization.
 * Implements partition pruning, file skipping, and statistics-based optimization.
 */
@Component
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class IcebergQueryPlanner {
    
    private static final Logger logger = LoggerFactory.getLogger(IcebergQueryPlanner.class);
    
    private final GlobalCatalogService catalogService;
    private final StatisticsCollectionService statisticsService;
    private final AtomicInteger stageIdGenerator = new AtomicInteger(0);
    
    @Autowired
    public IcebergQueryPlanner(
            GlobalCatalogService catalogService,
            StatisticsCollectionService statisticsService) {
        this.catalogService = catalogService;
        this.statisticsService = statisticsService;
    }
    
    /**
     * Creates an optimized execution plan for Iceberg tables using metadata-driven optimization.
     */
    public ExecutionPlan createOptimizedExecutionPlan(String queryId, ParsedQuery parsedQuery) {
        logger.info("Creating Iceberg-optimized execution plan for query: {}", queryId);
        
        try {
            RelNode relNode = parsedQuery.getRelNode();
            List<ExecutionStage> stages = new ArrayList<>();
            
            // Analyze the query for Iceberg table references
            IcebergTableAnalyzer analyzer = new IcebergTableAnalyzer();
            analyzer.visit(relNode, 0, null);
            
            // Create optimized stages with Iceberg metadata
            stages.addAll(analyzer.getOptimizedStages());
            
            // Add stage dependencies
            addStageDependencies(stages);
            
            ExecutionPlan plan = new ExecutionPlan(queryId, parsedQuery.getOriginalSql(), stages);
            
            logger.info("Created Iceberg-optimized execution plan with {} stages for query {}", 
                stages.size(), queryId);
            return plan;
            
        } catch (Exception e) {
            logger.error("Failed to create Iceberg-optimized execution plan for query: {}", queryId, e);
            throw new RuntimeException("Iceberg query planning failed", e);
        }
    }
    
    /**
     * Performs partition pruning using Iceberg metadata and query filters.
     */
    public List<DataFile> prunePartitions(TableIdentifier tableId, Expression filter) {
        try {
            logger.debug("Performing partition pruning for table: {} with filter: {}", tableId, filter);
            
            Table table = catalogService.loadTable(tableId);
            org.apache.iceberg.TableScan scan = table.newScan();
            
            // Apply filter for partition pruning
            if (filter != null) {
                scan = scan.filter(filter);
            }
            
            List<DataFile> prunedFiles = new ArrayList<>();
            try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
                for (FileScanTask task : tasks) {
                    prunedFiles.add(task.file());
                }
            }
            
            logger.info("Partition pruning for table {} reduced files from all to {} files", 
                tableId, prunedFiles.size());
            
            return prunedFiles;
            
        } catch (Exception e) {
            logger.error("Failed to perform partition pruning for table: {}", tableId, e);
            throw new RuntimeException("Partition pruning failed", e);
        }
    }
    
    /**
     * Estimates query cost using Iceberg statistics.
     */
    public QueryCostEstimate estimateQueryCost(TableIdentifier tableId, Expression filter) {
        try {
            // Get table statistics
            Optional<StatisticsCollectionService.QueryOptimizationHints> hints = 
                Optional.ofNullable(statisticsService.getOptimizationHints(tableId));
            
            if (!hints.isPresent() || !hints.get().hasStatistics()) {
                logger.warn("No statistics available for cost estimation of table: {}", tableId);
                return new QueryCostEstimate(-1, -1, -1);
            }
            
            long estimatedRows = hints.get().getEstimatedRowCount();
            long estimatedSize = hints.get().getEstimatedSize();
            
            // Apply filter selectivity (simplified estimation)
            double selectivity = estimateFilterSelectivity(filter);
            long filteredRows = (long) (estimatedRows * selectivity);
            long filteredSize = (long) (estimatedSize * selectivity);
            
            // Calculate cost (simplified cost model)
            long cost = filteredRows + (filteredSize / 1024); // Row cost + I/O cost
            
            logger.debug("Estimated query cost for table {}: {} rows, {} bytes, cost: {}", 
                tableId, filteredRows, filteredSize, cost);
            
            return new QueryCostEstimate(cost, filteredRows, filteredSize);
            
        } catch (Exception e) {
            logger.warn("Failed to estimate query cost for table: {}", tableId, e);
            return new QueryCostEstimate(-1, -1, -1);
        }
    }
    
    private double estimateFilterSelectivity(Expression filter) {
        if (filter == null) {
            return 1.0; // No filter, select all
        }
        
        // Simplified selectivity estimation
        // In a real implementation, this would analyze the filter expression
        // and use column statistics to estimate selectivity
        return 0.1; // Assume 10% selectivity for now
    }
    
    private void addStageDependencies(List<ExecutionStage> stages) {
        // Create dependencies between stages
        for (int i = 1; i < stages.size(); i++) {
            // Each stage depends on the previous stage (simplified)
            // In reality, this would analyze data flow dependencies
        }
    }
    
    /**
     * Visitor class to analyze RelNode trees and create Iceberg-optimized execution stages.
     */
    private class IcebergTableAnalyzer extends RelVisitor {
        
        private final List<ExecutionStage> stages = new ArrayList<>();
        private final Map<RelNode, Integer> nodeToStageMap = new HashMap<>();
        
        public List<ExecutionStage> getOptimizedStages() {
            return stages;
        }
        
        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            logger.debug("Analyzing RelNode for Iceberg optimization: {} (type: {})", 
                node.getRelTypeName(), node.getClass().getSimpleName());
            
            ExecutionStage stage = null;
            
            if (node instanceof TableScan) {
                stage = createIcebergScanStage((TableScan) node);
            } else {
                // For non-scan operations, create standard stages
                stage = createStandardStage(node);
            }
            
            if (stage != null) {
                stages.add(stage);
                nodeToStageMap.put(node, stage.getStageId());
            }
            
            // Continue visiting child nodes
            super.visit(node, ordinal, parent);
        }
        
        private ExecutionStage createIcebergScanStage(TableScan tableScan) {
            try {
                String tableName = String.join(".", tableScan.getTable().getQualifiedName());
                logger.debug("Creating Iceberg scan stage for table: {}", tableName);
                
                // Try to resolve as Iceberg table
                TableIdentifier tableId = TableIdentifier.of("default", tableName);
                
                if (!catalogService.tableExists(tableId)) {
                    logger.debug("Table {} is not an Iceberg table, creating standard scan stage", tableName);
                    return createStandardScanStage(tableScan);
                }
                
                // Load Iceberg table
                Table icebergTable = catalogService.loadTable(tableId);
                
                // Extract filter conditions from the query (simplified)
                Expression filter = extractFilterFromScan(tableScan);
                
                // Perform partition pruning
                List<DataFile> prunedFiles = prunePartitions(tableId, filter);
                
                // Get cost estimate
                QueryCostEstimate costEstimate = estimateQueryCost(tableId, filter);
                
                // Create partitions from pruned files
                List<CommonProto.DataPartition> partitions = createIcebergPartitions(
                    tableId, prunedFiles, costEstimate);
                
                // Create optimized scan stage
                ExecutionStage.Builder stageBuilder = ExecutionStage.newBuilder()
                    .setStageId(stageIdGenerator.incrementAndGet())
                    .setType(StageType.SCAN)
                    .addAllInputPartitions(partitions)
                    .setOutputPartitioning(createOptimalPartitioning(costEstimate))
                    .setSerializedPlan(serializeIcebergScan(icebergTable, filter))
                    .setPlanFormat("ICEBERG_SCAN");
                
                // Add Iceberg-specific metadata (simplified - metadata methods not available in current protobuf)
                // In a full implementation, these would be added to the protobuf definition
                
                ExecutionStage stage = stageBuilder.build();
                
                logger.info("Created Iceberg scan stage for table {} with {} pruned files (estimated {} rows)", 
                    tableName, prunedFiles.size(), costEstimate.getEstimatedRows());
                
                return stage;
                
            } catch (Exception e) {
                logger.warn("Failed to create Iceberg scan stage, falling back to standard scan", e);
                return createStandardScanStage(tableScan);
            }
        }
        
        private ExecutionStage createStandardScanStage(TableScan tableScan) {
            String tableName = String.join(".", tableScan.getTable().getQualifiedName());
            
            return ExecutionStage.newBuilder()
                .setStageId(stageIdGenerator.incrementAndGet())
                .setType(StageType.SCAN)
                .addInputPartitions(createStandardPartition(tableName))
                .setOutputPartitioning(createSinglePartitioning())
                .setSerializedPlan(serializeRelNode(tableScan))
                .setPlanFormat("CALCITE_JSON")

                .build();
        }
        
        private ExecutionStage createStandardStage(RelNode node) {
            return ExecutionStage.newBuilder()
                .setStageId(stageIdGenerator.incrementAndGet())
                .setType(getStageType(node))
                .setOutputPartitioning(createSinglePartitioning())
                .setSerializedPlan(serializeRelNode(node))
                .setPlanFormat("CALCITE_JSON")
                .build();
        }
        
        private StageType getStageType(RelNode node) {
            String nodeType = node.getClass().getSimpleName().toLowerCase();
            if (nodeType.contains("filter")) return StageType.FILTER;
            if (nodeType.contains("project")) return StageType.PROJECT;
            if (nodeType.contains("aggregate")) return StageType.AGGREGATE;
            if (nodeType.contains("join")) return StageType.JOIN;
            if (nodeType.contains("sort")) return StageType.SORT;
            return StageType.PROJECT; // Default
        }
        
        private Expression extractFilterFromScan(TableScan tableScan) {
            // In a real implementation, this would extract filter conditions
            // from the Calcite RelNode tree and convert them to Iceberg expressions
            // For now, return null (no filter)
            return null;
        }
        
        private List<CommonProto.DataPartition> createIcebergPartitions(
                TableIdentifier tableId, List<DataFile> files, QueryCostEstimate costEstimate) {
            
            List<CommonProto.DataPartition> partitions = new ArrayList<>();
            
            // Group files into partitions (simplified - one partition per file for now)
            for (int i = 0; i < files.size(); i++) {
                DataFile file = files.get(i);
                
                CommonProto.DataPartition partition = CommonProto.DataPartition.newBuilder()
                    .setPartitionId("iceberg-partition-" + i)
                    .setTableLocation(file.path().toString())
                    .addDataFiles(file.path().toString())
                    .setEstimatedRows(file.recordCount())
                    .setEstimatedBytes(file.fileSizeInBytes())

                    .build();
                
                partitions.add(partition);
            }
            
            // If no files (empty result), create empty partition
            if (partitions.isEmpty()) {
                partitions.add(CommonProto.DataPartition.newBuilder()
                    .setPartitionId("empty-partition")
                    .setTableLocation("")
                    .setEstimatedRows(0)
                    .setEstimatedBytes(0)
                    .build());
            }
            
            return partitions;
        }
        
        private PartitioningScheme createOptimalPartitioning(QueryCostEstimate costEstimate) {
            // Choose partitioning scheme based on cost estimate
            if (costEstimate.getEstimatedRows() > 1000000) {
                // Large result set - use hash partitioning
                return PartitioningScheme.newBuilder()
                    .setType(PartitionType.HASH)
                    .setPartitionCount(4)
                    .build();
            } else {
                // Small result set - use single partition
                return createSinglePartitioning();
            }
        }
        
        private PartitioningScheme createSinglePartitioning() {
            return PartitioningScheme.newBuilder()
                .setType(PartitionType.SINGLE)
                .setPartitionCount(1)
                .build();
        }
        
        private CommonProto.DataPartition createStandardPartition(String tableName) {
            return CommonProto.DataPartition.newBuilder()
                .setPartitionId("standard-partition-" + tableName)
                .setTableLocation("data/default/" + tableName)
                .addDataFiles(tableName + ".parquet")
                .setEstimatedRows(1000)
                .setEstimatedBytes(50000)
                .build();
        }
        
        private com.google.protobuf.ByteString serializeIcebergScan(Table table, Expression filter) {
            try {
                // Create a JSON representation of the Iceberg scan
                Map<String, Object> scanInfo = new HashMap<>();
                scanInfo.put("table_name", table.name());
                scanInfo.put("schema_id", table.schema().schemaId());
                scanInfo.put("snapshot_id", table.currentSnapshot() != null ? 
                    table.currentSnapshot().snapshotId() : null);
                scanInfo.put("filter", filter != null ? filter.toString() : null);
                
                String json = new com.fasterxml.jackson.databind.ObjectMapper()
                    .writeValueAsString(scanInfo);
                
                return com.google.protobuf.ByteString.copyFrom(json.getBytes("UTF-8"));
            } catch (Exception e) {
                logger.warn("Failed to serialize Iceberg scan, using simple representation", e);
                return com.google.protobuf.ByteString.copyFrom(
                    ("ICEBERG_SCAN:" + table.name()).getBytes());
            }
        }
        
        private com.google.protobuf.ByteString serializeRelNode(RelNode node) {
            try {
                String json = org.apache.calcite.plan.RelOptUtil.toString(node);
                return com.google.protobuf.ByteString.copyFrom(json.getBytes("UTF-8"));
            } catch (Exception e) {
                logger.warn("Failed to serialize RelNode, using simple representation", e);
                return com.google.protobuf.ByteString.copyFrom(node.toString().getBytes());
            }
        }
    }
    
    /**
     * Query cost estimate containing cost metrics.
     */
    public static class QueryCostEstimate {
        private final long cost;
        private final long estimatedRows;
        private final long estimatedSize;
        
        public QueryCostEstimate(long cost, long estimatedRows, long estimatedSize) {
            this.cost = cost;
            this.estimatedRows = estimatedRows;
            this.estimatedSize = estimatedSize;
        }
        
        public long getCost() {
            return cost;
        }
        
        public long getEstimatedRows() {
            return estimatedRows;
        }
        
        public long getEstimatedSize() {
            return estimatedSize;
        }
        
        public boolean hasValidEstimate() {
            return cost >= 0 && estimatedRows >= 0 && estimatedSize >= 0;
        }
    }
}