package com.minicloud.controlplane.planner;

import com.minicloud.controlplane.sql.ParsedQuery;
import com.minicloud.controlplane.sql.SqlParsingService;
import com.minicloud.proto.execution.QueryExecutionProto.*;
import com.minicloud.proto.common.CommonProto;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.RelVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Distributed query planner that converts Calcite RelNode trees into distributed execution plans.
 * Creates stage-based execution plans that can be distributed across multiple workers.
 */
@Component
public class DistributedQueryPlanner {
    
    private static final Logger logger = LoggerFactory.getLogger(DistributedQueryPlanner.class);
    
    private final AtomicInteger stageIdGenerator = new AtomicInteger(0);
    private final SqlParsingService sqlParsingService;
    private final IcebergQueryPlanner icebergQueryPlanner;
    
    @Autowired
    public DistributedQueryPlanner(SqlParsingService sqlParsingService, 
                                 IcebergQueryPlanner icebergQueryPlanner) {
        this.sqlParsingService = sqlParsingService;
        this.icebergQueryPlanner = icebergQueryPlanner;
    }
    
    /**
     * Create a distributed execution plan from a parsed query
     */
    public ExecutionPlan createExecutionPlan(String queryId, ParsedQuery parsedQuery) {
        logger.info("Creating distributed execution plan for query: {}", queryId);
        
        try {
            // Check if query involves Iceberg tables
            Set<String> icebergTables = sqlParsingService.identifyIcebergTables(parsedQuery.getOriginalSql());
            
            if (!icebergTables.isEmpty()) {
                logger.info("Query involves Iceberg tables: {}, using Iceberg-optimized planner", icebergTables);
                return icebergQueryPlanner.createOptimizedExecutionPlan(queryId, parsedQuery);
            } else {
                logger.info("Query uses standard tables, using standard planner");
                return createStandardExecutionPlan(queryId, parsedQuery);
            }
        } catch (Exception e) {
            logger.warn("Failed to create optimized execution plan, falling back to standard planner", e);
            return createStandardExecutionPlan(queryId, parsedQuery);
        }
    }
    
    /**
     * Create a standard execution plan for non-Iceberg queries
     */
    private ExecutionPlan createStandardExecutionPlan(String queryId, ParsedQuery parsedQuery) {
        RelNode relNode = parsedQuery.getRelNode();
        List<ExecutionStage> stages = new ArrayList<>();
        
        // Analyze the RelNode tree and create execution stages
        RelNodeAnalyzer analyzer = new RelNodeAnalyzer();
        analyzer.visit(relNode, 0, null);
        
        // Create stages based on the analysis
        stages.addAll(analyzer.getStages());
        
        // Add dependencies between stages
        addStageDependencies(stages);
        
        ExecutionPlan plan = new ExecutionPlan(queryId, parsedQuery.getOriginalSql(), stages);
        
        logger.info("Created standard execution plan with {} stages for query {}", stages.size(), queryId);
        return plan;
    }
    
    /**
     * Create a simple execution plan for single-table queries (fallback)
     */
    public ExecutionPlan createSimpleExecutionPlan(String queryId, String sql) {
        logger.info("Creating simple execution plan for query: {}", queryId);
        
        List<ExecutionStage> stages = new ArrayList<>();
        
        // Create a single scan stage for simple queries
        ExecutionStage scanStage = ExecutionStage.newBuilder()
                .setStageId(stageIdGenerator.incrementAndGet())
                .setType(StageType.SCAN)
                .addInputPartitions(createDefaultPartition())
                .setOutputPartitioning(createSinglePartitioning())
                .setSerializedPlan(com.google.protobuf.ByteString.copyFrom(sql.getBytes()))
                .setPlanFormat("SQL")
                .build();
        
        stages.add(scanStage);
        
        ExecutionPlan plan = new ExecutionPlan(queryId, sql, stages);
        
        logger.info("Created simple execution plan with {} stages for query {}", stages.size(), queryId);
        return plan;
    }
    
    /**
     * Add dependencies between execution stages
     */
    private void addStageDependencies(List<ExecutionStage> stages) {
        // For now, create a simple linear dependency chain
        // In a more sophisticated implementation, this would analyze data flow
        for (int i = 1; i < stages.size(); i++) {
            // Each stage depends on the previous stage
            // This is a simplification - real dependencies would be more complex
        }
    }
    
    /**
     * Create a default data partition for simple queries
     */
    private CommonProto.DataPartition createDefaultPartition() {
        return CommonProto.DataPartition.newBuilder()
                .setPartitionId("default-partition")
                .setTableLocation("data/default")
                .addDataFiles("bank_transactions")
                .setEstimatedRows(1000)
                .setEstimatedBytes(50000)
                .build();
    }
    
    /**
     * Create single partitioning scheme (no partitioning)
     */
    private PartitioningScheme createSinglePartitioning() {
        return PartitioningScheme.newBuilder()
                .setType(PartitionType.SINGLE)
                .setPartitionCount(1)
                .build();
    }
    
    /**
     * Create hash partitioning scheme
     */
    private PartitioningScheme createHashPartitioning(List<String> columns, int partitionCount) {
        return PartitioningScheme.newBuilder()
                .setType(PartitionType.HASH)
                .addAllPartitionColumns(columns)
                .setPartitionCount(partitionCount)
                .build();
    }
    
    /**
     * Visitor class to analyze RelNode trees and extract execution stages
     */
    private class RelNodeAnalyzer extends RelVisitor {
        
        private final List<ExecutionStage> stages = new ArrayList<>();
        private final Map<RelNode, Integer> nodeToStageMap = new HashMap<>();
        
        public List<ExecutionStage> getStages() {
            return stages;
        }
        
        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            logger.debug("Analyzing RelNode: {} (type: {})", node.getRelTypeName(), node.getClass().getSimpleName());
            
            ExecutionStage stage = null;
            
            if (node instanceof TableScan) {
                stage = createScanStage((TableScan) node);
            } else if (node instanceof Filter) {
                stage = createFilterStage((Filter) node);
            } else if (node instanceof Project) {
                stage = createProjectStage((Project) node);
            } else if (node instanceof Aggregate) {
                stage = createAggregateStage((Aggregate) node);
            } else if (node instanceof Join) {
                stage = createJoinStage((Join) node);
            } else if (node instanceof Sort) {
                stage = createSortStage((Sort) node);
            } else {
                // Generic stage for unsupported operations
                stage = createGenericStage(node);
            }
            
            if (stage != null) {
                stages.add(stage);
                nodeToStageMap.put(node, stage.getStageId());
            }
            
            // Continue visiting child nodes
            super.visit(node, ordinal, parent);
        }
        
        private ExecutionStage createScanStage(TableScan tableScan) {
            logger.debug("Creating scan stage for table: {}", tableScan.getTable().getQualifiedName());
            
            return ExecutionStage.newBuilder()
                    .setStageId(stageIdGenerator.incrementAndGet())
                    .setType(StageType.SCAN)
                    .addInputPartitions(createTablePartition(tableScan))
                    .setOutputPartitioning(createSinglePartitioning())
                    .setSerializedPlan(serializeRelNode(tableScan))
                    .setPlanFormat("CALCITE_JSON")
                    .build();
        }
        
        private ExecutionStage createFilterStage(Filter filter) {
            logger.debug("Creating filter stage");
            
            return ExecutionStage.newBuilder()
                    .setStageId(stageIdGenerator.incrementAndGet())
                    .setType(StageType.FILTER)
                    .setOutputPartitioning(createSinglePartitioning())
                    .setSerializedPlan(serializeRelNode(filter))
                    .setPlanFormat("CALCITE_JSON")
                    .build();
        }
        
        private ExecutionStage createProjectStage(Project project) {
            logger.debug("Creating project stage");
            
            return ExecutionStage.newBuilder()
                    .setStageId(stageIdGenerator.incrementAndGet())
                    .setType(StageType.PROJECT)
                    .setOutputPartitioning(createSinglePartitioning())
                    .setSerializedPlan(serializeRelNode(project))
                    .setPlanFormat("CALCITE_JSON")
                    .build();
        }
        
        private ExecutionStage createAggregateStage(Aggregate aggregate) {
            logger.debug("Creating aggregate stage");
            
            // For aggregates, we might want hash partitioning on group keys
            List<String> groupKeys = new ArrayList<>();
            for (int groupKey : aggregate.getGroupSet()) {
                groupKeys.add("col_" + groupKey); // Simplified column naming
            }
            
            PartitioningScheme partitioning = groupKeys.isEmpty() ? 
                    createSinglePartitioning() : 
                    createHashPartitioning(groupKeys, 4); // Default to 4 partitions
            
            return ExecutionStage.newBuilder()
                    .setStageId(stageIdGenerator.incrementAndGet())
                    .setType(StageType.AGGREGATE)
                    .setOutputPartitioning(partitioning)
                    .setSerializedPlan(serializeRelNode(aggregate))
                    .setPlanFormat("CALCITE_JSON")
                    .build();
        }
        
        private ExecutionStage createJoinStage(Join join) {
            logger.debug("Creating join stage");
            
            // Joins typically require hash partitioning on join keys
            return ExecutionStage.newBuilder()
                    .setStageId(stageIdGenerator.incrementAndGet())
                    .setType(StageType.JOIN)
                    .setOutputPartitioning(createHashPartitioning(List.of("join_key"), 4))
                    .setSerializedPlan(serializeRelNode(join))
                    .setPlanFormat("CALCITE_JSON")
                    .build();
        }
        
        private ExecutionStage createSortStage(Sort sort) {
            logger.debug("Creating sort stage");
            
            return ExecutionStage.newBuilder()
                    .setStageId(stageIdGenerator.incrementAndGet())
                    .setType(StageType.SORT)
                    .setOutputPartitioning(createSinglePartitioning()) // Sort typically produces single partition
                    .setSerializedPlan(serializeRelNode(sort))
                    .setPlanFormat("CALCITE_JSON")
                    .build();
        }
        
        private ExecutionStage createGenericStage(RelNode node) {
            logger.debug("Creating generic stage for: {}", node.getRelTypeName());
            
            return ExecutionStage.newBuilder()
                    .setStageId(stageIdGenerator.incrementAndGet())
                    .setType(StageType.PROJECT) // Default to project for unknown operations
                    .setOutputPartitioning(createSinglePartitioning())
                    .setSerializedPlan(serializeRelNode(node))
                    .setPlanFormat("CALCITE_JSON")
                    .build();
        }
        
        private CommonProto.DataPartition createTablePartition(TableScan tableScan) {
            String tableName = String.join(".", tableScan.getTable().getQualifiedName());
            
            return CommonProto.DataPartition.newBuilder()
                    .setPartitionId("partition-" + tableName)
                    .setTableLocation("data/default/" + tableName)
                    .addDataFiles(tableName + ".parquet")
                    .setEstimatedRows(1000) // TODO: Get actual statistics
                    .setEstimatedBytes(50000) // TODO: Get actual statistics
                    .build();
        }
        
        private com.google.protobuf.ByteString serializeRelNode(RelNode node) {
            // For now, serialize as JSON string
            // In a production system, this might use Substrait or other formats
            try {
                String json = org.apache.calcite.plan.RelOptUtil.toString(node);
                return com.google.protobuf.ByteString.copyFrom(json.getBytes("UTF-8"));
            } catch (Exception e) {
                logger.warn("Failed to serialize RelNode, using simple representation", e);
                return com.google.protobuf.ByteString.copyFrom(node.toString().getBytes());
            }
        }
    }
}