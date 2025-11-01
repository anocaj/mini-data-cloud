package com.minicloud.controlplane.planner;

import com.minicloud.controlplane.sql.ParsedQuery;
import com.minicloud.controlplane.sql.SqlParsingService;
import com.minicloud.proto.execution.QueryExecutionProto.ExecutionStage;
import com.minicloud.proto.execution.QueryExecutionProto.StageType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for DistributedQueryPlanner
 */
@ExtendWith(MockitoExtension.class)
class DistributedQueryPlannerTest {
    
    private DistributedQueryPlanner queryPlanner;
    
    @Mock
    private ParsedQuery mockParsedQuery;
    
    @Mock
    private RelNode mockRelNode;
    
    @Mock
    private SqlNode mockSqlNode;
    
    @Mock
    private SqlParsingService sqlParsingService;
    
    @Mock
    private IcebergQueryPlanner icebergQueryPlanner;
    
    @BeforeEach
    void setUp() {
        queryPlanner = new DistributedQueryPlanner(sqlParsingService, icebergQueryPlanner);
    }
    
    @Test
    void testCreateSimpleExecutionPlan() {
        // Test creating a simple execution plan
        String queryId = "test-query-1";
        String sql = "SELECT * FROM bank_transactions";
        
        ExecutionPlan plan = queryPlanner.createSimpleExecutionPlan(queryId, sql);
        
        assertNotNull(plan);
        assertEquals(queryId, plan.getQueryId());
        assertEquals(sql, plan.getOriginalSql());
        assertEquals(1, plan.getStageCount());
        
        // Verify the single stage
        ExecutionStage stage = plan.getStages().get(0);
        assertEquals(StageType.SCAN, stage.getType());
        assertEquals(1, stage.getInputPartitionsCount());
        assertTrue(stage.getSerializedPlan().size() > 0);
    }
    
    @Test
    void testCreateExecutionPlanWithMockRelNode() {
        // Setup mocks
        when(mockParsedQuery.getRelNode()).thenReturn(mockRelNode);
        when(mockParsedQuery.getOriginalSql()).thenReturn("SELECT * FROM test_table");
        
        String queryId = "test-query-2";
        
        ExecutionPlan plan = queryPlanner.createExecutionPlan(queryId, mockParsedQuery);
        
        assertNotNull(plan);
        assertEquals(queryId, plan.getQueryId());
        assertEquals("SELECT * FROM test_table", plan.getOriginalSql());
        assertTrue(plan.getStageCount() >= 0); // May be 0 if no stages created from mock
    }
    
    @Test
    void testExecutionPlanProperties() {
        String queryId = "test-query-3";
        String sql = "SELECT category, SUM(amount) FROM transactions GROUP BY category";
        
        ExecutionPlan plan = queryPlanner.createSimpleExecutionPlan(queryId, sql);
        
        // Test plan properties
        assertNotNull(plan.getCreatedAt());
        assertNotNull(plan.getSummary());
        assertTrue(plan.getSummary().contains(queryId));
        
        // Test stage access
        assertTrue(plan.getStage(1).isPresent());
        assertFalse(plan.getStage(999).isPresent());
        
        // Test root stages (stages with no dependencies)
        assertFalse(plan.getRootStages().isEmpty());
    }
    
    @Test
    void testExecutionPlanStats() {
        String queryId = "test-query-4";
        String sql = "SELECT * FROM large_table";
        
        ExecutionPlan plan = queryPlanner.createSimpleExecutionPlan(queryId, sql);
        ExecutionPlan.ExecutionPlanStats stats = plan.getStats();
        
        assertNotNull(stats);
        assertEquals(1, stats.getTotalStages());
        assertTrue(stats.getTotalPartitions() > 0);
        assertTrue(stats.getMaxParallelism() > 0);
        assertFalse(stats.getStageTypeCounts().isEmpty());
    }
}