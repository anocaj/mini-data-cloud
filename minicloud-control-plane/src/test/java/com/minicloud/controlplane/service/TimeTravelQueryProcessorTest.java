package com.minicloud.controlplane.service;

import com.minicloud.controlplane.service.TimeTravelQueryProcessor.TimeTravelQueryResult;
import com.minicloud.controlplane.service.TimeTravelQueryProcessor.TimeTravelQueryException;
import com.minicloud.controlplane.sql.SqlParsingService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for TimeTravelQueryProcessor.
 */
class TimeTravelQueryProcessorTest {
    
    @Mock
    private GlobalCatalogService globalCatalogService;
    
    @Mock
    private SqlParsingService sqlParsingService;
    
    @Mock
    private SnapshotManagementService snapshotManagementService;
    
    private TimeTravelQueryProcessor timeTravelQueryProcessor;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        timeTravelQueryProcessor = new TimeTravelQueryProcessor(
            globalCatalogService, sqlParsingService, snapshotManagementService);
    }
    
    @Test
    void testIsTimeTravelQuery_WithTimestampSyntax() {
        String sql = "SELECT * FROM taxi_trips AS OF TIMESTAMP '2024-01-01 12:00:00'";
        assertTrue(timeTravelQueryProcessor.isTimeTravelQuery(sql));
    }
    
    @Test
    void testIsTimeTravelQuery_WithSnapshotSyntax() {
        String sql = "SELECT * FROM taxi_trips AS OF SNAPSHOT 1234567890";
        assertTrue(timeTravelQueryProcessor.isTimeTravelQuery(sql));
    }
    
    @Test
    void testIsTimeTravelQuery_WithoutTimeTravelSyntax() {
        String sql = "SELECT * FROM taxi_trips WHERE pickup_date > '2024-01-01'";
        assertFalse(timeTravelQueryProcessor.isTimeTravelQuery(sql));
    }
    
    @Test
    void testIsTimeTravelQuery_CaseInsensitive() {
        String sql = "select * from taxi_trips as of timestamp '2024-01-01 12:00:00'";
        assertTrue(timeTravelQueryProcessor.isTimeTravelQuery(sql));
    }
    
    @Test
    void testProcessTimeTravelQuery_ThrowsExceptionForNonTimeTravelQuery() {
        String sql = "SELECT * FROM taxi_trips";
        
        assertThrows(TimeTravelQueryException.class, () -> {
            timeTravelQueryProcessor.processTimeTravelQuery(sql);
        });
    }
}