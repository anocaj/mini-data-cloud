package com.minicloud.controlplane.integration;

import com.minicloud.controlplane.dto.QueryRequest;
import com.minicloud.controlplane.dto.QueryResponse;
import com.minicloud.controlplane.model.QueryStatus;
import com.minicloud.controlplane.service.NYCDataPlatform;
import com.minicloud.controlplane.service.IcebergQueryMetricsService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Performance test for NYC datasets with Iceberg integration.
 * Tests query performance, scalability, and system behavior under load.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("integration-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class NYCDatasetPerformanceTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private NYCDataPlatform nycDataPlatform;

    @Autowired
    private IcebergQueryMetricsService metricsService;

    private static final String BASE_URL = "http://localhost:";
    private static final long PERFORMANCE_THRESHOLD_MS = 30000; // 30 seconds max for test queries

    @BeforeEach
    void setUp() {
        // Wait for system to be ready
        await().atMost(60, TimeUnit.SECONDS)
                .until(() -> {
                    try {
                        ResponseEntity<String> response = restTemplate.getForEntity(
                                BASE_URL + port + "/health", String.class);
                        return response.getStatusCode().is2xxSuccessful();
                    } catch (Exception e) {
                        return false;
                    }
                });
    }

    @Test
    @Order(1)
    void testBasicQueryPerformance() {
        List<QueryPerformanceResult> results = new ArrayList<>();

        // Test simple count queries
        String[] countQueries = {
                "SELECT COUNT(*) FROM test_taxi_trips",
                "SELECT COUNT(*) FROM test_service_requests"
        };

        for (String sql : countQueries) {
            QueryPerformanceResult result = executeAndMeasureQuery(sql);
            results.add(result);
            
            assertThat(result.isSuccess()).isTrue();
            assertThat(result.getExecutionTimeMs()).isLessThan(PERFORMANCE_THRESHOLD_MS);
        }

        logPerformanceResults("Basic Count Queries", results);
    }

    @Test
    @Order(2)
    void testAggregationQueryPerformance() {
        List<QueryPerformanceResult> results = new ArrayList<>();

        // Test aggregation queries
        String[] aggregationQueries = {
                "SELECT pickup_location_id, COUNT(*) as trip_count FROM test_taxi_trips GROUP BY pickup_location_id LIMIT 10",
                "SELECT dropoff_location_id, AVG(fare_amount) as avg_fare FROM test_taxi_trips GROUP BY dropoff_location_id LIMIT 10",
                "SELECT agency, COUNT(*) as request_count FROM test_service_requests GROUP BY agency",
                "SELECT borough, complaint_type, COUNT(*) as count FROM test_service_requests GROUP BY borough, complaint_type LIMIT 20"
        };

        for (String sql : aggregationQueries) {
            QueryPerformanceResult result = executeAndMeasureQuery(sql);
            results.add(result);
            
            assertThat(result.isSuccess()).isTrue();
            assertThat(result.getExecutionTimeMs()).isLessThan(PERFORMANCE_THRESHOLD_MS);
        }

        logPerformanceResults("Aggregation Queries", results);
    }

    @Test
    @Order(3)
    void testComplexAnalyticalQueries() {
        List<QueryPerformanceResult> results = new ArrayList<>();

        // Test complex analytical queries
        String[] complexQueries = {
                "SELECT pickup_location_id, dropoff_location_id, COUNT(*) as trips, AVG(fare_amount) as avg_fare, SUM(trip_distance) as total_distance FROM test_taxi_trips GROUP BY pickup_location_id, dropoff_location_id HAVING COUNT(*) > 1 ORDER BY trips DESC LIMIT 10",
                "SELECT borough, complaint_type, COUNT(*) as complaints, COUNT(CASE WHEN status = 'Closed' THEN 1 END) as closed_complaints FROM test_service_requests GROUP BY borough, complaint_type HAVING COUNT(*) > 1 ORDER BY complaints DESC LIMIT 15"
        };

        for (String sql : complexQueries) {
            QueryPerformanceResult result = executeAndMeasureQuery(sql);
            results.add(result);
            
            assertThat(result.isSuccess()).isTrue();
            // Allow more time for complex queries
            assertThat(result.getExecutionTimeMs()).isLessThan(PERFORMANCE_THRESHOLD_MS * 2);
        }

        logPerformanceResults("Complex Analytical Queries", results);
    }

    @Test
    @Order(4)
    void testTimeTravelQueryPerformance() {
        List<QueryPerformanceResult> results = new ArrayList<>();

        // Test time travel queries (may not have historical data, but test performance)
        String[] timeTravelQueries = {
                "SELECT COUNT(*) FROM test_taxi_trips AS OF TIMESTAMP '2024-01-01 12:00:00'",
                "SELECT COUNT(*) FROM test_service_requests AS OF TIMESTAMP '2024-01-01 12:00:00'"
        };

        for (String sql : timeTravelQueries) {
            QueryPerformanceResult result = executeAndMeasureQuery(sql);
            results.add(result);
            
            // Time travel queries should handle gracefully even without historical data
            assertThat(result.getExecutionTimeMs()).isLessThan(PERFORMANCE_THRESHOLD_MS);
        }

        logPerformanceResults("Time Travel Queries", results);
    }

    @Test
    @Order(5)
    void testConcurrentQueryPerformance() {
        // Test concurrent query execution
        List<Thread> queryThreads = new ArrayList<>();
        List<QueryPerformanceResult> results = new ArrayList<>();

        String testQuery = "SELECT pickup_location_id, COUNT(*) FROM test_taxi_trips GROUP BY pickup_location_id LIMIT 5";

        // Execute 5 concurrent queries
        for (int i = 0; i < 5; i++) {
            Thread queryThread = new Thread(() -> {
                QueryPerformanceResult result = executeAndMeasureQuery(testQuery);
                synchronized (results) {
                    results.add(result);
                }
            });
            queryThreads.add(queryThread);
            queryThread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : queryThreads) {
            try {
                thread.join(PERFORMANCE_THRESHOLD_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        assertThat(results).hasSize(5);
        for (QueryPerformanceResult result : results) {
            assertThat(result.isSuccess()).isTrue();
            assertThat(result.getExecutionTimeMs()).isLessThan(PERFORMANCE_THRESHOLD_MS);
        }

        logPerformanceResults("Concurrent Queries", results);
    }

    @Test
    @Order(6)
    void testQueryOptimizationEffectiveness() {
        // Test that Iceberg optimizations are working
        List<QueryPerformanceResult> results = new ArrayList<>();

        // Queries that should benefit from partition pruning and metadata optimization
        String[] optimizedQueries = {
                "SELECT COUNT(*) FROM test_taxi_trips WHERE pickup_location_id = 161",
                "SELECT AVG(fare_amount) FROM test_taxi_trips WHERE pickup_location_id IN (161, 237, 48)",
                "SELECT COUNT(*) FROM test_service_requests WHERE borough = 'MANHATTAN'",
                "SELECT * FROM test_service_requests WHERE agency = 'NYPD' LIMIT 10"
        };

        for (String sql : optimizedQueries) {
            QueryPerformanceResult result = executeAndMeasureQuery(sql);
            results.add(result);
            
            assertThat(result.isSuccess()).isTrue();
            // These queries should be fast due to optimizations
            assertThat(result.getExecutionTimeMs()).isLessThan(PERFORMANCE_THRESHOLD_MS / 2);
        }

        logPerformanceResults("Optimization Test Queries", results);
    }

    @Test
    @Order(7)
    void testSystemResourceUtilization() {
        // Test system behavior under sustained load
        long startTime = System.currentTimeMillis();
        List<QueryPerformanceResult> results = new ArrayList<>();

        // Execute a series of queries to test sustained performance
        for (int i = 0; i < 10; i++) {
            String query = "SELECT pickup_location_id, COUNT(*) as trips FROM test_taxi_trips GROUP BY pickup_location_id LIMIT 5";
            QueryPerformanceResult result = executeAndMeasureQuery(query);
            results.add(result);
            
            assertThat(result.isSuccess()).isTrue();
        }

        long totalTime = System.currentTimeMillis() - startTime;
        
        // Calculate average performance
        double avgExecutionTime = results.stream()
                .mapToLong(QueryPerformanceResult::getExecutionTimeMs)
                .average()
                .orElse(0.0);

        System.out.println("Sustained Load Test Results:");
        System.out.println("Total queries: " + results.size());
        System.out.println("Total time: " + totalTime + "ms");
        System.out.println("Average query time: " + avgExecutionTime + "ms");
        
        // Performance should remain consistent
        assertThat(avgExecutionTime).isLessThan(PERFORMANCE_THRESHOLD_MS);
    }

    private QueryPerformanceResult executeAndMeasureQuery(String sql) {
        QueryRequest request = new QueryRequest();
        request.setSql(sql);

        long startTime = System.currentTimeMillis();
        
        try {
            ResponseEntity<QueryResponse> response = restTemplate.postForEntity(
                    BASE_URL + port + "/api/query/execute", request, QueryResponse.class);
            
            long executionTime = System.currentTimeMillis() - startTime;
            
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                boolean success = response.getBody().getStatus() == QueryStatus.COMPLETED;
                return new QueryPerformanceResult(
                        sql, 
                        executionTime, 
                        success,
                        response.getBody().getRows() != null ? response.getBody().getRows().size() : 0,
                        response.getBody().getErrorMessage()
                );
            } else {
                return new QueryPerformanceResult(sql, executionTime, false, 0, "HTTP error: " + response.getStatusCode());
            }
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            return new QueryPerformanceResult(sql, executionTime, false, 0, e.getMessage());
        }
    }

    private void logPerformanceResults(String testName, List<QueryPerformanceResult> results) {
        System.out.println("\n" + testName + " Performance Results:");
        System.out.println("=" + "=".repeat(testName.length() + 20));
        
        for (QueryPerformanceResult result : results) {
            System.out.printf("Query: %s%n", result.getSql().substring(0, Math.min(50, result.getSql().length())) + "...");
            System.out.printf("  Execution Time: %dms%n", result.getExecutionTimeMs());
            System.out.printf("  Success: %s%n", result.isSuccess());
            System.out.printf("  Rows Returned: %d%n", result.getRowCount());
            if (!result.isSuccess()) {
                System.out.printf("  Error: %s%n", result.getErrorMessage());
            }
            System.out.println();
        }
        
        double avgTime = results.stream()
                .mapToLong(QueryPerformanceResult::getExecutionTimeMs)
                .average()
                .orElse(0.0);
        
        System.out.printf("Average Execution Time: %.2fms%n", avgTime);
        System.out.println();
    }

    private static class QueryPerformanceResult {
        private final String sql;
        private final long executionTimeMs;
        private final boolean success;
        private final int rowCount;
        private final String errorMessage;

        public QueryPerformanceResult(String sql, long executionTimeMs, boolean success, int rowCount, String errorMessage) {
            this.sql = sql;
            this.executionTimeMs = executionTimeMs;
            this.success = success;
            this.rowCount = rowCount;
            this.errorMessage = errorMessage;
        }

        public String getSql() { return sql; }
        public long getExecutionTimeMs() { return executionTimeMs; }
        public boolean isSuccess() { return success; }
        public int getRowCount() { return rowCount; }
        public String getErrorMessage() { return errorMessage; }
    }
}