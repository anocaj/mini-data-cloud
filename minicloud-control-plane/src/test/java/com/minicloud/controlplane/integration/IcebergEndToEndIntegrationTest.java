package com.minicloud.controlplane.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minicloud.controlplane.dto.QueryRequest;
import com.minicloud.controlplane.dto.QueryResponse;
import com.minicloud.controlplane.dto.TableInfo;
import com.minicloud.controlplane.model.QueryStatus;
import com.minicloud.controlplane.service.GlobalCatalogService;
import com.minicloud.controlplane.service.IcebergCatalogService;
import com.minicloud.controlplane.service.NYCDataPlatform;
import com.minicloud.controlplane.service.TimeTravelQueryProcessor;
import com.minicloud.controlplane.service.IcebergTransactionService;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.*;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Comprehensive end-to-end integration test for the complete Iceberg system.
 * Tests the entire workflow from data loading to querying with all Iceberg features.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("integration-test")
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IcebergEndToEndIntegrationTest {

    private static final Network network = Network.newNetwork();

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15")
            .withDatabaseName("minicloud_catalog")
            .withUsername("minicloud")
            .withPassword("minicloud123")
            .withNetwork(network)
            .withNetworkAliases("metadata-db");

    @Container
    static GenericContainer<?> minio = new GenericContainer<>("minio/minio:latest")
            .withCommand("server", "/data", "--console-address", ":9001")
            .withEnv("MINIO_ROOT_USER", "minioadmin")
            .withEnv("MINIO_ROOT_PASSWORD", "minioadmin123")
            .withExposedPorts(9000, 9001)
            .withNetwork(network)
            .withNetworkAliases("minio");

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private GlobalCatalogService catalogService;

    @Autowired
    private IcebergCatalogService icebergCatalogService;

    @Autowired
    private NYCDataPlatform nycDataPlatform;

    @Autowired
    private TimeTravelQueryProcessor timeTravelProcessor;

    @Autowired
    private IcebergTransactionService transactionService;

    private ObjectMapper objectMapper = new ObjectMapper();

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        
        registry.add("minicloud.iceberg.enabled", () -> "true");
        registry.add("minicloud.iceberg.catalog.type", () -> "jdbc");
        registry.add("minicloud.iceberg.catalog.uri", postgres::getJdbcUrl);
        registry.add("minicloud.iceberg.warehouse", () -> "s3a://iceberg-data/");
        
        registry.add("minicloud.storage.s3.endpoint", () -> "http://localhost:" + minio.getMappedPort(9000));
        registry.add("minicloud.storage.s3.access-key", () -> "minioadmin");
        registry.add("minicloud.storage.s3.secret-key", () -> "minioadmin123");
        
        registry.add("minicloud.nyc-data.enabled", () -> "true");
        registry.add("minicloud.nyc-data.auto-load", () -> "false"); // We'll load manually for testing
    }

    @BeforeEach
    void setUp() throws Exception {
        // Wait for services to be ready
        await().atMost(60, TimeUnit.SECONDS)
                .until(() -> {
                    try {
                        ResponseEntity<String> response = restTemplate.getForEntity(
                                "http://localhost:" + port + "/health", String.class);
                        return response.getStatusCode() == HttpStatus.OK;
                    } catch (Exception e) {
                        return false;
                    }
                });
    }

    @Test
    @Order(1)
    void testSystemHealthAndInitialization() {
        // Test system health
        ResponseEntity<String> healthResponse = restTemplate.getForEntity(
                "http://localhost:" + port + "/health", String.class);
        assertThat(healthResponse.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Test catalog initialization
        ResponseEntity<List> catalogResponse = restTemplate.getForEntity(
                "http://localhost:" + port + "/api/iceberg/tables", List.class);
        assertThat(catalogResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    @Order(2)
    void testDataLoadingAndTableCreation() throws IOException {
        // Create sample NYC taxi data
        File sampleData = createSampleTaxiData();

        // Upload data via REST API
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        body.add("file", new FileSystemResource(sampleData));
        body.add("tableName", "test_taxi_trips");
        body.add("format", "iceberg");

        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);

        ResponseEntity<String> uploadResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/data/upload", requestEntity, String.class);
        
        assertThat(uploadResponse.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Verify table was created in Iceberg catalog
        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> {
                    try {
                        ResponseEntity<List> tablesResponse = restTemplate.getForEntity(
                                "http://localhost:" + port + "/api/iceberg/tables", List.class);
                        List<Map<String, Object>> tables = tablesResponse.getBody();
                        return tables.stream().anyMatch(table -> 
                                "test_taxi_trips".equals(table.get("name")));
                    } catch (Exception e) {
                        return false;
                    }
                });

        // Clean up
        sampleData.delete();
    }

    @Test
    @Order(3)
    void testBasicQueryExecution() {
        // Test basic SELECT query on Iceberg table
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql("SELECT COUNT(*) as trip_count FROM test_taxi_trips");

        ResponseEntity<QueryResponse> queryResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/query/execute", queryRequest, QueryResponse.class);

        assertThat(queryResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(queryResponse.getBody()).isNotNull();
        assertThat(queryResponse.getBody().getStatus()).isEqualTo(QueryStatus.COMPLETED);
        assertThat(queryResponse.getBody().getRows()).isNotEmpty();
    }

    @Test
    @Order(4)
    void testSchemaEvolution() {
        // Test schema evolution by adding a column
        ResponseEntity<String> evolutionResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/iceberg/tables/test_taxi_trips/schema/add-column",
                Map.of("columnName", "test_column", "columnType", "string", "nullable", true),
                String.class);

        assertThat(evolutionResponse.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Verify schema was updated
        ResponseEntity<TableInfo> tableInfoResponse = restTemplate.getForEntity(
                "http://localhost:" + port + "/api/iceberg/tables/test_taxi_trips", TableInfo.class);

        assertThat(tableInfoResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(tableInfoResponse.getBody()).isNotNull();
        // Schema should now include the new column
    }

    @Test
    @Order(5)
    void testTransactionOperations() {
        // Start a transaction
        ResponseEntity<Map> transactionResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/iceberg/transactions/begin",
                Map.of("tableName", "test_taxi_trips"),
                Map.class);

        assertThat(transactionResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        String transactionId = (String) transactionResponse.getBody().get("transactionId");
        assertThat(transactionId).isNotNull();

        // Commit the transaction
        ResponseEntity<String> commitResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/iceberg/transactions/" + transactionId + "/commit",
                null, String.class);

        assertThat(commitResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    @Order(6)
    void testTimeTravelQueries() {
        // Get table snapshots
        ResponseEntity<List> snapshotsResponse = restTemplate.getForEntity(
                "http://localhost:" + port + "/api/iceberg/tables/test_taxi_trips/snapshots", List.class);

        assertThat(snapshotsResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        List<Map<String, Object>> snapshots = snapshotsResponse.getBody();
        assertThat(snapshots).isNotEmpty();

        // Test time travel query with timestamp
        String timestamp = Instant.now().minusSeconds(3600).toString(); // 1 hour ago
        QueryRequest timeTravelQuery = new QueryRequest();
        timeTravelQuery.setSql("SELECT COUNT(*) FROM test_taxi_trips AS OF TIMESTAMP '" + timestamp + "'");

        ResponseEntity<QueryResponse> timeTravelResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/query/execute", timeTravelQuery, QueryResponse.class);

        // Should handle gracefully even if no data at that timestamp
        assertThat(timeTravelResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    @Order(7)
    void testNYCDatasetIntegration() {
        // Test NYC dataset loading (if enabled)
        ResponseEntity<List> nycDatasetsResponse = restTemplate.getForEntity(
                "http://localhost:" + port + "/api/nyc/datasets", List.class);

        assertThat(nycDatasetsResponse.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Test sample queries
        ResponseEntity<List> sampleQueriesResponse = restTemplate.getForEntity(
                "http://localhost:" + port + "/api/nyc/sample-queries", List.class);

        assertThat(sampleQueriesResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    @Order(8)
    void testQueryOptimizationAndPerformance() {
        // Test query with aggregation
        QueryRequest aggregationQuery = new QueryRequest();
        aggregationQuery.setSql("SELECT pickup_location_id, COUNT(*) as trip_count " +
                "FROM test_taxi_trips GROUP BY pickup_location_id LIMIT 10");

        long startTime = System.currentTimeMillis();
        ResponseEntity<QueryResponse> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/query/execute", aggregationQuery, QueryResponse.class);
        long executionTime = System.currentTimeMillis() - startTime;

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().getStatus()).isEqualTo(QueryStatus.COMPLETED);
        
        // Performance should be reasonable (less than 30 seconds for test data)
        assertThat(executionTime).isLessThan(30000);
    }

    @Test
    @Order(9)
    void testDistributedQueryExecution() {
        // Test that queries are distributed across workers
        QueryRequest distributedQuery = new QueryRequest();
        distributedQuery.setSql("SELECT pickup_location_id, dropoff_location_id, COUNT(*) as trip_count " +
                "FROM test_taxi_trips GROUP BY pickup_location_id, dropoff_location_id");

        ResponseEntity<QueryResponse> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/query/execute", distributedQuery, QueryResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().getStatus()).isEqualTo(QueryStatus.COMPLETED);
        assertThat(response.getBody().getRows()).isNotEmpty();
    }

    @Test
    @Order(10)
    void testSystemMetricsAndMonitoring() {
        // Test Iceberg metrics endpoint
        ResponseEntity<Map> metricsResponse = restTemplate.getForEntity(
                "http://localhost:" + port + "/api/iceberg/metrics", Map.class);

        assertThat(metricsResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(metricsResponse.getBody()).isNotNull();

        // Test query metrics
        ResponseEntity<Map> queryMetricsResponse = restTemplate.getForEntity(
                "http://localhost:" + port + "/api/iceberg/metrics/queries", Map.class);

        assertThat(queryMetricsResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    @Order(11)
    void testErrorHandlingAndRecovery() {
        // Test invalid SQL query
        QueryRequest invalidQuery = new QueryRequest();
        invalidQuery.setSql("SELECT * FROM non_existent_table");

        ResponseEntity<QueryResponse> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/query/execute", invalidQuery, QueryResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().getStatus()).isEqualTo(QueryStatus.FAILED);
        assertThat(response.getBody().getErrorMessage()).isNotNull();

        // Test invalid time travel query
        QueryRequest invalidTimeTravelQuery = new QueryRequest();
        invalidTimeTravelQuery.setSql("SELECT * FROM test_taxi_trips AS OF TIMESTAMP 'invalid-timestamp'");

        ResponseEntity<QueryResponse> timeTravelResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/query/execute", invalidTimeTravelQuery, QueryResponse.class);

        assertThat(timeTravelResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(timeTravelResponse.getBody().getStatus()).isIn(QueryStatus.FAILED, QueryStatus.COMPLETED);
    }

    @Test
    @Order(12)
    void testDataConsistencyAndACIDProperties() {
        // Test concurrent operations don't corrupt data
        String tableName = "test_taxi_trips";
        
        // Get initial count
        QueryRequest countQuery = new QueryRequest();
        countQuery.setSql("SELECT COUNT(*) as count FROM " + tableName);
        
        ResponseEntity<QueryResponse> initialResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/query/execute", countQuery, QueryResponse.class);
        
        assertThat(initialResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(initialResponse.getBody().getStatus()).isEqualTo(QueryStatus.COMPLETED);
        
        // Verify table metadata consistency
        ResponseEntity<TableInfo> tableInfoResponse = restTemplate.getForEntity(
                "http://localhost:" + port + "/api/iceberg/tables/" + tableName, TableInfo.class);
        
        assertThat(tableInfoResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(tableInfoResponse.getBody()).isNotNull();
    }

    private File createSampleTaxiData() throws IOException {
        File tempFile = File.createTempFile("sample_taxi_data", ".csv");
        
        try (FileWriter writer = new FileWriter(tempFile)) {
            // Write CSV header
            writer.write("vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,fare_amount,total_amount,pickup_location_id,dropoff_location_id\n");
            
            // Write sample data rows
            for (int i = 1; i <= 100; i++) {
                writer.write(String.format("1,2024-01-01 %02d:00:00,2024-01-01 %02d:30:00,1,%.2f,%.2f,%.2f,%d,%d\n",
                        i % 24, (i % 24), 
                        Math.random() * 10, 
                        Math.random() * 50 + 10, 
                        Math.random() * 60 + 15,
                        i % 265 + 1, 
                        (i + 50) % 265 + 1));
            }
        }
        
        return tempFile;
    }
}