package com.minicloud.worker.grpc;

import com.minicloud.worker.service.WorkerRegistrationService;

import com.minicloud.proto.execution.QueryExecutionServiceGrpc;
import com.minicloud.proto.execution.QueryExecutionProto.*;
import com.minicloud.proto.common.CommonProto;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * gRPC implementation of QueryExecutionService for distributed query execution.
 * This service handles query stage execution, progress reporting, and health monitoring.
 */
@GrpcService
public class QueryExecutionServiceImpl extends QueryExecutionServiceGrpc.QueryExecutionServiceImplBase {
    
    private static final Logger logger = LoggerFactory.getLogger(QueryExecutionServiceImpl.class);
    
    @Autowired
    private WorkerRegistrationService workerRegistrationService;
    

    
    private final ConcurrentHashMap<String, QueryExecution> activeQueries = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    
    @Override
    public void executeStage(ExecuteStageRequest request, StreamObserver<ExecuteStageResponse> responseObserver) {
        String queryId = request.getQueryId();
        int stageId = request.getStageId();
        
        logger.info("Executing stage {} for query {}", stageId, queryId);
        
        try {
            // Create query execution tracking
            QueryExecution execution = new QueryExecution(queryId, stageId);
            activeQueries.put(queryId, execution);
            
            // Update active query count for health monitoring
            workerRegistrationService.incrementActiveQueries();
            
            // Execute stage asynchronously
            CompletableFuture.supplyAsync(() -> {
                try {
                    return executeStageInternal(request);
                } catch (Exception e) {
                    logger.error("Error executing stage {} for query {}", stageId, queryId, e);
                    throw new RuntimeException(e);
                }
            }).thenAccept(response -> {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                activeQueries.remove(queryId);
                workerRegistrationService.decrementActiveQueries();
            }).exceptionally(throwable -> {
                logger.error("Stage execution failed for query {}, stage {}", queryId, stageId, throwable);
                ExecuteStageResponse errorResponse = ExecuteStageResponse.newBuilder()
                        .setQueryId(queryId)
                        .setStageId(stageId)
                        .setStatus(ExecutionStatus.FAILED)
                        .setErrorMessage(throwable.getMessage())
                        .setTraceId(request.getTraceId())
                        .build();
                responseObserver.onNext(errorResponse);
                responseObserver.onCompleted();
                activeQueries.remove(queryId);
                workerRegistrationService.decrementActiveQueries();
                return null;
            });
            
        } catch (Exception e) {
            logger.error("Error starting stage execution for query {}, stage {}", queryId, stageId, e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void streamProgress(ExecuteStageRequest request, StreamObserver<ProgressUpdate> responseObserver) {
        String queryId = request.getQueryId();
        int stageId = request.getStageId();
        
        logger.info("Starting progress stream for query {}, stage {}", queryId, stageId);
        
        // Schedule periodic progress updates
        scheduler.scheduleAtFixedRate(() -> {
            try {
                QueryExecution execution = activeQueries.get(queryId);
                if (execution != null) {
                    ProgressUpdate update = ProgressUpdate.newBuilder()
                            .setQueryId(queryId)
                            .setStageId(stageId)
                            .setStatus(execution.getStatus())
                            .setProgressPercentage(execution.getProgressPercentage())
                            .setCurrentStats(execution.getStats())
                            .build();
                    
                    responseObserver.onNext(update);
                } else {
                    // Query completed or cancelled
                    responseObserver.onCompleted();
                }
            } catch (Exception e) {
                logger.error("Error sending progress update for query {}", queryId, e);
                responseObserver.onError(e);
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    @Override
    public void reportHealth(HealthRequest request, StreamObserver<HealthResponse> responseObserver) {
        logger.debug("Health check requested by worker {}", request.getWorkerId());
        
        try {
            // Create worker info with current resource usage
            CommonProto.ResourceInfo resourceInfo = CommonProto.ResourceInfo.newBuilder()
                    .setCpuCores(Runtime.getRuntime().availableProcessors())
                    .setMemoryMb(Runtime.getRuntime().maxMemory() / (1024 * 1024))
                    .setActiveQueries(activeQueries.size())
                    .setCpuUtilization(0.5) // TODO: Get actual CPU utilization
                    .setMemoryUtilization(0.3) // TODO: Get actual memory utilization
                    .build();
            
            CommonProto.WorkerInfo workerInfo = CommonProto.WorkerInfo.newBuilder()
                    .setWorkerId(request.getWorkerId())
                    .setEndpoint("localhost:9090") // TODO: Get actual endpoint
                    .setStatus(CommonProto.WorkerStatus.HEALTHY)
                    .setResources(resourceInfo)
                    .build();
            
            HealthResponse response = HealthResponse.newBuilder()
                    .setWorkerInfo(workerInfo)
                    .setResponse(createSuccessResponse())
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            logger.error("Error reporting health for worker {}", request.getWorkerId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void cancelQuery(CancelQueryRequest request, StreamObserver<CancelQueryResponse> responseObserver) {
        String queryId = request.getQueryId();
        logger.info("Cancelling query {}, reason: {}", queryId, request.getReason());
        
        try {
            QueryExecution execution = activeQueries.get(queryId);
            boolean cancelled = false;
            
            if (execution != null) {
                execution.cancel();
                activeQueries.remove(queryId);
                workerRegistrationService.decrementActiveQueries();
                cancelled = true;
            }
            
            CancelQueryResponse response = CancelQueryResponse.newBuilder()
                    .setQueryId(queryId)
                    .setCancelled(cancelled)
                    .setResponse(createSuccessResponse())
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            logger.error("Error cancelling query {}", queryId, e);
            responseObserver.onError(e);
        }
    }

    private ExecuteStageResponse executeStageInternal(ExecuteStageRequest request) {
        String queryId = request.getQueryId();
        int stageId = request.getStageId();
        ExecutionStage stage = request.getStage();
        
        logger.info("Executing stage type: {} for query {}", stage.getType(), queryId);
        
        long startTime = System.currentTimeMillis();
        long rowsProcessed = 0;
        long bytesProcessed = 0;
        
        try {
            // Check if this is an Iceberg scan stage
            if (stage.getType() == StageType.SCAN && isIcebergStage(stage)) {
                logger.info("Executing Iceberg scan stage for query {}", queryId);
                
                // Execute Iceberg scan (simplified implementation)
                VectorSchemaRoot result = executeIcebergScan(stage, queryId, stageId);
                
                if (result != null) {
                    rowsProcessed = result.getRowCount();
                    bytesProcessed = estimateDataSize(result);
                    
                    // Store result for later retrieval
                    storeQueryResult(queryId, stageId, result);
                    
                    logger.info("Iceberg scan completed: {} rows, {} bytes", rowsProcessed, bytesProcessed);
                } else {
                    // Simulate successful Iceberg execution
                    rowsProcessed = 1000;
                    bytesProcessed = 50000;
                    logger.info("Iceberg scan simulated: {} rows, {} bytes", rowsProcessed, bytesProcessed);
                }
            } else {
                // Execute standard stage
                return executeStandardStage(request, startTime);
            }
            
        } catch (Exception e) {
            logger.error("Error executing stage {} for query {}", stageId, queryId, e);
            throw new RuntimeException("Stage execution failed", e);
        }
        
        long executionTime = System.currentTimeMillis() - startTime;
        
        // Create execution stats
        CommonProto.ExecutionStats stats = CommonProto.ExecutionStats.newBuilder()
                .setRowsProcessed(rowsProcessed)
                .setBytesProcessed(bytesProcessed)
                .setExecutionTimeMs(executionTime)
                .setCpuTimeMs((long) (executionTime * 0.8)) // Estimate CPU time
                .setMemoryPeakMb(estimateMemoryUsage(rowsProcessed))
                .build();
        
        return ExecuteStageResponse.newBuilder()
                .setQueryId(queryId)
                .setStageId(stageId)
                .setStatus(ExecutionStatus.COMPLETED)
                .setResultLocation("flight://localhost:8815/results/" + queryId + "/" + stageId)
                .setStats(stats)
                .setTraceId(request.getTraceId())
                .build();
    }
    
    private ExecuteStageResponse executeStandardStage(ExecuteStageRequest request, long startTime) {
        String queryId = request.getQueryId();
        int stageId = request.getStageId();
        
        // Standard execution with simulated delay
        try {
            Thread.sleep(1000); // Simulate processing time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Stage execution interrupted", e);
        }
        
        long executionTime = System.currentTimeMillis() - startTime;
        
        // Create mock execution stats
        CommonProto.ExecutionStats stats = CommonProto.ExecutionStats.newBuilder()
                .setRowsProcessed(1000)
                .setBytesProcessed(50000)
                .setExecutionTimeMs(executionTime)
                .setCpuTimeMs((long) (executionTime * 0.8))
                .setMemoryPeakMb(100)
                .build();
        
        return ExecuteStageResponse.newBuilder()
                .setQueryId(queryId)
                .setStageId(stageId)
                .setStatus(ExecutionStatus.COMPLETED)
                .setResultLocation("flight://localhost:8815/results/" + queryId + "/" + stageId)
                .setStats(stats)
                .setTraceId(request.getTraceId())
                .build();
    }
    
    private boolean isIcebergStage(ExecutionStage stage) {
        // Check if stage indicates Iceberg table format
        return "ICEBERG_SCAN".equals(stage.getPlanFormat());
    }
    
    private VectorSchemaRoot executeIcebergScan(ExecutionStage stage, String queryId, int stageId) {
        try {
            logger.debug("Executing Iceberg scan for stage {}", stageId);
            
            // Extract Iceberg scan information from stage (simplified)
            logger.info("Iceberg scan stage detected");
            
            // For now, simulate Iceberg data reading
            // In a full implementation, this would:
            // 1. Parse the serialized Iceberg scan plan
            // 2. Load the Iceberg table
            // 3. Read the specified data files
            // 4. Apply filters and projections
            // 5. Convert to Arrow format
            
            // Simulate successful Iceberg scan
            Thread.sleep(500); // Simulate I/O time
            
            // Return null for now - in real implementation, would return VectorSchemaRoot
            return null;
            
        } catch (Exception e) {
            logger.error("Failed to execute Iceberg scan", e);
            throw new RuntimeException("Iceberg scan execution failed", e);
        }
    }
    
    private void storeQueryResult(String queryId, int stageId, VectorSchemaRoot result) {
        // Store the result for later retrieval
        // In a full implementation, this would serialize the Arrow data
        // and store it in a location accessible by the control plane
        logger.debug("Storing query result for query {}, stage {} ({} rows)", 
            queryId, stageId, result.getRowCount());
    }
    
    private long estimateDataSize(VectorSchemaRoot result) {
        // Estimate the size of Arrow data in bytes
        // This is a simplified estimation
        return result.getRowCount() * result.getSchema().getFields().size() * 8; // 8 bytes per field average
    }
    
    private long estimateMemoryUsage(long rowsProcessed) {
        // Estimate memory usage in MB based on rows processed
        return Math.max(100, rowsProcessed / 10000); // Minimum 100MB, 1MB per 10k rows
    }

    private CommonProto.StandardResponse createSuccessResponse() {
        return CommonProto.StandardResponse.newBuilder()
                .setStatus(CommonProto.ResponseStatus.SUCCESS)
                .setRequestId(UUID.randomUUID().toString())
                .build();
    }
    
    /**
     * Internal class to track query execution state
     */
    private static class QueryExecution {
        private final String queryId;
        private final int stageId;
        private volatile ExecutionStatus status = ExecutionStatus.RUNNING;
        private volatile double progressPercentage = 0.0;
        private volatile boolean cancelled = false;
        
        public QueryExecution(String queryId, int stageId) {
            this.queryId = queryId;
            this.stageId = stageId;
        }
        
        public ExecutionStatus getStatus() {
            return cancelled ? ExecutionStatus.CANCELLED : status;
        }
        
        public double getProgressPercentage() {
            return progressPercentage;
        }
        
        public CommonProto.ExecutionStats getStats() {
            return CommonProto.ExecutionStats.newBuilder()
                    .setRowsProcessed((long) (progressPercentage * 1000))
                    .setBytesProcessed((long) (progressPercentage * 50000))
                    .setExecutionTimeMs(System.currentTimeMillis() % 10000)
                    .build();
        }
        
        public void cancel() {
            this.cancelled = true;
        }
    }
}