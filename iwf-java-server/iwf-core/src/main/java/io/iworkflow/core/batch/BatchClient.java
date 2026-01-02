/*
 * Copyright (c) 2024 iWF Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.iworkflow.core.batch;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Client for starting and managing batch jobs.
 * 
 * <p>Provides convenient methods for:</p>
 * <ul>
 *   <li>Starting new batch jobs</li>
 *   <li>Querying batch progress</li>
 *   <li>Pausing and resuming batches</li>
 *   <li>Cancelling batch jobs</li>
 * </ul>
 * 
 * <h3>Usage Example:</h3>
 * <pre>{@code
 * @Autowired
 * BatchClient batchClient;
 * 
 * // Start a batch job
 * BatchConfig config = BatchConfig.builder()
 *     .batchId("user-migration-2024")
 *     .pageProcessorName("migrateUsers")
 *     .maxParallelism(10)
 *     .pageSize(100)
 *     .build();
 * 
 * String workflowId = batchClient.startBatch(config);
 * 
 * // Check progress
 * BatchProgress progress = batchClient.getProgress(workflowId);
 * System.out.println("Completed: " + progress.getCompletedPages());
 * 
 * // Pause if system is under load
 * batchClient.pause(workflowId);
 * 
 * // Resume when ready
 * batchClient.resume(workflowId);
 * 
 * // Wait for completion
 * BatchProgress result = batchClient.waitForCompletion(workflowId);
 * }</pre>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BatchClient {
    
    private static final String DEFAULT_TASK_QUEUE = "batch-orchestrator-queue";
    
    private final WorkflowClient workflowClient;
    
    /**
     * Start a new batch job.
     * 
     * @param config batch configuration
     * @return workflow ID of the batch job
     */
    public String startBatch(BatchConfig config) {
        return startBatch(config, DEFAULT_TASK_QUEUE);
    }
    
    /**
     * Start a new batch job on a specific task queue.
     * 
     * @param config batch configuration
     * @param taskQueue task queue name
     * @return workflow ID of the batch job
     */
    public String startBatch(BatchConfig config, String taskQueue) {
        String workflowId = "batch-" + config.getBatchId() + "-" + UUID.randomUUID();
        return startBatch(config, taskQueue, workflowId);
    }
    
    /**
     * Start a new batch job with specific workflow ID.
     * 
     * @param config batch configuration
     * @param taskQueue task queue name
     * @param workflowId specific workflow ID to use
     * @return workflow ID
     */
    public String startBatch(BatchConfig config, String taskQueue, String workflowId) {
        log.info("Starting batch job {} with ID {}", config.getBatchId(), workflowId);
        
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(taskQueue)
                .setWorkflowExecutionTimeout(Duration.ofHours(24)) // Default 24h timeout
                .build();
        
        BatchOrchestratorWorkflow workflow = workflowClient.newWorkflowStub(
                BatchOrchestratorWorkflow.class, 
                options
        );
        
        WorkflowClient.start(workflow::run, config);
        
        return workflowId;
    }
    
    /**
     * Get the current progress of a batch job.
     * 
     * @param workflowId workflow ID of the batch job
     * @return current progress
     */
    public BatchProgress getProgress(String workflowId) {
        BatchOrchestratorWorkflow workflow = workflowClient.newWorkflowStub(
                BatchOrchestratorWorkflow.class,
                workflowId
        );
        return workflow.getProgress();
    }
    
    /**
     * Pause a batch job by setting parallelism to 0.
     * 
     * @param workflowId workflow ID of the batch job
     */
    public void pause(String workflowId) {
        log.info("Pausing batch job {}", workflowId);
        BatchOrchestratorWorkflow workflow = workflowClient.newWorkflowStub(
                BatchOrchestratorWorkflow.class,
                workflowId
        );
        workflow.setMaxParallelism(0);
    }
    
    /**
     * Resume a paused batch job.
     * 
     * @param workflowId workflow ID of the batch job
     */
    public void resume(String workflowId) {
        log.info("Resuming batch job {}", workflowId);
        BatchOrchestratorWorkflow workflow = workflowClient.newWorkflowStub(
                BatchOrchestratorWorkflow.class,
                workflowId
        );
        workflow.restoreParallelism();
    }
    
    /**
     * Set the parallelism of a batch job.
     * 
     * @param workflowId workflow ID of the batch job
     * @param parallelism new parallelism value
     */
    public void setParallelism(String workflowId, int parallelism) {
        log.info("Setting parallelism for batch {} to {}", workflowId, parallelism);
        BatchOrchestratorWorkflow workflow = workflowClient.newWorkflowStub(
                BatchOrchestratorWorkflow.class,
                workflowId
        );
        workflow.setMaxParallelism(parallelism);
    }
    
    /**
     * Cancel a batch job.
     * 
     * @param workflowId workflow ID of the batch job
     * @param reason reason for cancellation
     */
    public void cancel(String workflowId, String reason) {
        log.info("Cancelling batch job {}: {}", workflowId, reason);
        BatchOrchestratorWorkflow workflow = workflowClient.newWorkflowStub(
                BatchOrchestratorWorkflow.class,
                workflowId
        );
        workflow.cancel(reason);
    }
    
    /**
     * Wait for a batch job to complete.
     * 
     * @param workflowId workflow ID of the batch job
     * @return final progress
     */
    public BatchProgress waitForCompletion(String workflowId) {
        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(
                workflowId,
                Optional.empty(),
                Optional.empty()
        );
        return stub.getResult(BatchProgress.class);
    }
    
    /**
     * Wait for a batch job to complete asynchronously.
     * 
     * @param workflowId workflow ID of the batch job
     * @return completable future with final progress
     */
    public CompletableFuture<BatchProgress> waitForCompletionAsync(String workflowId) {
        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(
                workflowId,
                Optional.empty(),
                Optional.empty()
        );
        return stub.getResultAsync(BatchProgress.class);
    }
    
    /**
     * Terminate a batch job immediately.
     * 
     * @param workflowId workflow ID of the batch job
     * @param reason reason for termination
     */
    public void terminate(String workflowId, String reason) {
        log.warn("Terminating batch job {}: {}", workflowId, reason);
        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(
                workflowId,
                Optional.empty(),
                Optional.empty()
        );
        stub.terminate(reason);
    }
}
