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

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Batch Orchestrator Workflow for processing large datasets with controlled parallelism.
 * 
 * <p>This workflow implements the "pipelined pagination" pattern for efficient
 * parallel processing of large datasets. Key features:</p>
 * 
 * <ul>
 *   <li><b>Controlled Parallelism:</b> Process multiple pages concurrently with configurable limits</li>
 *   <li><b>Fair Pagination:</b> Pages are evenly sized, no need to pre-compute boundaries</li>
 *   <li><b>Single Scan:</b> Data is scanned only once, not pre-indexed</li>
 *   <li><b>Fault Tolerant:</b> Automatic retries with extended retry for stuck pages</li>
 *   <li><b>Progress Tracking:</b> Query progress at any time</li>
 *   <li><b>Dynamic Control:</b> Pause, resume, and adjust parallelism at runtime</li>
 * </ul>
 * 
 * <h3>How Pipelined Pagination Works:</h3>
 * <pre>
 * Traditional parallel:
 *   1. Pre-scan data to find boundaries (expensive)
 *   2. Fan out to workers
 *   3. Join results
 * 
 * Pipelined:
 *   1. Start first page
 *   2. Page processor determines next cursor, signals orchestrator
 *   3. Orchestrator starts next page immediately (parallel)
 *   4. Repeat until done
 * 
 * Benefits:
 *   - No pre-scan needed
 *   - Even page sizes
 *   - Maximum parallelism achieved quickly
 * </pre>
 * 
 * <h3>Usage Example:</h3>
 * <pre>{@code
 * // Start a batch job
 * BatchOrchestratorWorkflow batch = client.newWorkflowStub(
 *     BatchOrchestratorWorkflow.class,
 *     WorkflowOptions.newBuilder()
 *         .setWorkflowId("batch-" + UUID.randomUUID())
 *         .setTaskQueue("batch-queue")
 *         .build()
 * );
 * 
 * BatchConfig config = BatchConfig.builder()
 *     .batchId("migrate-users-2024")
 *     .pageProcessorName("userMigration")
 *     .maxParallelism(10)
 *     .pageSize(100)
 *     .build();
 * 
 * // Start async
 * WorkflowClient.start(batch::run, config);
 * 
 * // Query progress
 * BatchProgress progress = batch.getProgress();
 * System.out.println("Completed: " + progress.getCompletedPages());
 * 
 * // Pause if needed
 * batch.setMaxParallelism(0);
 * 
 * // Resume
 * batch.restoreParallelism();
 * }</pre>
 */
@WorkflowInterface
public interface BatchOrchestratorWorkflow {
    
    /**
     * Main workflow method that orchestrates the batch processing.
     * 
     * @param config batch configuration
     * @return final progress summary
     */
    @WorkflowMethod
    BatchProgress run(BatchConfig config);
    
    /**
     * Query the current progress of the batch.
     */
    @QueryMethod
    BatchProgress getProgress();
    
    /**
     * Signal to set maximum parallelism.
     * Set to 0 to pause processing, restore with restoreParallelism().
     * 
     * @param maxParallelism new maximum parallelism value
     */
    @SignalMethod
    void setMaxParallelism(int maxParallelism);
    
    /**
     * Signal to restore the previous maximum parallelism value.
     * Used after pausing (setMaxParallelism(0)).
     */
    @SignalMethod
    void restoreParallelism();
    
    /**
     * Signal to add a new page to the processing queue.
     * Called internally by page processors to enqueue the next page.
     * 
     * @param page the page to add
     */
    @SignalMethod
    void addPage(BatchPage page);
    
    /**
     * Signal to cancel the batch job.
     * 
     * @param reason reason for cancellation
     */
    @SignalMethod
    void cancel(String reason);
}
