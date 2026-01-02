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

/**
 * Context provided to page processors during execution.
 * 
 * <p>This is the main interface for implementing batch processing logic.
 * Page processors receive this context and use it to:
 * <ul>
 *   <li>Access the current page being processed</li>
 *   <li>Enqueue the next page for pipelined processing</li>
 *   <li>Access batch configuration and arguments</li>
 *   <li>Report progress and metrics</li>
 * </ul>
 * </p>
 * 
 * <h3>Example Usage:</h3>
 * <pre>{@code
 * @BatchProcessor(name = "processUsers")
 * public class UserBatchProcessor implements PageProcessor {
 *     @Override
 *     public void process(BatchProcessorContext context) throws Exception {
 *         BatchPage page = context.getCurrentPage();
 *         UserCursor cursor = JsonUtils.fromJson(page.getCursorStr(), UserCursor.class);
 *         
 *         // Query users from database
 *         List<User> users = userRepository.findByIdGreaterThan(cursor.getLastId(), page.getSize());
 *         
 *         // IMPORTANT: Enqueue next page BEFORE processing
 *         if (users.size() == page.getSize()) {
 *             String nextCursor = JsonUtils.toJson(new UserCursor(users.get(users.size()-1).getId()));
 *             context.enqueueNextPage(BatchPage.of(nextCursor, page.getSize()));
 *         }
 *         
 *         // Process each user
 *         for (User user : users) {
 *             processUser(user);
 *             context.incrementItemsProcessed();
 *         }
 *     }
 * }
 * }</pre>
 */
public interface BatchProcessorContext {
    
    /**
     * Get the current page being processed.
     */
    BatchPage getCurrentPage();
    
    /**
     * Get the batch ID.
     */
    String getBatchId();
    
    /**
     * Get the page processor arguments as string (typically JSON).
     */
    String getArgsStr();
    
    /**
     * Get the page processor arguments parsed as a specific type.
     */
    <T> T getArgs(Class<T> clazz);
    
    /**
     * Enqueue the next page for processing.
     * 
     * <p><b>IMPORTANT:</b> Call this BEFORE processing the current page to enable
     * pipelined parallelism. This allows other workers to start on the next page
     * while this page is being processed.</p>
     * 
     * @param nextPage the next page to process
     * @throws IllegalStateException if called more than once per page
     */
    void enqueueNextPage(BatchPage nextPage);
    
    /**
     * Mark current page as the last page (no more pages to process).
     * 
     * <p>Call this instead of enqueueNextPage when there's no more data.</p>
     */
    void markAsLastPage();
    
    /**
     * Increment the count of processed items.
     * Used for progress tracking and throughput calculation.
     */
    void incrementItemsProcessed();
    
    /**
     * Increment items processed by a specific count.
     */
    void incrementItemsProcessed(int count);
    
    /**
     * Get the workflow ID of the batch orchestrator.
     */
    String getWorkflowId();
    
    /**
     * Get the workflow run ID.
     */
    String getWorkflowRunId();
    
    /**
     * Record an activity heartbeat to prevent timeout.
     */
    void heartbeat();
    
    /**
     * Record a heartbeat with details for resumption.
     */
    void heartbeat(Object details);
    
    /**
     * Get heartbeat details from a previous run (for resumption).
     */
    <T> T getHeartbeatDetails(Class<T> clazz);
    
    /**
     * Check if this is a retry of a previously failed page.
     */
    boolean isRetry();
    
    /**
     * Get the current retry attempt number (0 for first attempt).
     */
    int getAttemptNumber();
    
    /**
     * Check if the next page was already signaled in a previous attempt.
     */
    boolean wasNextPageAlreadySignaled();
}
