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
 * Interface for implementing page processors in batch jobs.
 * 
 * <p>Page processors handle the actual work of processing a page of data.
 * They must follow the pipelined pagination pattern:</p>
 * 
 * <ol>
 *   <li>Read the current page data</li>
 *   <li>Determine and enqueue the next page cursor BEFORE processing</li>
 *   <li>Process each item in the current page</li>
 * </ol>
 * 
 * <h3>Important Guidelines:</h3>
 * <ul>
 *   <li>Operations should be idempotent for retry safety</li>
 *   <li>Call enqueueNextPage() before processing to enable parallelism</li>
 *   <li>Keep page processing under 5 minutes</li>
 *   <li>Use heartbeats for long operations to prevent timeouts</li>
 * </ul>
 * 
 * <h3>Example:</h3>
 * <pre>{@code
 * @BatchProcessor(name = "migrateUsers", retryMode = RetryMode.AT_LEAST_ONCE)
 * public class UserMigrationProcessor implements PageProcessor {
 *     
 *     private final UserRepository userRepo;
 *     private final NewUserService newService;
 *     
 *     @Override
 *     public void process(BatchProcessorContext context) throws Exception {
 *         BatchPage page = context.getCurrentPage();
 *         
 *         // Parse cursor (could be JSON, offset, key, etc.)
 *         long lastUserId = page.getCursorStr().isEmpty() ? 0 : Long.parseLong(page.getCursorStr());
 *         
 *         // Fetch page of users
 *         List<User> users = userRepo.findUsersAfterId(lastUserId, page.getSize());
 *         
 *         // Enqueue next page FIRST (enables parallelism)
 *         if (users.size() == page.getSize()) {
 *             long nextId = users.get(users.size() - 1).getId();
 *             context.enqueueNextPage(BatchPage.of(String.valueOf(nextId), page.getSize()));
 *         } else {
 *             context.markAsLastPage();
 *         }
 *         
 *         // Process each user
 *         for (User user : users) {
 *             context.heartbeat(); // Prevent timeout
 *             newService.migrateUser(user);
 *             context.incrementItemsProcessed();
 *         }
 *     }
 * }
 * }</pre>
 */
public interface PageProcessor {
    
    /**
     * Retry mode for page processing.
     */
    enum RetryMode {
        /**
         * Page may be processed multiple times on failure.
         * Operations should be idempotent.
         * Recommended for most use cases.
         */
        AT_LEAST_ONCE,
        
        /**
         * Page will not be retried on failure.
         * Use when operations are not idempotent and failures are acceptable.
         */
        AT_MOST_ONCE
    }
    
    /**
     * Process a page of data.
     * 
     * @param context the batch processor context
     * @throws Exception if processing fails
     */
    void process(BatchProcessorContext context) throws Exception;
    
    /**
     * Get the retry mode for this processor.
     * Override to customize retry behavior.
     */
    default RetryMode getRetryMode() {
        return RetryMode.AT_LEAST_ONCE;
    }
    
    /**
     * Get the maximum number of initial retry attempts.
     * Override to customize retry behavior.
     */
    default int getMaxInitialRetries() {
        return 10;
    }
    
    /**
     * Whether to use extended retries for stuck pages.
     * Extended retries occur after initial retries are exhausted,
     * at a slower interval, until success or workflow timeout.
     */
    default boolean useExtendedRetries() {
        return getRetryMode() == RetryMode.AT_LEAST_ONCE;
    }
    
    /**
     * Get the interval between extended retries in seconds.
     */
    default int getExtendedRetryIntervalSeconds() {
        return 300; // 5 minutes
    }
    
    /**
     * Get non-retryable error types.
     * Exceptions of these types will fail immediately without retry.
     */
    default Class<? extends Throwable>[] getNonRetryableErrors() {
        return new Class[0];
    }
}
