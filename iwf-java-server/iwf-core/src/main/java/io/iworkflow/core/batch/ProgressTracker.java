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
 * Interface for tracking batch progress.
 * 
 * <p>Implement this interface to receive periodic progress updates during
 * batch execution. Useful for:</p>
 * <ul>
 *   <li>Logging progress to external systems</li>
 *   <li>Updating dashboards</li>
 *   <li>Sending notifications</li>
 *   <li>Recording metrics</li>
 * </ul>
 * 
 * <h3>Example:</h3>
 * <pre>{@code
 * @BatchTracker(name = "slackProgressTracker")
 * public class SlackProgressTracker implements ProgressTracker {
 *     
 *     private final SlackClient slackClient;
 *     
 *     @Override
 *     public void onProgress(BatchProgress progress, String args) {
 *         if (progress.isFinished()) {
 *             slackClient.sendMessage(
 *                 "#batch-jobs",
 *                 String.format("Batch %s completed! Processed %d pages.",
 *                     progress.getBatchId(), progress.getCompletedPages())
 *             );
 *         }
 *     }
 * }
 * }</pre>
 */
public interface ProgressTracker {
    
    /**
     * Called periodically with progress updates.
     * 
     * @param progress current batch progress
     * @param args optional tracker arguments (JSON string)
     */
    void onProgress(BatchProgress progress, String args);
    
    /**
     * Called when the batch completes (success or failure).
     * 
     * @param progress final batch progress
     * @param args optional tracker arguments
     */
    default void onComplete(BatchProgress progress, String args) {
        onProgress(progress, args);
    }
    
    /**
     * Called when an error occurs during tracking.
     * Override to handle tracking errors.
     * 
     * @param error the error that occurred
     * @param progress progress at time of error
     */
    default void onError(Throwable error, BatchProgress progress) {
        // Default: ignore tracking errors
    }
}
