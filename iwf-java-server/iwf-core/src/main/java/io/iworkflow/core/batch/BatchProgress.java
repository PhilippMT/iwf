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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.Set;

/**
 * Progress information for a batch job.
 * 
 * <p>Can be queried at any time during batch execution to monitor progress.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BatchProgress implements Serializable {
    
    /**
     * Batch identifier.
     */
    private String batchId;
    
    /**
     * Total number of pages completed successfully.
     */
    private int completedPages;
    
    /**
     * Number of pages currently being processed.
     */
    private int processingPages;
    
    /**
     * Number of pages waiting in queue.
     */
    private int pendingPages;
    
    /**
     * Number of pages that are stuck (failed initial retries).
     */
    private int stuckPages;
    
    /**
     * Number of pages that permanently failed.
     */
    private int failedPages;
    
    /**
     * Maximum parallelism achieved during execution.
     */
    private int maxParallelismAchieved;
    
    /**
     * Current parallelism setting.
     */
    private int currentParallelism;
    
    /**
     * Total items processed (if tracked).
     */
    private long itemsProcessed;
    
    /**
     * Timestamp when batch started.
     */
    private Instant startTime;
    
    /**
     * Estimated completion time (if determinable).
     */
    private Instant estimatedCompletionTime;
    
    /**
     * Whether the batch is finished.
     */
    private boolean finished;
    
    /**
     * Page numbers that are stuck.
     */
    private Set<Integer> stuckPageNumbers;
    
    /**
     * Page numbers that failed permanently.
     */
    private Set<Integer> failedPageNumbers;
    
    /**
     * Current throughput (items/second).
     */
    private double throughput;
    
    /**
     * Calculate completion percentage.
     */
    public double getCompletionPercentage() {
        int total = completedPages + processingPages + pendingPages + stuckPages + failedPages;
        if (total == 0) return 0.0;
        return (completedPages * 100.0) / total;
    }
    
    /**
     * Get elapsed time in seconds since start.
     */
    public long getElapsedSeconds() {
        if (startTime == null) return 0;
        return Instant.now().getEpochSecond() - startTime.getEpochSecond();
    }
    
    /**
     * Check if there are any issues.
     */
    public boolean hasIssues() {
        return stuckPages > 0 || failedPages > 0;
    }
}
