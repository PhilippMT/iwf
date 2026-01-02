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
import java.time.Duration;

/**
 * Configuration for batch orchestration.
 * 
 * <p>Controls parallelism, retry behavior, and progress tracking.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BatchConfig implements Serializable {
    
    /**
     * Unique identifier for this batch job.
     */
    private String batchId;
    
    /**
     * Name of the page processor to use.
     */
    private String pageProcessorName;
    
    /**
     * Maximum number of pages to process in parallel.
     * Setting to 0 or 1 makes processing sequential.
     */
    @Builder.Default
    private int maxParallelism = 10;
    
    /**
     * Size of each page (number of items).
     */
    @Builder.Default
    private int pageSize = 100;
    
    /**
     * Initial cursor for the first page. Empty string for start.
     */
    @Builder.Default
    private String firstCursor = "";
    
    /**
     * Timeout for processing a single page.
     */
    @Builder.Default
    private Duration pageTimeout = Duration.ofMinutes(5);
    
    /**
     * Maximum attempts for initial retry before moving to extended retry.
     */
    @Builder.Default
    private int maxInitialRetries = 10;
    
    /**
     * Interval between extended retries.
     */
    @Builder.Default
    private Duration extendedRetryInterval = Duration.ofMinutes(5);
    
    /**
     * Whether to use extended retries for stuck pages.
     */
    @Builder.Default
    private boolean useExtendedRetries = true;
    
    /**
     * Number of pages to process before continue-as-new.
     * Helps manage workflow history size.
     */
    @Builder.Default
    private int pagesPerRun = 500;
    
    /**
     * Optional arguments passed to page processor.
     */
    private String pageProcessorArgs;
    
    /**
     * Custom tracker name for progress reporting.
     */
    private String trackerName;
    
    /**
     * Custom tracker arguments.
     */
    private String trackerArgs;
    
    /**
     * Polling interval for progress tracking.
     */
    @Builder.Default
    private Duration trackerPollingInterval = Duration.ofSeconds(30);
    
    /**
     * Whether to enable dynamic parallelism adjustment based on throughput.
     */
    @Builder.Default
    private boolean enableDynamicParallelism = false;
    
    /**
     * Target throughput (items/second) for dynamic parallelism.
     */
    @Builder.Default
    private double targetThroughput = 100.0;
    
    /**
     * Create a simple batch config with defaults.
     */
    public static BatchConfig simple(String batchId, String pageProcessorName, int maxParallelism, int pageSize) {
        return BatchConfig.builder()
                .batchId(batchId)
                .pageProcessorName(pageProcessorName)
                .maxParallelism(maxParallelism)
                .pageSize(pageSize)
                .build();
    }
}
