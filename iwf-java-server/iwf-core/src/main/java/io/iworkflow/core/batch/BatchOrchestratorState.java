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
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * State for continue-as-new to preserve batch processing progress.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BatchOrchestratorState implements Serializable {
    
    /**
     * Page states keyed by page number.
     */
    private Map<Integer, PageStateData> pages;
    
    /**
     * List of pending page numbers.
     */
    private List<Integer> pendingPages;
    
    /**
     * Set of stuck page numbers.
     */
    private Set<Integer> stuckPages;
    
    /**
     * Set of failed page numbers.
     */
    private Set<Integer> failedPages;
    
    /**
     * Total pages ever enqueued.
     */
    private int pagesEnqueued;
    
    /**
     * Total pages completed.
     */
    private int pagesCompleted;
    
    /**
     * Total items processed.
     */
    private long itemsProcessed;
    
    /**
     * Current max parallelism.
     */
    private int maxParallelism;
    
    /**
     * Maximum parallelism achieved.
     */
    private int maxParallelismAchieved;
    
    /**
     * Batch start time.
     */
    private Instant startTime;
    
    /**
     * Whether in extended retry phase.
     */
    private boolean inExtendedRetryPhase;
    
    /**
     * Serializable page state data.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PageStateData implements Serializable {
        private BatchPage page;
        private boolean stuck;
        private boolean completed;
        private boolean failed;
        private String failureReason;
    }
}
