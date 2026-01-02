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

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.failure.CanceledFailure;
import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of the Batch Orchestrator Workflow.
 * 
 * <p>Uses pipelined pagination pattern for efficient parallel processing:
 * <ol>
 *   <li>Start with first page</li>
 *   <li>Page processor signals next page cursor</li>
 *   <li>Orchestrator immediately starts next page (up to max parallelism)</li>
 *   <li>Continue until all pages complete</li>
 * </ol>
 * </p>
 */
public class BatchOrchestratorWorkflowImpl implements BatchOrchestratorWorkflow {
    
    private static final Logger logger = Workflow.getLogger(BatchOrchestratorWorkflowImpl.class);
    
    // Configuration
    private BatchConfig config;
    
    // Page tracking
    private final Map<Integer, PageState> pages = new ConcurrentHashMap<>();
    private final List<Integer> pendingPages = Collections.synchronizedList(new ArrayList<>());
    private final Set<Integer> processingPages = ConcurrentHashMap.newKeySet();
    private final Set<Integer> stuckPages = ConcurrentHashMap.newKeySet();
    private final Set<Integer> failedPages = ConcurrentHashMap.newKeySet();
    
    // Counters
    private final AtomicInteger pagesEnqueued = new AtomicInteger(0);
    private final AtomicInteger pagesCompleted = new AtomicInteger(0);
    private final AtomicInteger pagesInThisRun = new AtomicInteger(0);
    private long itemsProcessed = 0;
    
    // Parallelism control
    private int maxParallelism;
    private final Stack<Integer> previousParallelisms = new Stack<>();
    private int maxParallelismAchieved = 0;
    
    // State
    private Instant startTime;
    private boolean finished = false;
    private boolean cancelled = false;
    private String cancellationReason;
    private boolean inExtendedRetryPhase = false;
    
    // Activity stub
    private BatchPageActivity pageActivity;
    
    @Override
    public BatchProgress run(BatchConfig config) {
        this.config = config;
        this.maxParallelism = config.getMaxParallelism();
        this.startTime = Instant.ofEpochMilli(Workflow.currentTimeMillis());
        
        // Create activity stub
        this.pageActivity = Workflow.newActivityStub(
                BatchPageActivity.class,
                buildActivityOptions(config, false)
        );
        
        logger.info("Starting batch {} with parallelism {}", config.getBatchId(), maxParallelism);
        
        // Start with first page
        BatchPage firstPage = BatchPage.builder()
                .cursorStr(config.getFirstCursor())
                .size(config.getPageSize())
                .pageNum(0)
                .build();
        enqueuePage(firstPage);
        
        // Main processing loop
        runProcessingLoop();
        
        // Extended retry phase if needed
        if (config.isUseExtendedRetries() && !stuckPages.isEmpty() && !cancelled) {
            logger.info("Entering extended retry phase with {} stuck pages", stuckPages.size());
            inExtendedRetryPhase = true;
            reEnqueueStuckPages();
            runProcessingLoop();
        }
        
        finished = true;
        logger.info("Batch {} completed. Completed: {}, Failed: {}", 
                config.getBatchId(), pagesCompleted.get(), failedPages.size());
        
        return getProgress();
    }
    
    private void runProcessingLoop() {
        while (shouldContinueProcessing()) {
            // Wait until we can start a new page or work is complete
            Workflow.await(() -> 
                    isReadyToStartPage() || isWorkComplete() || cancelled || shouldContinueAsNew()
            );
            
            if (cancelled) {
                logger.info("Batch cancelled: {}", cancellationReason);
                break;
            }
            
            if (shouldContinueAsNew()) {
                logger.info("Triggering continue-as-new after {} pages", pagesInThisRun.get());
                triggerContinueAsNew();
                return;
            }
            
            if (isReadyToStartPage()) {
                startNextPage();
            }
        }
    }
    
    private boolean shouldContinueProcessing() {
        return (!pendingPages.isEmpty() || !processingPages.isEmpty()) && !cancelled;
    }
    
    private boolean isWorkComplete() {
        return pendingPages.isEmpty() && processingPages.isEmpty();
    }
    
    private boolean isReadyToStartPage() {
        return !pendingPages.isEmpty() && 
               processingPages.size() < maxParallelism &&
               maxParallelism > 0;
    }
    
    private boolean shouldContinueAsNew() {
        if (Workflow.getInfo().isContinueAsNewSuggested()) {
            return true;
        }
        if (config.getPagesPerRun() > 0 && pagesInThisRun.get() >= config.getPagesPerRun()) {
            return !processingPages.isEmpty(); // Wait for in-flight pages
        }
        return false;
    }
    
    private void startNextPage() {
        if (pendingPages.isEmpty()) return;
        
        int pageNum = pendingPages.remove(0);
        PageState pageState = pages.get(pageNum);
        if (pageState == null) {
            logger.warn("Page {} not found in pages map", pageNum);
            return;
        }
        
        processingPages.add(pageNum);
        maxParallelismAchieved = Math.max(maxParallelismAchieved, processingPages.size());
        
        logger.debug("Starting page {} (cursor: {})", pageNum, pageState.getPage().getCursorStr());
        
        // Start page processing asynchronously
        Promise<PageResult> promise = Async.function(
                () -> pageActivity.processPage(
                        config.getPageProcessorName(),
                        config.getBatchId(),
                        pageState.getPage(),
                        config.getPageProcessorArgs(),
                        pageState.isStuck()
                )
        );
        
        pageState.setProcessingPromise(promise);
        
        // Handle completion
        promise.handle((result, error) -> {
            onPageCompleted(pageNum, result, error);
            return null;
        });
    }
    
    private void onPageCompleted(int pageNum, PageResult result, Throwable error) {
        processingPages.remove(pageNum);
        PageState pageState = pages.get(pageNum);
        
        if (error != null) {
            handlePageFailure(pageNum, pageState, error);
        } else {
            handlePageSuccess(pageNum, pageState, result);
        }
    }
    
    private void handlePageSuccess(int pageNum, PageState pageState, PageResult result) {
        logger.debug("Page {} completed successfully", pageNum);
        
        if (pageState.isStuck()) {
            stuckPages.remove(pageNum);
        }
        
        pagesCompleted.incrementAndGet();
        itemsProcessed += result.getItemsProcessed();
        
        pageState.markCompleted();
    }
    
    private void handlePageFailure(int pageNum, PageState pageState, Throwable error) {
        Throwable cause = error;
        if (error instanceof ActivityFailure) {
            cause = error.getCause();
        }
        
        boolean isNonRetryable = isNonRetryableError(cause);
        boolean shouldExtendedRetry = config.isUseExtendedRetries() && 
                                       !isNonRetryable && 
                                       !pageState.isStuck();
        
        if (shouldExtendedRetry && !inExtendedRetryPhase) {
            logger.info("Page {} got stuck, will retry in extended phase", pageNum);
            stuckPages.add(pageNum);
            pageState.markStuck();
        } else {
            logger.error("Page {} failed permanently: {}", pageNum, cause.getMessage());
            failedPages.add(pageNum);
            stuckPages.remove(pageNum);
            pageState.markFailed(cause);
        }
    }
    
    private boolean isNonRetryableError(Throwable error) {
        if (error instanceof ApplicationFailure) {
            return ((ApplicationFailure) error).isNonRetryable();
        }
        return false;
    }
    
    private void enqueuePage(BatchPage page) {
        int pageNum = page.getPageNum();
        
        // Check for duplicate
        if (pages.containsKey(pageNum)) {
            logger.warn("Duplicate page {} signal received, ignoring", pageNum);
            return;
        }
        
        PageState pageState = new PageState(page);
        pages.put(pageNum, pageState);
        pendingPages.add(pageNum);
        pagesEnqueued.incrementAndGet();
        pagesInThisRun.incrementAndGet();
        
        logger.debug("Enqueued page {} (total: {})", pageNum, pagesEnqueued.get());
    }
    
    private void reEnqueueStuckPages() {
        for (int pageNum : new ArrayList<>(stuckPages)) {
            pendingPages.add(pageNum);
        }
        
        // Recreate activity stub with extended retry options
        this.pageActivity = Workflow.newActivityStub(
                BatchPageActivity.class,
                buildActivityOptions(config, true)
        );
    }
    
    private void triggerContinueAsNew() {
        // Save state and continue as new
        BatchOrchestratorState state = BatchOrchestratorState.builder()
                .pages(pages)
                .pendingPages(new ArrayList<>(pendingPages))
                .stuckPages(new HashSet<>(stuckPages))
                .failedPages(new HashSet<>(failedPages))
                .pagesEnqueued(pagesEnqueued.get())
                .pagesCompleted(pagesCompleted.get())
                .itemsProcessed(itemsProcessed)
                .maxParallelism(maxParallelism)
                .maxParallelismAchieved(maxParallelismAchieved)
                .startTime(startTime)
                .inExtendedRetryPhase(inExtendedRetryPhase)
                .build();
        
        Workflow.continueAsNew(config, state);
    }
    
    private ActivityOptions buildActivityOptions(BatchConfig config, boolean extendedRetry) {
        RetryOptions.Builder retryBuilder = RetryOptions.newBuilder();
        
        if (extendedRetry) {
            retryBuilder
                    .setBackoffCoefficient(1.0)
                    .setInitialInterval(config.getExtendedRetryInterval())
                    .setMaximumAttempts(0); // Infinite until workflow timeout
        } else {
            retryBuilder
                    .setMaximumAttempts(config.getMaxInitialRetries())
                    .setInitialInterval(Duration.ofSeconds(1))
                    .setMaximumInterval(Duration.ofMinutes(1))
                    .setBackoffCoefficient(2.0);
        }
        
        return ActivityOptions.newBuilder()
                .setStartToCloseTimeout(config.getPageTimeout())
                .setRetryOptions(retryBuilder.build())
                .build();
    }
    
    @Override
    public BatchProgress getProgress() {
        return BatchProgress.builder()
                .batchId(config.getBatchId())
                .completedPages(pagesCompleted.get())
                .processingPages(processingPages.size())
                .pendingPages(pendingPages.size())
                .stuckPages(stuckPages.size())
                .failedPages(failedPages.size())
                .maxParallelismAchieved(maxParallelismAchieved)
                .currentParallelism(maxParallelism)
                .itemsProcessed(itemsProcessed)
                .startTime(startTime)
                .finished(finished)
                .stuckPageNumbers(new HashSet<>(stuckPages))
                .failedPageNumbers(new HashSet<>(failedPages))
                .throughput(calculateThroughput())
                .build();
    }
    
    private double calculateThroughput() {
        if (startTime == null) return 0.0;
        long elapsed = Workflow.currentTimeMillis() - startTime.toEpochMilli();
        if (elapsed <= 0) return 0.0;
        return (itemsProcessed * 1000.0) / elapsed;
    }
    
    @Override
    public void setMaxParallelism(int maxParallelism) {
        logger.info("Changing parallelism from {} to {}", this.maxParallelism, maxParallelism);
        previousParallelisms.push(this.maxParallelism);
        this.maxParallelism = maxParallelism;
    }
    
    @Override
    public void restoreParallelism() {
        if (!previousParallelisms.isEmpty()) {
            int previous = previousParallelisms.pop();
            logger.info("Restoring parallelism from {} to {}", maxParallelism, previous);
            maxParallelism = previous;
        }
    }
    
    @Override
    public void addPage(BatchPage page) {
        enqueuePage(page);
    }
    
    @Override
    public void cancel(String reason) {
        logger.info("Batch cancellation requested: {}", reason);
        cancelled = true;
        cancellationReason = reason;
    }
    
    /**
     * Internal state for a page.
     */
    @lombok.Data
    private static class PageState {
        private final BatchPage page;
        private Promise<PageResult> processingPromise;
        private boolean stuck;
        private boolean completed;
        private boolean failed;
        private Throwable failureReason;
        
        void markStuck() {
            this.stuck = true;
        }
        
        void markCompleted() {
            this.completed = true;
            this.stuck = false;
        }
        
        void markFailed(Throwable reason) {
            this.failed = true;
            this.failureReason = reason;
        }
    }
    
    /**
     * Result from processing a page.
     */
    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class PageResult {
        private int pageNum;
        private int itemsProcessed;
        private String nextCursor;
        private boolean isLastPage;
    }
    
    /**
     * State for continue-as-new.
     */
    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class BatchOrchestratorState {
        private Map<Integer, PageState> pages;
        private List<Integer> pendingPages;
        private Set<Integer> stuckPages;
        private Set<Integer> failedPages;
        private int pagesEnqueued;
        private int pagesCompleted;
        private long itemsProcessed;
        private int maxParallelism;
        private int maxParallelismAchieved;
        private Instant startTime;
        private boolean inExtendedRetryPhase;
    }
}
