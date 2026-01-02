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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowStub;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of BatchPageActivity that processes pages using registered processors.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BatchPageActivityImpl implements BatchPageActivity {
    
    private final WorkflowClient workflowClient;
    private final ObjectMapper objectMapper;
    
    // Registry of page processors
    private static final Map<String, PageProcessor> processorRegistry = new ConcurrentHashMap<>();
    
    /**
     * Register a page processor.
     */
    public static void registerProcessor(String name, PageProcessor processor) {
        processorRegistry.put(name, processor);
        log.info("Registered batch processor: {}", name);
    }
    
    /**
     * Get a registered processor.
     */
    public static PageProcessor getProcessor(String name) {
        PageProcessor processor = processorRegistry.get(name);
        if (processor == null) {
            throw new IllegalArgumentException(
                    "Page processor '" + name + "' not found. Available: " + processorRegistry.keySet());
        }
        return processor;
    }
    
    @Override
    public BatchOrchestratorWorkflowImpl.PageResult processPage(
            String pageProcessorName,
            String batchId,
            BatchPage page,
            String args,
            boolean isRetry) {
        
        log.debug("Processing page {} for batch {}", page.getPageNum(), batchId);
        
        ActivityExecutionContext activityContext = Activity.getExecutionContext();
        PageProcessor processor = getProcessor(pageProcessorName);
        
        // Create processor context
        ActivityBasedContext context = new ActivityBasedContext(
                page,
                batchId,
                args,
                activityContext,
                workflowClient,
                objectMapper,
                isRetry
        );
        
        try {
            processor.process(context);
            
            return BatchOrchestratorWorkflowImpl.PageResult.builder()
                    .pageNum(page.getPageNum())
                    .itemsProcessed(context.getItemsProcessedCount())
                    .nextCursor(context.getNextCursor())
                    .isLastPage(context.isLastPage())
                    .build();
            
        } catch (Exception e) {
            log.error("Error processing page {} for batch {}: {}", 
                    page.getPageNum(), batchId, e.getMessage(), e);
            throw new RuntimeException("Page processing failed", e);
        }
    }
    
    /**
     * Activity-based implementation of BatchProcessorContext.
     */
    @Slf4j
    private static class ActivityBasedContext implements BatchProcessorContext {
        
        private final BatchPage page;
        private final String batchId;
        private final String args;
        private final ActivityExecutionContext activityContext;
        private final WorkflowClient workflowClient;
        private final ObjectMapper objectMapper;
        private final boolean isRetry;
        
        private final AtomicInteger itemsProcessed = new AtomicInteger(0);
        private String nextCursor;
        private boolean lastPage = false;
        private boolean nextPageSignaled = false;
        private HeartbeatState heartbeatState;
        
        ActivityBasedContext(BatchPage page, String batchId, String args,
                             ActivityExecutionContext activityContext,
                             WorkflowClient workflowClient, ObjectMapper objectMapper,
                             boolean isRetry) {
            this.page = page;
            this.batchId = batchId;
            this.args = args;
            this.activityContext = activityContext;
            this.workflowClient = workflowClient;
            this.objectMapper = objectMapper;
            this.isRetry = isRetry;
            
            // Check for previous heartbeat state
            this.heartbeatState = activityContext.getHeartbeatDetails(HeartbeatState.class)
                    .orElse(new HeartbeatState());
        }
        
        @Override
        public BatchPage getCurrentPage() {
            return page;
        }
        
        @Override
        public String getBatchId() {
            return batchId;
        }
        
        @Override
        public String getArgsStr() {
            return args;
        }
        
        @Override
        public <T> T getArgs(Class<T> clazz) {
            if (args == null || args.isEmpty()) {
                return null;
            }
            try {
                return objectMapper.readValue(args, clazz);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse args", e);
            }
        }
        
        @Override
        public void enqueueNextPage(BatchPage nextPage) {
            if (nextPageSignaled) {
                throw new IllegalStateException("Next page already signaled for this page");
            }
            
            // Check if we already signaled in a previous attempt
            if (heartbeatState.isNextPageSignaled()) {
                log.debug("Next page was already signaled in previous attempt");
                nextPageSignaled = true;
                return;
            }
            
            // Set the page number
            nextPage.setPageNum(page.getPageNum() + 1);
            
            // Signal the orchestrator workflow
            try {
                WorkflowStub orchestrator = workflowClient.newUntypedWorkflowStub(
                        activityContext.getInfo().getWorkflowId(),
                        Optional.empty(),
                        Optional.empty()
                );
                orchestrator.signal("addPage", nextPage);
                
                nextPageSignaled = true;
                nextCursor = nextPage.getCursorStr();
                
                // Record in heartbeat state
                heartbeatState.setNextPageSignaled(true);
                activityContext.heartbeat(heartbeatState);
                
                log.debug("Signaled next page {} with cursor {}", nextPage.getPageNum(), nextCursor);
                
            } catch (Exception e) {
                log.error("Failed to signal next page", e);
                throw new RuntimeException("Failed to enqueue next page", e);
            }
        }
        
        @Override
        public void markAsLastPage() {
            lastPage = true;
        }
        
        @Override
        public void incrementItemsProcessed() {
            itemsProcessed.incrementAndGet();
        }
        
        @Override
        public void incrementItemsProcessed(int count) {
            itemsProcessed.addAndGet(count);
        }
        
        @Override
        public String getWorkflowId() {
            return activityContext.getInfo().getWorkflowId();
        }
        
        @Override
        public String getWorkflowRunId() {
            return activityContext.getInfo().getRunId();
        }
        
        @Override
        public void heartbeat() {
            activityContext.heartbeat(heartbeatState);
        }
        
        @Override
        public void heartbeat(Object details) {
            heartbeatState.setUserDetails(details);
            activityContext.heartbeat(heartbeatState);
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public <T> T getHeartbeatDetails(Class<T> clazz) {
            return (T) heartbeatState.getUserDetails();
        }
        
        @Override
        public boolean isRetry() {
            return isRetry;
        }
        
        @Override
        public int getAttemptNumber() {
            return activityContext.getInfo().getAttempt();
        }
        
        @Override
        public boolean wasNextPageAlreadySignaled() {
            return heartbeatState.isNextPageSignaled();
        }
        
        int getItemsProcessedCount() {
            return itemsProcessed.get();
        }
        
        String getNextCursor() {
            return nextCursor;
        }
        
        boolean isLastPage() {
            return lastPage;
        }
    }
    
    /**
     * State saved in heartbeats for resumption.
     */
    @lombok.Data
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class HeartbeatState {
        private boolean nextPageSignaled;
        private Object userDetails;
    }
}
