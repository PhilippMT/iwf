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
package io.iworkflow.core.service;

import java.util.Map;

/**
 * Service interface for workflow operations.
 * 
 * <p>This interface defines all workflow management operations exposed by the iWF API.</p>
 */
public interface WorkflowService {

    // Workflow Lifecycle
    
    /**
     * Start a new workflow.
     */
    Map<String, Object> startWorkflow(Map<String, Object> request);
    
    /**
     * Signal a running workflow.
     */
    void signalWorkflow(Map<String, Object> request);
    
    /**
     * Publish to internal channel.
     */
    void publishToInternalChannel(Map<String, Object> request);
    
    /**
     * Stop a workflow (cancel, terminate, or fail).
     */
    void stopWorkflow(Map<String, Object> request);
    
    /**
     * Get workflow status and optionally wait for completion.
     */
    Map<String, Object> getWorkflow(Map<String, Object> request, boolean waitForCompletion);
    
    /**
     * Wait for a specific state execution to complete.
     */
    Map<String, Object> waitForStateCompletion(Map<String, Object> request);
    
    /**
     * Search for workflows.
     */
    Map<String, Object> searchWorkflows(Map<String, Object> request);
    
    /**
     * Reset a workflow to a previous point.
     */
    Map<String, Object> resetWorkflow(Map<String, Object> request);

    // Data/Search Attributes
    
    /**
     * Get data objects from a workflow.
     */
    Map<String, Object> getDataObjects(Map<String, Object> request);
    
    /**
     * Set data objects in a workflow.
     */
    void setDataObjects(Map<String, Object> request);
    
    /**
     * Get search attributes from a workflow.
     */
    Map<String, Object> getSearchAttributes(Map<String, Object> request);
    
    /**
     * Set search attributes in a workflow.
     */
    void setSearchAttributes(Map<String, Object> request);

    // RPC
    
    /**
     * Execute RPC on a workflow.
     */
    Map<String, Object> executeRpc(Map<String, Object> request);

    // Timer
    
    /**
     * Skip a timer command.
     */
    void skipTimer(Map<String, Object> request);

    // Config
    
    /**
     * Update workflow configuration.
     */
    void updateConfig(Map<String, Object> request);
    
    /**
     * Trigger continue-as-new.
     */
    void triggerContinueAsNew(Map<String, Object> request);

    // Internal
    
    /**
     * Dump workflow internal state.
     */
    Map<String, Object> dumpWorkflowInternal(Map<String, Object> request);

    // Health
    
    /**
     * Health check.
     */
    Map<String, Object> healthCheck();
}
