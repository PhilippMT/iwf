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
package io.iworkflow.core.interpreter;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Input for the interpreter workflow.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InterpreterWorkflowInput {
    
    /**
     * The iWF workflow type name
     */
    private String iwfWorkflowType;
    
    /**
     * URL of the worker that implements this workflow
     */
    private String iwfWorkerUrl;
    
    /**
     * Initial state ID to start with
     */
    private String startStateId;
    
    /**
     * Input for the initial state (JSON encoded)
     */
    private Object stateInput;
    
    /**
     * Options for the initial state
     */
    private Object stateOptions;
    
    /**
     * Initial search attributes
     */
    private List<Object> initSearchAttributes;
    
    /**
     * Initial data attributes
     */
    private List<Object> initDataAttributes;
    
    /**
     * Workflow configuration
     */
    private WorkflowConfig config;
    
    /**
     * Whether to use memo for data attributes
     */
    private boolean useMemoForDataAttributes;
    
    /**
     * State execution IDs to wait for completion
     */
    private List<String> waitForCompletionStateExecutionIds;
    
    /**
     * State IDs to wait for completion
     */
    private List<String> waitForCompletionStateIds;
    
    /**
     * Whether this is a resume from continue-as-new
     */
    private boolean isResumeFromContinueAsNew;
    
    /**
     * Continue-as-new input (only set when isResumeFromContinueAsNew is true)
     */
    private ContinueAsNewInput continueAsNewInput;
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ContinueAsNewInput {
        private String previousInternalRunId;
    }
}
