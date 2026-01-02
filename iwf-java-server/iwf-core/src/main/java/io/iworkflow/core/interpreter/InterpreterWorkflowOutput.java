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
 * Output from the interpreter workflow.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InterpreterWorkflowOutput {
    
    /**
     * Completion outputs from all states that completed with output
     */
    private List<StateCompletionOutput> stateCompletionOutputs;
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StateCompletionOutput {
        private String completedStateId;
        private String completedStateExecutionId;
        private Object completedStateOutput;
    }
}
