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

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.UpdateMethod;

/**
 * The Interpreter Workflow is the core workflow that executes iWF workflows.
 * 
 * <p>This workflow receives state execution requests and orchestrates the execution
 * of workflow states by calling worker APIs (waitUntil and execute) and processing
 * commands (timers, signals, internal channels).</p>
 * 
 * <p>The interpreter workflow supports:</p>
 * <ul>
 *   <li>State execution with waitUntil and execute APIs</li>
 *   <li>Timer, signal, and internal channel commands</li>
 *   <li>Search attributes and data attributes persistence</li>
 *   <li>RPC execution (read-only and with locks)</li>
 *   <li>Continue-as-new for long-running workflows</li>
 * </ul>
 */
@WorkflowInterface
public interface InterpreterWorkflow {

    /**
     * Main workflow method that executes the interpreter logic.
     * 
     * @param input the workflow input containing initial state and configuration
     * @return the workflow output containing completion results
     */
    @WorkflowMethod
    InterpreterWorkflowOutput execute(InterpreterWorkflowInput input);

    // Query Methods

    /**
     * Get data attributes from the workflow.
     * 
     * @param request the request containing keys to retrieve
     * @return response containing the requested data attributes
     */
    @QueryMethod(name = "IwfGetDataAttributes")
    GetDataAttributesResponse getDataAttributes(GetDataAttributesRequest request);

    /**
     * Get current timer information for skip timer functionality.
     * 
     * @return response containing current timer states
     */
    @QueryMethod(name = "IwfGetCurrentTimerInfos")
    GetCurrentTimerInfosResponse getCurrentTimerInfos();

    /**
     * Prepare for RPC execution by loading persistence data.
     * 
     * @param request the RPC preparation request
     * @return response containing loaded data and workflow info
     */
    @QueryMethod(name = "IwfPrepareRpc")
    PrepareRpcResponse prepareRpc(PrepareRpcRequest request);

    /**
     * Dump workflow internal state for continue-as-new.
     * 
     * @param request the dump request with pagination info
     * @return response containing serialized workflow state
     */
    @QueryMethod(name = "IwfContinueAsNewDump")
    ContinueAsNewDumpResponse continueAsNewDump(ContinueAsNewDumpRequest request);

    // Signal Methods

    /**
     * Skip a timer command.
     * 
     * @param request the skip timer request
     */
    @SignalMethod(name = "IwfSkipTimerSignal")
    void skipTimer(SkipTimerRequest request);

    /**
     * Fail the workflow with a specific reason.
     * 
     * @param request the fail workflow request
     */
    @SignalMethod(name = "IwfFailWorkflowSignal")
    void failWorkflow(FailWorkflowRequest request);

    /**
     * Execute RPC operations (without locking).
     * 
     * @param request the RPC signal request
     */
    @SignalMethod(name = "IwfExecuteRpcSignal")
    void executeRpcSignal(ExecuteRpcSignalRequest request);

    /**
     * Update workflow configuration.
     * 
     * @param config the new workflow configuration
     */
    @SignalMethod(name = "IwfUpdateConfigSignal")
    void updateConfig(WorkflowConfigSignal config);

    /**
     * Trigger continue-as-new.
     */
    @SignalMethod(name = "IwfTriggerContinueAsNewSignal")
    void triggerContinueAsNew();

    // Update Method (for optimistic locking RPC)

    /**
     * Execute RPC with optimistic locking.
     * 
     * @param request the RPC request with locking requirements
     * @return the RPC response
     */
    @UpdateMethod(name = "IwfExecuteOptimisticLockingRpc")
    ExecuteRpcUpdateResponse executeOptimisticLockingRpc(ExecuteRpcUpdateRequest request);
}
