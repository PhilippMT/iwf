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
package io.iworkflow.core.client;

import io.iworkflow.core.IwfConstants;
import io.iworkflow.core.interpreter.InterpreterWorkflow;
import io.iworkflow.core.interpreter.InterpreterWorkflowInput;
import io.iworkflow.core.interpreter.InterpreterWorkflowOutput;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse;
import io.temporal.api.workflowservice.v1.ListWorkflowExecutionsResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.client.WorkflowUpdateException;
import io.temporal.serviceclient.WorkflowServiceStubs;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Wrapper around Temporal WorkflowClient for iWF operations.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TemporalClientWrapper {

    private final WorkflowClient workflowClient;
    private final WorkflowServiceStubs workflowServiceStubs;

    /**
     * Start the interpreter workflow.
     */
    public String startInterpreterWorkflow(WorkflowOptions options, InterpreterWorkflowInput input) {
        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(
                "InterpreterWorkflow",
                options
        );
        
        return stub.start(input).getRunId();
    }

    /**
     * Signal a workflow.
     */
    public void signalWorkflow(String workflowId, String runId, String signalName, Object signalArg) {
        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(workflowId, Optional.ofNullable(runId), Optional.empty());
        stub.signal(signalName, signalArg);
    }

    /**
     * Cancel a workflow.
     */
    public void cancelWorkflow(String workflowId, String runId) {
        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(workflowId, Optional.ofNullable(runId), Optional.empty());
        stub.cancel();
    }

    /**
     * Terminate a workflow.
     */
    public void terminateWorkflow(String workflowId, String runId, String reason) {
        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(workflowId, Optional.ofNullable(runId), Optional.empty());
        stub.terminate(reason);
    }

    /**
     * Describe a workflow execution.
     */
    public DescribeWorkflowExecutionResponse describeWorkflow(String workflowId, String runId) {
        return workflowServiceStubs.blockingStub().describeWorkflowExecution(
                io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest.newBuilder()
                        .setNamespace(workflowClient.getOptions().getNamespace())
                        .setExecution(io.temporal.api.common.v1.WorkflowExecution.newBuilder()
                                .setWorkflowId(workflowId)
                                .setRunId(runId != null ? runId : "")
                                .build())
                        .build()
        );
    }

    /**
     * Get workflow result with timeout.
     */
    public InterpreterWorkflowOutput getWorkflowResult(String workflowId, String runId, 
                                                        long timeout, TimeUnit unit) {
        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(workflowId, Optional.ofNullable(runId), Optional.empty());
        try {
            return stub.getResult(timeout, unit, InterpreterWorkflowOutput.class);
        } catch (TimeoutException e) {
            throw new RuntimeException("Workflow result timeout", e);
        }
    }

    /**
     * List workflows.
     */
    public ListWorkflowExecutionsResponse listWorkflows(String query, int pageSize, byte[] nextPageToken) {
        var requestBuilder = io.temporal.api.workflowservice.v1.ListWorkflowExecutionsRequest.newBuilder()
                .setNamespace(workflowClient.getOptions().getNamespace())
                .setQuery(query)
                .setPageSize(pageSize);
        
        if (nextPageToken != null) {
            requestBuilder.setNextPageToken(com.google.protobuf.ByteString.copyFrom(nextPageToken));
        }
        
        return workflowServiceStubs.blockingStub().listWorkflowExecutions(requestBuilder.build());
    }

    /**
     * Reset a workflow.
     */
    public String resetWorkflow(String workflowId, String runId, String resetType, String reason,
                                 Integer historyEventId, String historyEventTime,
                                 String stateId, String stateExecutionId,
                                 Boolean skipSignalReapply, Boolean skipUpdateReapply) {
        // Get current workflow description to find proper reset point
        DescribeWorkflowExecutionResponse descResp = describeWorkflow(workflowId, runId);
        String actualRunId = runId != null && !runId.isEmpty() ? runId : 
                descResp.getWorkflowExecutionInfo().getExecution().getRunId();

        long resetEventId = determineResetEventId(workflowId, actualRunId, resetType, 
                historyEventId, historyEventTime, stateId, stateExecutionId);

        var requestBuilder = io.temporal.api.workflowservice.v1.ResetWorkflowExecutionRequest.newBuilder()
                .setNamespace(workflowClient.getOptions().getNamespace())
                .setWorkflowExecution(io.temporal.api.common.v1.WorkflowExecution.newBuilder()
                        .setWorkflowId(workflowId)
                        .setRunId(actualRunId)
                        .build())
                .setReason(reason != null ? reason : "Reset via iWF API")
                .setWorkflowTaskFinishEventId(resetEventId)
                .setRequestId(UUID.randomUUID().toString());

        if (Boolean.TRUE.equals(skipSignalReapply)) {
            requestBuilder.addResetReapplyExcludeTypes(
                    io.temporal.api.enums.v1.ResetReapplyExcludeType.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL);
        }
        if (Boolean.TRUE.equals(skipUpdateReapply)) {
            requestBuilder.addResetReapplyExcludeTypes(
                    io.temporal.api.enums.v1.ResetReapplyExcludeType.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE);
        }

        var response = workflowServiceStubs.blockingStub().resetWorkflowExecution(requestBuilder.build());
        return response.getRunId();
    }

    /**
     * Wait for state completion workflow.
     */
    public Object waitForStateCompletionWorkflow(String workflowId, String taskQueue, long timeoutSeconds) {
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(taskQueue)
                .setWorkflowExecutionTimeout(Duration.ofSeconds(timeoutSeconds + 60))
                .build();

        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(
                "WaitForStateCompletionWorkflow",
                options
        );

        try {
            stub.start();
        } catch (Exception e) {
            // Workflow may already exist, that's fine
            log.debug("Wait workflow may already exist: {}", workflowId);
        }

        try {
            return stub.getResult(timeoutSeconds, TimeUnit.SECONDS, Object.class);
        } catch (TimeoutException e) {
            throw new RuntimeException("Wait for state completion timeout", e);
        }
    }

    /**
     * Get data objects from workflow memo.
     */
    public List<Object> getDataObjectsFromMemo(String workflowId, String runId, List<String> keys) {
        DescribeWorkflowExecutionResponse descResp = describeWorkflow(workflowId, runId);
        var memo = descResp.getWorkflowExecutionInfo().getMemo();
        
        List<Object> result = new ArrayList<>();
        Set<String> requestedKeys = keys != null ? new HashSet<>(keys) : null;
        
        for (var entry : memo.getFieldsMap().entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(IwfConstants.IWF_SYSTEM_CONST_PREFIX)) {
                continue;
            }
            if (requestedKeys != null && !requestedKeys.isEmpty() && !requestedKeys.contains(key)) {
                continue;
            }
            // Decode the payload
            result.add(Map.of("key", key, "value", decodePayload(entry.getValue())));
        }
        
        return result;
    }

    /**
     * Query workflow for data objects.
     */
    public List<Object> queryDataObjects(String workflowId, String runId, List<String> keys) {
        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(workflowId, Optional.ofNullable(runId), Optional.empty());
        Map<String, Object> queryResult = stub.query(IwfConstants.QUERY_TYPE_GET_DATA_ATTRIBUTES, 
                Map.class, Map.of("keys", keys != null ? keys : List.of()));
        
        return (List<Object>) queryResult.getOrDefault("dataAttributes", List.of());
    }

    /**
     * Get search attributes from workflow.
     */
    public List<Object> getSearchAttributes(String workflowId, String runId, List<Object> keys) {
        DescribeWorkflowExecutionResponse descResp = describeWorkflow(workflowId, runId);
        var searchAttrs = descResp.getWorkflowExecutionInfo().getSearchAttributes();
        
        List<Object> result = new ArrayList<>();
        Set<String> requestedKeys = new HashSet<>();
        if (keys != null) {
            for (Object keyObj : keys) {
                if (keyObj instanceof Map) {
                    String key = (String) ((Map<String, Object>) keyObj).get("key");
                    if (key != null) {
                        requestedKeys.add(key);
                    }
                }
            }
        }
        
        for (var entry : searchAttrs.getIndexedFieldsMap().entrySet()) {
            String key = entry.getKey();
            if (!requestedKeys.isEmpty() && !requestedKeys.contains(key)) {
                continue;
            }
            result.add(Map.of("key", key, "value", decodePayload(entry.getValue())));
        }
        
        return result;
    }

    /**
     * Execute RPC using workflow update (with locking).
     */
    public Map<String, Object> executeRpcWithUpdate(String workflowId, String runId, Map<String, Object> request) {
        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(workflowId, Optional.ofNullable(runId), Optional.empty());
        
        try {
            Object result = stub.update(IwfConstants.UPDATE_TYPE_EXECUTE_OPTIMISTIC_LOCKING_RPC, Object.class, request);
            if (result instanceof Map) {
                return (Map<String, Object>) result;
            }
            return Map.of("output", result);
        } catch (WorkflowUpdateException e) {
            throw new RuntimeException("RPC execution failed", e);
        }
    }

    /**
     * Execute RPC using query and signal (without locking).
     */
    public Map<String, Object> executeRpcWithQueryAndSignal(String workflowId, String runId, Map<String, Object> request) {
        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(workflowId, Optional.ofNullable(runId), Optional.empty());
        
        // First, query to prepare RPC
        Map<String, Object> prepareResult = stub.query(IwfConstants.QUERY_TYPE_PREPARE_RPC, Map.class,
                Map.of(
                        "dataObjectsLoadingPolicy", request.getOrDefault("dataAttributesLoadingPolicy", Map.of()),
                        "searchAttributesLoadingPolicy", request.getOrDefault("searchAttributesLoadingPolicy", Map.of())
                ));
        
        // Call worker RPC API
        // This would be implemented to call the worker's /api/v1/workflowWorker/rpc endpoint
        // For now, return a placeholder
        Map<String, Object> workerResponse = callWorkerRpc(prepareResult, request);
        
        // If there are mutations, send signal
        if (hasMutations(workerResponse)) {
            Map<String, Object> signalValue = buildRpcSignalValue(request, workerResponse);
            stub.signal(IwfConstants.SIGNAL_CHANNEL_EXECUTE_RPC, signalValue);
        }
        
        return Map.of("output", workerResponse.get("output"));
    }

    /**
     * Query workflow internal dump for continue-as-new.
     */
    public Map<String, Object> queryWorkflowDump(String workflowId, String runId, int pageSizeInBytes, int pageNum) {
        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(workflowId, Optional.ofNullable(runId), Optional.empty());
        return stub.query(IwfConstants.QUERY_TYPE_CONTINUE_AS_NEW_DUMP, Map.class,
                Map.of("pageSizeInBytes", pageSizeInBytes, "pageNum", pageNum));
    }

    // Helper methods

    private long determineResetEventId(String workflowId, String runId, String resetType,
                                        Integer historyEventId, String historyEventTime,
                                        String stateId, String stateExecutionId) {
        // Implementation would parse workflow history to find appropriate reset point
        // For now, return a default or the specified event ID
        if (historyEventId != null) {
            return historyEventId;
        }
        
        // Would need to implement logic for different reset types:
        // - HISTORY_EVENT_ID: use provided historyEventId
        // - BEGINNING: find first workflow task completed event
        // - HISTORY_EVENT_TIME: find event at or before the specified time
        // - STATE_ID/STATE_EXECUTION_ID: find the event when the state started
        
        return 3; // Default to first workflow task completed event
    }

    private Object decodePayload(io.temporal.api.common.v1.Payload payload) {
        // Use the data converter to decode
        // For now, return the raw bytes as string
        return payload.getData().toStringUtf8();
    }

    private Map<String, Object> callWorkerRpc(Map<String, Object> prepareResult, Map<String, Object> request) {
        // Build worker RPC request
        String workerUrl = (String) prepareResult.get("iwfWorkerUrl");
        String workflowType = (String) prepareResult.get("iwfWorkflowType");
        
        if (workerUrl == null || workerUrl.isEmpty()) {
            throw new IllegalStateException("Worker URL not available from prepare result");
        }
        
        // Build the worker request
        Map<String, Object> workerRequest = new HashMap<>();
        
        // Build context
        Map<String, Object> context = new HashMap<>();
        context.put("workflowId", request.get("workflowId"));
        context.put("workflowRunId", request.get("workflowRunId"));
        context.put("workflowStartedTimestamp", System.currentTimeMillis());
        workerRequest.put("context", context);
        
        workerRequest.put("workflowType", workflowType);
        workerRequest.put("rpcName", request.get("rpcName"));
        workerRequest.put("input", request.get("input"));
        workerRequest.put("searchAttributes", prepareResult.get("searchAttributes"));
        workerRequest.put("dataAttributes", prepareResult.get("dataAttributes"));
        workerRequest.put("signalChannelInfos", prepareResult.get("signalChannelInfos"));
        workerRequest.put("internalChannelInfos", prepareResult.get("internalChannelInfos"));
        
        // Make HTTP call to worker
        try {
            String url = buildWorkerUrl(workerUrl, "/api/v1/workflowWorker/rpc");
            return makeHttpRequest(url, workerRequest);
        } catch (Exception e) {
            log.error("Failed to call worker RPC API: {}", e.getMessage(), e);
            throw new RuntimeException("Worker RPC call failed: " + e.getMessage(), e);
        }
    }
    
    private String buildWorkerUrl(String baseUrl, String path) {
        return baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) + path : baseUrl + path;
    }
    
    private Map<String, Object> makeHttpRequest(String url, Map<String, Object> request) {
        // Use RestTemplate or WebClient to make HTTP call
        // For simplicity, using RestTemplate here
        org.springframework.web.client.RestTemplate restTemplate = new org.springframework.web.client.RestTemplate();
        
        org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
        headers.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);
        
        org.springframework.http.HttpEntity<Map<String, Object>> entity = 
                new org.springframework.http.HttpEntity<>(request, headers);
        
        org.springframework.http.ResponseEntity<Map> response = restTemplate.exchange(
                url,
                org.springframework.http.HttpMethod.POST,
                entity,
                Map.class
        );
        
        if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
            return new HashMap<>(response.getBody());
        }
        
        throw new RuntimeException("Worker RPC returned error: " + response.getStatusCode());
    }

    private boolean hasMutations(Map<String, Object> workerResponse) {
        return !isEmpty(workerResponse.get("upsertDataAttributes")) ||
               !isEmpty(workerResponse.get("upsertSearchAttributes")) ||
               !isEmpty(workerResponse.get("publishToInterStateChannel")) ||
               !isEmpty(workerResponse.get("recordEvents")) ||
               hasNextStates(workerResponse.get("stateDecision"));
    }

    private boolean isEmpty(Object obj) {
        if (obj == null) return true;
        if (obj instanceof Collection) return ((Collection<?>) obj).isEmpty();
        if (obj instanceof Map) return ((Map<?, ?>) obj).isEmpty();
        return false;
    }

    private boolean hasNextStates(Object stateDecision) {
        if (stateDecision instanceof Map) {
            Object nextStates = ((Map<String, Object>) stateDecision).get("nextStates");
            return !isEmpty(nextStates);
        }
        return false;
    }

    private Map<String, Object> buildRpcSignalValue(Map<String, Object> request, Map<String, Object> workerResponse) {
        Map<String, Object> signalValue = new HashMap<>();
        signalValue.put("rpcInput", request.get("input"));
        signalValue.put("rpcOutput", workerResponse.get("output"));
        signalValue.put("upsertDataObjects", workerResponse.get("upsertDataAttributes"));
        signalValue.put("upsertSearchAttributes", workerResponse.get("upsertSearchAttributes"));
        signalValue.put("stateDecision", workerResponse.get("stateDecision"));
        signalValue.put("recordEvents", workerResponse.get("recordEvents"));
        signalValue.put("interStateChannelPublishing", workerResponse.get("publishToInterStateChannel"));
        return signalValue;
    }
}
