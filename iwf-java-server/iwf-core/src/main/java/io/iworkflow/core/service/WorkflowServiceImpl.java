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

import io.iworkflow.core.IwfConstants;
import io.iworkflow.core.client.TemporalClientWrapper;
import io.iworkflow.core.interpreter.InterpreterWorkflowInput;
import io.iworkflow.core.interpreter.InterpreterWorkflowOutput;
import io.iworkflow.core.interpreter.WorkflowConfig;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.api.workflow.v1.WorkflowExecutionInfo;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.client.WorkflowNotFoundException;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of WorkflowService using Temporal.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WorkflowServiceImpl implements WorkflowService {

    private final TemporalClientWrapper clientWrapper;
    private final WorkflowServiceConfig config;

    @Override
    public Map<String, Object> startWorkflow(Map<String, Object> request) {
        String workflowId = (String) request.get("workflowId");
        String iwfWorkflowType = (String) request.get("iwfWorkflowType");
        String iwfWorkerUrl = (String) request.get("iwfWorkerUrl");
        Integer workflowTimeoutSeconds = (Integer) request.get("workflowTimeoutSeconds");
        String startStateId = (String) request.get("startStateId");
        Object stateInput = request.get("stateInput");
        Object stateOptions = request.get("stateOptions");
        Map<String, Object> workflowStartOptions = (Map<String, Object>) request.get("workflowStartOptions");

        // Build search attributes
        Map<String, Object> searchAttributes = new HashMap<>();
        searchAttributes.put(IwfConstants.SEARCH_ATTRIBUTE_IWF_WORKFLOW_TYPE, iwfWorkflowType);

        // Build memo
        Map<String, Object> memo = new HashMap<>();
        memo.put(IwfConstants.MEMO_KEY_WORKER_URL, Map.of("data", iwfWorkerUrl));

        // Build workflow options
        WorkflowOptions.Builder optionsBuilder = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(config.getTaskQueue())
                .setWorkflowExecutionTimeout(Duration.ofSeconds(workflowTimeoutSeconds != null ? workflowTimeoutSeconds : 3600));

        // Process start options
        boolean ignoreAlreadyStartedError = false;
        String requestId = null;
        List<Object> initSearchAttributes = null;
        List<Object> initDataAttributes = null;
        
        if (workflowStartOptions != null) {
            // Handle ID reuse policy
            String idReusePolicy = (String) workflowStartOptions.get("workflowIDReusePolicy");
            if (idReusePolicy != null) {
                optionsBuilder.setWorkflowIdReusePolicy(mapIdReusePolicy(idReusePolicy));
            }
            
            // Handle cron
            String cronSchedule = (String) workflowStartOptions.get("cronSchedule");
            if (cronSchedule != null && !cronSchedule.isEmpty()) {
                optionsBuilder.setCronSchedule(cronSchedule);
            }
            
            // Handle start delay
            Integer startDelaySeconds = (Integer) workflowStartOptions.get("workflowStartDelaySeconds");
            if (startDelaySeconds != null) {
                optionsBuilder.setStartDelay(Duration.ofSeconds(startDelaySeconds));
            }
            
            // Handle already started options
            Map<String, Object> alreadyStartedOptions = (Map<String, Object>) workflowStartOptions.get("workflowAlreadyStartedOptions");
            if (alreadyStartedOptions != null) {
                ignoreAlreadyStartedError = Boolean.TRUE.equals(alreadyStartedOptions.get("ignoreAlreadyStartedError"));
                requestId = (String) alreadyStartedOptions.get("requestId");
            }
            
            initSearchAttributes = (List<Object>) workflowStartOptions.get("searchAttributes");
            initDataAttributes = (List<Object>) workflowStartOptions.get("dataAttributes");
        }

        // Build workflow config
        WorkflowConfig workflowConfig = WorkflowConfig.builder()
                .continueAsNewThreshold(config.getContinueAsNewThreshold())
                .build();

        // Build interpreter input
        InterpreterWorkflowInput input = InterpreterWorkflowInput.builder()
                .iwfWorkflowType(iwfWorkflowType)
                .iwfWorkerUrl(iwfWorkerUrl)
                .startStateId(startStateId)
                .stateInput(stateInput)
                .stateOptions(stateOptions)
                .initSearchAttributes(initSearchAttributes)
                .initDataAttributes(initDataAttributes)
                .config(workflowConfig)
                .waitForCompletionStateExecutionIds(
                        (List<String>) request.get("waitForCompletionStateExecutionIds"))
                .waitForCompletionStateIds(
                        (List<String>) request.get("waitForCompletionStateIds"))
                .build();

        try {
            String runId = clientWrapper.startInterpreterWorkflow(optionsBuilder.build(), input);
            log.info("Started workflow {} with runId {}", workflowId, runId);
            
            return Map.of("workflowRunId", runId);
        } catch (WorkflowExecutionAlreadyStarted e) {
            if (ignoreAlreadyStartedError) {
                String existingRunId = e.getExecution().getRunId();
                log.info("Workflow {} already started with runId {}, ignoring", workflowId, existingRunId);
                return Map.of("workflowRunId", existingRunId);
            }
            throw new WorkflowAlreadyStartedException(workflowId, e.getExecution().getRunId());
        }
    }

    @Override
    public void signalWorkflow(Map<String, Object> request) {
        String workflowId = (String) request.get("workflowId");
        String runId = (String) request.get("workflowRunId");
        String signalChannelName = (String) request.get("signalChannelName");
        Object signalValue = request.get("signalValue");

        clientWrapper.signalWorkflow(workflowId, runId, signalChannelName, signalValue);
    }

    @Override
    public void publishToInternalChannel(Map<String, Object> request) {
        String workflowId = (String) request.get("workflowId");
        String runId = (String) request.get("workflowRunId");
        List<Object> messages = (List<Object>) request.get("messages");

        Map<String, Object> signalValue = Map.of("interStateChannelPublishing", messages);
        clientWrapper.signalWorkflow(workflowId, runId, IwfConstants.SIGNAL_CHANNEL_EXECUTE_RPC, signalValue);
    }

    @Override
    public void stopWorkflow(Map<String, Object> request) {
        String workflowId = (String) request.get("workflowId");
        String runId = (String) request.get("workflowRunId");
        String reason = (String) request.get("reason");
        String stopType = (String) request.get("stopType");

        if (stopType == null) {
            stopType = "CANCEL";
        }

        switch (stopType.toUpperCase()) {
            case "CANCEL" -> clientWrapper.cancelWorkflow(workflowId, runId);
            case "TERMINATE" -> clientWrapper.terminateWorkflow(workflowId, runId, reason != null ? reason : "User requested termination");
            case "FAIL" -> {
                Map<String, Object> failRequest = Map.of("reason", reason != null ? reason : "User requested failure");
                clientWrapper.signalWorkflow(workflowId, runId, IwfConstants.SIGNAL_CHANNEL_FAIL_WORKFLOW, failRequest);
            }
            default -> throw new IllegalArgumentException("Unknown stop type: " + stopType);
        }
    }

    @Override
    public Map<String, Object> getWorkflow(Map<String, Object> request, boolean waitForCompletion) {
        String workflowId = (String) request.get("workflowId");
        String runId = (String) request.get("workflowRunId");
        Boolean needsResults = (Boolean) request.get("needsResults");
        Integer waitTimeSeconds = (Integer) request.get("waitTimeSeconds");

        DescribeWorkflowExecutionResponse describeResp = clientWrapper.describeWorkflow(workflowId, runId);
        WorkflowExecutionInfo info = describeResp.getWorkflowExecutionInfo();
        String actualRunId = info.getExecution().getRunId();
        WorkflowExecutionStatus status = info.getStatus();

        Map<String, Object> response = new HashMap<>();
        response.put("workflowRunId", actualRunId);
        response.put("workflowStatus", mapWorkflowStatus(status));

        if (waitForCompletion || (needsResults != null && needsResults && !isRunning(status))) {
            try {
                long timeout = waitTimeSeconds != null ? waitTimeSeconds : config.getMaxWaitSeconds();
                InterpreterWorkflowOutput output = clientWrapper.getWorkflowResult(
                        workflowId, runId, timeout, TimeUnit.SECONDS);
                
                response.put("workflowStatus", "COMPLETED");
                if (output != null && output.getStateCompletionOutputs() != null) {
                    response.put("results", output.getStateCompletionOutputs());
                }
            } catch (Exception e) {
                handleWorkflowError(e, response);
            }
        }

        return response;
    }

    @Override
    public Map<String, Object> waitForStateCompletion(Map<String, Object> request) {
        // Implementation for wait for state completion
        // This involves starting or connecting to a WaitForStateCompletion workflow
        String workflowId = (String) request.get("workflowId");
        String stateExecutionId = (String) request.get("stateExecutionId");
        String stateId = (String) request.get("stateId");
        String waitForKey = (String) request.get("waitForKey");
        Integer waitTimeSeconds = (Integer) request.get("waitTimeSeconds");

        // Generate the wait workflow ID
        String waitWorkflowId = generateWaitForStateWorkflowId(workflowId, stateExecutionId, waitForKey, stateId);
        
        // Start or get the wait workflow
        Object result = clientWrapper.waitForStateCompletionWorkflow(
                waitWorkflowId,
                config.getTaskQueue(),
                waitTimeSeconds != null ? waitTimeSeconds : config.getMaxWaitSeconds()
        );

        return Map.of("stateCompletionOutput", result != null ? result : Map.of());
    }

    @Override
    public Map<String, Object> searchWorkflows(Map<String, Object> request) {
        String query = (String) request.get("query");
        Integer pageSize = (Integer) request.get("pageSize");
        String nextPageToken = (String) request.get("nextPageToken");

        if (pageSize == null || pageSize <= 0) {
            pageSize = 1000;
        }

        var searchResult = clientWrapper.listWorkflows(query, pageSize, 
                nextPageToken != null ? nextPageToken.getBytes() : null);

        List<Map<String, String>> executions = new ArrayList<>();
        for (WorkflowExecutionInfo exec : searchResult.getExecutionsList()) {
            executions.add(Map.of(
                    "workflowId", exec.getExecution().getWorkflowId(),
                    "workflowRunId", exec.getExecution().getRunId()
            ));
        }

        Map<String, Object> response = new HashMap<>();
        response.put("workflowExecutions", executions);
        if (!searchResult.getNextPageToken().isEmpty()) {
            response.put("nextPageToken", searchResult.getNextPageToken().toStringUtf8());
        }
        return response;
    }

    @Override
    public Map<String, Object> resetWorkflow(Map<String, Object> request) {
        String workflowId = (String) request.get("workflowId");
        String runId = (String) request.get("workflowRunId");
        String resetType = (String) request.get("resetType");
        String reason = (String) request.get("reason");
        
        // Additional reset parameters
        Integer historyEventId = (Integer) request.get("historyEventId");
        String historyEventTime = (String) request.get("historyEventTime");
        String stateId = (String) request.get("stateId");
        String stateExecutionId = (String) request.get("stateExecutionId");
        Boolean skipSignalReapply = (Boolean) request.get("skipSignalReapply");
        Boolean skipUpdateReapply = (Boolean) request.get("skipUpdateReapply");

        String newRunId = clientWrapper.resetWorkflow(
                workflowId, runId, resetType, reason,
                historyEventId, historyEventTime, stateId, stateExecutionId,
                skipSignalReapply, skipUpdateReapply
        );

        return Map.of("workflowRunId", newRunId);
    }

    @Override
    public Map<String, Object> getDataObjects(Map<String, Object> request) {
        String workflowId = (String) request.get("workflowId");
        String runId = (String) request.get("workflowRunId");
        List<String> keys = (List<String>) request.get("keys");
        Boolean useMemoForDataAttributes = (Boolean) request.get("useMemoForDataAttributes");

        List<Object> dataObjects;
        
        if (Boolean.TRUE.equals(useMemoForDataAttributes)) {
            dataObjects = clientWrapper.getDataObjectsFromMemo(workflowId, runId, keys);
        } else {
            dataObjects = clientWrapper.queryDataObjects(workflowId, runId, keys);
        }

        return Map.of("objects", dataObjects);
    }

    @Override
    public void setDataObjects(Map<String, Object> request) {
        String workflowId = (String) request.get("workflowId");
        String runId = (String) request.get("workflowRunId");
        List<Object> objects = (List<Object>) request.get("objects");

        Map<String, Object> signalValue = Map.of("upsertDataObjects", objects);
        clientWrapper.signalWorkflow(workflowId, runId, IwfConstants.SIGNAL_CHANNEL_EXECUTE_RPC, signalValue);
    }

    @Override
    public Map<String, Object> getSearchAttributes(Map<String, Object> request) {
        String workflowId = (String) request.get("workflowId");
        String runId = (String) request.get("workflowRunId");
        List<Object> keys = (List<Object>) request.get("keys");

        List<Object> searchAttributes = clientWrapper.getSearchAttributes(workflowId, runId, keys);
        return Map.of("searchAttributes", searchAttributes);
    }

    @Override
    public void setSearchAttributes(Map<String, Object> request) {
        String workflowId = (String) request.get("workflowId");
        String runId = (String) request.get("workflowRunId");
        List<Object> searchAttributes = (List<Object>) request.get("searchAttributes");

        Map<String, Object> signalValue = Map.of("upsertSearchAttributes", searchAttributes);
        clientWrapper.signalWorkflow(workflowId, runId, IwfConstants.SIGNAL_CHANNEL_EXECUTE_RPC, signalValue);
    }

    @Override
    public Map<String, Object> executeRpc(Map<String, Object> request) {
        String workflowId = (String) request.get("workflowId");
        String runId = (String) request.get("workflowRunId");
        String rpcName = (String) request.get("rpcName");
        Object input = request.get("input");
        Object saLoadingPolicy = request.get("searchAttributesLoadingPolicy");
        Object daLoadingPolicy = request.get("dataAttributesLoadingPolicy");
        Integer timeoutSeconds = (Integer) request.get("timeoutSeconds");

        boolean needsLocking = needsLocking(saLoadingPolicy) || needsLocking(daLoadingPolicy);

        if (needsLocking) {
            // Use workflow update for optimistic locking
            return clientWrapper.executeRpcWithUpdate(workflowId, runId, request);
        } else {
            // Use query + signal for non-locking RPC
            return clientWrapper.executeRpcWithQueryAndSignal(workflowId, runId, request);
        }
    }

    @Override
    public void skipTimer(Map<String, Object> request) {
        String workflowId = (String) request.get("workflowId");
        String runId = (String) request.get("workflowRunId");
        String stateExecutionId = (String) request.get("workflowStateExecutionId");
        String timerCommandId = (String) request.get("timerCommandId");
        Integer timerCommandIndex = (Integer) request.get("timerCommandIndex");

        Map<String, Object> signalValue = new HashMap<>();
        signalValue.put("stateExecutionId", stateExecutionId);
        signalValue.put("commandId", timerCommandId != null ? timerCommandId : "");
        signalValue.put("commandIndex", timerCommandIndex != null ? timerCommandIndex : 0);

        clientWrapper.signalWorkflow(workflowId, runId, IwfConstants.SIGNAL_CHANNEL_SKIP_TIMER, signalValue);
    }

    @Override
    public void updateConfig(Map<String, Object> request) {
        String workflowId = (String) request.get("workflowId");
        String runId = (String) request.get("workflowRunId");
        Object workflowConfig = request.get("workflowConfig");

        clientWrapper.signalWorkflow(workflowId, runId, IwfConstants.SIGNAL_CHANNEL_UPDATE_CONFIG, request);
    }

    @Override
    public void triggerContinueAsNew(Map<String, Object> request) {
        String workflowId = (String) request.get("workflowId");
        String runId = (String) request.get("workflowRunId");

        clientWrapper.signalWorkflow(workflowId, runId, IwfConstants.SIGNAL_CHANNEL_TRIGGER_CONTINUE_AS_NEW, null);
    }

    @Override
    public Map<String, Object> dumpWorkflowInternal(Map<String, Object> request) {
        String workflowId = (String) request.get("workflowId");
        String runId = (String) request.get("workflowRunId");
        Integer pageSizeInBytes = (Integer) request.get("pageSizeInBytes");
        Integer pageNum = (Integer) request.get("pageNum");

        return clientWrapper.queryWorkflowDump(workflowId, runId, 
                pageSizeInBytes != null ? pageSizeInBytes : 1024 * 1024,
                pageNum != null ? pageNum : 0);
    }

    @Override
    public Map<String, Object> healthCheck() {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            hostname = "unknown";
        }
        
        return Map.of(
                "condition", "OK",
                "hostname", hostname,
                "duration", 0
        );
    }

    // Helper methods

    private io.temporal.api.enums.v1.WorkflowIdReusePolicy mapIdReusePolicy(String policy) {
        return switch (policy.toUpperCase()) {
            case "ALLOW_DUPLICATE" -> io.temporal.api.enums.v1.WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE;
            case "ALLOW_DUPLICATE_FAILED_ONLY" -> io.temporal.api.enums.v1.WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY;
            case "REJECT_DUPLICATE" -> io.temporal.api.enums.v1.WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE;
            case "TERMINATE_IF_RUNNING" -> io.temporal.api.enums.v1.WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING;
            default -> io.temporal.api.enums.v1.WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE;
        };
    }

    private String mapWorkflowStatus(WorkflowExecutionStatus status) {
        return switch (status) {
            case WORKFLOW_EXECUTION_STATUS_RUNNING -> "RUNNING";
            case WORKFLOW_EXECUTION_STATUS_COMPLETED -> "COMPLETED";
            case WORKFLOW_EXECUTION_STATUS_FAILED -> "FAILED";
            case WORKFLOW_EXECUTION_STATUS_CANCELED -> "CANCELED";
            case WORKFLOW_EXECUTION_STATUS_TERMINATED -> "TERMINATED";
            case WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW -> "CONTINUED_AS_NEW";
            case WORKFLOW_EXECUTION_STATUS_TIMED_OUT -> "TIMEOUT";
            default -> "UNKNOWN";
        };
    }

    private boolean isRunning(WorkflowExecutionStatus status) {
        return status == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING;
    }

    private void handleWorkflowError(Exception e, Map<String, Object> response) {
        // Handle different error types and populate error response
        String errorType = "SERVER_INTERNAL_ERROR_TYPE";
        String errorMessage = e.getMessage();
        
        response.put("workflowStatus", "FAILED");
        response.put("errorType", errorType);
        response.put("errorMessage", errorMessage);
    }

    private String generateWaitForStateWorkflowId(String parentWorkflowId, String stateExecutionId, 
                                                    String waitForKey, String stateId) {
        StringBuilder sb = new StringBuilder();
        sb.append("iwf-wait-").append(parentWorkflowId);
        
        if (stateExecutionId != null && !stateExecutionId.isEmpty()) {
            sb.append("-").append(stateExecutionId);
        } else if (stateId != null) {
            sb.append("-").append(stateId);
            if (waitForKey != null) {
                sb.append("-").append(waitForKey);
            }
        }
        
        return sb.toString();
    }

    private boolean needsLocking(Object loadingPolicy) {
        if (loadingPolicy instanceof Map) {
            String type = (String) ((Map<String, Object>) loadingPolicy).get("persistenceLoadingType");
            return "LOAD_PARTIAL_WITH_EXCLUSIVE_LOCK".equals(type) || 
                   "LOAD_ALL_WITH_PARTIAL_LOCK".equals(type);
        }
        return false;
    }

    /**
     * Exception for workflow already started.
     */
    public static class WorkflowAlreadyStartedException extends RuntimeException {
        private final String workflowId;
        private final String runId;

        public WorkflowAlreadyStartedException(String workflowId, String runId) {
            super("Workflow " + workflowId + " already started with runId " + runId);
            this.workflowId = workflowId;
            this.runId = runId;
        }

        public String getWorkflowId() {
            return workflowId;
        }

        public String getRunId() {
            return runId;
        }
    }
}
