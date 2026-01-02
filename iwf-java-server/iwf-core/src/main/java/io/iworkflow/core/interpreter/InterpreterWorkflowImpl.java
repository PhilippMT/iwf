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

import io.iworkflow.core.IwfConstants;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.*;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of the Interpreter Workflow.
 * 
 * <p>This is the core workflow that executes iWF workflows. It maintains state,
 * processes commands, and coordinates state transitions.</p>
 */
public class InterpreterWorkflowImpl implements InterpreterWorkflow {

    private static final Logger logger = Workflow.getLogger(InterpreterWorkflowImpl.class);

    // Workflow state
    private String iwfWorkflowType;
    private String iwfWorkerUrl;
    private WorkflowConfig config;
    private boolean useMemoForDataAttributes;

    // Persistence
    private final Map<String, Object> dataObjects = new ConcurrentHashMap<>();
    private final Map<String, Object> searchAttributes = new ConcurrentHashMap<>();
    
    // State execution tracking
    private final List<StateExecution> pendingStateExecutions = Collections.synchronizedList(new ArrayList<>());
    private final List<InterpreterWorkflowOutput.StateCompletionOutput> completionOutputs = 
            Collections.synchronizedList(new ArrayList<>());
    private final Map<String, List<TimerInfo>> currentTimerInfos = new ConcurrentHashMap<>();
    
    // Internal channels
    private final Map<String, List<Object>> internalChannels = new ConcurrentHashMap<>();
    
    // Counters
    private final AtomicInteger stateExecutionCounter = new AtomicInteger(0);
    private final AtomicInteger signalCounter = new AtomicInteger(0);
    
    // Wait for state completion
    private final Set<String> waitForCompletionStateExecutionIds = ConcurrentHashMap.newKeySet();
    private final Set<String> waitForCompletionStateIds = ConcurrentHashMap.newKeySet();
    
    // Continue-as-new
    private boolean shouldContinueAsNew = false;
    private boolean continueAsNewTriggered = false;

    // Activities
    private final StateApiActivities stateApiActivities;

    public InterpreterWorkflowImpl() {
        ActivityOptions activityOptions = ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofSeconds(30))
                .setRetryOptions(RetryOptions.newBuilder()
                        .setInitialInterval(Duration.ofSeconds(1))
                        .setMaximumAttempts(3)
                        .build())
                .build();
        
        this.stateApiActivities = Workflow.newActivityStub(StateApiActivities.class, activityOptions);
    }

    @Override
    public InterpreterWorkflowOutput execute(InterpreterWorkflowInput input) {
        logger.info("Starting interpreter workflow for type: {}", input.getIwfWorkflowType());
        
        // Initialize workflow state
        this.iwfWorkflowType = input.getIwfWorkflowType();
        this.iwfWorkerUrl = input.getIwfWorkerUrl();
        this.config = input.getConfig() != null ? input.getConfig() : new WorkflowConfig();
        this.useMemoForDataAttributes = input.isUseMemoForDataAttributes();
        
        // Initialize wait for completion sets
        if (input.getWaitForCompletionStateExecutionIds() != null) {
            waitForCompletionStateExecutionIds.addAll(input.getWaitForCompletionStateExecutionIds());
        }
        if (input.getWaitForCompletionStateIds() != null) {
            waitForCompletionStateIds.addAll(input.getWaitForCompletionStateIds());
        }
        
        // Initialize data and search attributes
        initializePersistence(input);
        
        // Handle resume from continue-as-new
        if (input.isResumeFromContinueAsNew() && input.getContinueAsNewInput() != null) {
            restoreStateFromContinueAsNew(input.getContinueAsNewInput());
        }
        
        // Start initial state if provided
        if (input.getStartStateId() != null && !input.getStartStateId().isEmpty()) {
            scheduleStateExecution(input.getStartStateId(), input.getStateInput(), input.getStateOptions(), null);
        }
        
        // Main workflow loop
        while (true) {
            // Check for continue-as-new
            if (shouldTriggerContinueAsNew()) {
                return executeContinueAsNew();
            }
            
            // Process pending state executions
            if (!pendingStateExecutions.isEmpty()) {
                StateExecution stateExec = pendingStateExecutions.remove(0);
                processStateExecution(stateExec);
            }
            
            // Wait for signals or workflow completion
            boolean signalReceived = Workflow.await(
                    Duration.ofSeconds(1),
                    () -> !pendingStateExecutions.isEmpty() || 
                          shouldContinueAsNew || 
                          continueAsNewTriggered ||
                          isWorkflowComplete()
            );
            
            // Check for workflow completion
            if (isWorkflowComplete() && pendingStateExecutions.isEmpty()) {
                break;
            }
        }
        
        logger.info("Workflow completed with {} completion outputs", completionOutputs.size());
        return InterpreterWorkflowOutput.builder()
                .stateCompletionOutputs(new ArrayList<>(completionOutputs))
                .build();
    }

    private void initializePersistence(InterpreterWorkflowInput input) {
        // Initialize search attributes
        if (input.getInitSearchAttributes() != null) {
            for (Object sa : input.getInitSearchAttributes()) {
                if (sa instanceof Map) {
                    Map<String, Object> saMap = (Map<String, Object>) sa;
                    String key = (String) saMap.get("key");
                    if (key != null) {
                        searchAttributes.put(key, saMap);
                    }
                }
            }
        }
        
        // Initialize data attributes
        if (input.getInitDataAttributes() != null) {
            for (Object da : input.getInitDataAttributes()) {
                if (da instanceof Map) {
                    Map<String, Object> daMap = (Map<String, Object>) da;
                    String key = (String) daMap.get("key");
                    if (key != null) {
                        dataObjects.put(key, daMap.get("value"));
                    }
                }
            }
        }
    }

    private void scheduleStateExecution(String stateId, Object stateInput, Object stateOptions, String waitForKey) {
        int execNum = stateExecutionCounter.incrementAndGet();
        String stateExecutionId = stateId + "-" + execNum;
        
        StateExecution stateExec = StateExecution.builder()
                .stateId(stateId)
                .stateExecutionId(stateExecutionId)
                .stateInput(stateInput)
                .stateOptions(stateOptions)
                .waitForKey(waitForKey)
                .build();
        
        pendingStateExecutions.add(stateExec);
        logger.debug("Scheduled state execution: {}", stateExecutionId);
    }

    private void processStateExecution(StateExecution stateExec) {
        logger.info("Processing state execution: {}", stateExec.getStateExecutionId());
        
        // Check for system states
        if (isSystemState(stateExec.getStateId())) {
            handleSystemState(stateExec);
            return;
        }
        
        // Check if we should skip waitUntil
        boolean skipWaitUntil = shouldSkipWaitUntil(stateExec.getStateOptions());
        
        if (!skipWaitUntil) {
            // Execute waitUntil (start) API
            try {
                WaitUntilResult waitResult = executeWaitUntil(stateExec);
                
                if (waitResult.getCommandRequest() != null) {
                    // Wait for commands to complete
                    processCommands(stateExec, waitResult);
                }
            } catch (Exception e) {
                logger.error("WaitUntil failed for state: {}", stateExec.getStateId(), e);
                handleStateApiFailure(stateExec, e, true);
                return;
            }
        }
        
        // Execute execute (decide) API
        try {
            ExecuteResult executeResult = executeExecute(stateExec);
            
            // Process state decision
            if (executeResult.getStateDecision() != null) {
                processStateDecision(stateExec, executeResult.getStateDecision());
            }
        } catch (Exception e) {
            logger.error("Execute failed for state: {}", stateExec.getStateId(), e);
            handleStateApiFailure(stateExec, e, false);
        }
    }

    private boolean isSystemState(String stateId) {
        return stateId.startsWith("_SYS_");
    }

    private void handleSystemState(StateExecution stateExec) {
        switch (stateExec.getStateId()) {
            case IwfConstants.STATE_ID_GRACEFUL_COMPLETING_WORKFLOW:
            case IwfConstants.STATE_ID_FORCE_COMPLETING_WORKFLOW:
                // Record completion output
                addCompletionOutput(stateExec);
                break;
            case IwfConstants.STATE_ID_FORCE_FAILING_WORKFLOW:
                throw new WorkflowFailureException(stateExec);
            case IwfConstants.STATE_ID_DEAD_END:
                // Do nothing - this state just ends the branch
                break;
        }
    }

    private WaitUntilResult executeWaitUntil(StateExecution stateExec) {
        // Build request for worker
        Map<String, Object> request = buildWorkerRequest(stateExec);
        
        // Call worker via activity
        Map<String, Object> response = stateApiActivities.callWaitUntilApi(
                iwfWorkerUrl,
                iwfWorkflowType,
                stateExec.getStateId(),
                request
        );
        
        // Process upserts
        processUpserts(response);
        
        return WaitUntilResult.fromResponse(response);
    }

    private ExecuteResult executeExecute(StateExecution stateExec) {
        // Build request for worker
        Map<String, Object> request = buildWorkerRequest(stateExec);
        
        // Call worker via activity
        Map<String, Object> response = stateApiActivities.callExecuteApi(
                iwfWorkerUrl,
                iwfWorkflowType,
                stateExec.getStateId(),
                request
        );
        
        // Process upserts
        processUpserts(response);
        
        return ExecuteResult.fromResponse(response);
    }

    private Map<String, Object> buildWorkerRequest(StateExecution stateExec) {
        Map<String, Object> request = new HashMap<>();
        
        // Context
        Map<String, Object> context = new HashMap<>();
        context.put("workflowId", Workflow.getInfo().getWorkflowId());
        context.put("workflowRunId", Workflow.getInfo().getRunId());
        context.put("workflowStartedTimestamp", Workflow.getInfo().getRunStartedTimestampMillis());
        context.put("stateExecutionId", stateExec.getStateExecutionId());
        context.put("attempt", 1);
        request.put("context", context);
        
        request.put("workflowType", iwfWorkflowType);
        request.put("workflowStateId", stateExec.getStateId());
        request.put("stateInput", stateExec.getStateInput());
        request.put("searchAttributes", getSearchAttributesForWorker());
        request.put("dataObjects", getDataObjectsForWorker(null));
        
        return request;
    }

    private void processUpserts(Map<String, Object> response) {
        // Upsert search attributes
        List<Object> saUpserts = (List<Object>) response.get("upsertSearchAttributes");
        if (saUpserts != null) {
            for (Object sa : saUpserts) {
                if (sa instanceof Map) {
                    Map<String, Object> saMap = (Map<String, Object>) sa;
                    String key = (String) saMap.get("key");
                    if (key != null) {
                        searchAttributes.put(key, saMap);
                    }
                }
            }
        }
        
        // Upsert data objects
        List<Object> daUpserts = (List<Object>) response.get("upsertDataObjects");
        if (daUpserts != null) {
            for (Object da : daUpserts) {
                if (da instanceof Map) {
                    Map<String, Object> daMap = (Map<String, Object>) da;
                    String key = (String) daMap.get("key");
                    if (key != null) {
                        dataObjects.put(key, daMap.get("value"));
                    }
                }
            }
        }
        
        // Publish to internal channels
        List<Object> channelPublishes = (List<Object>) response.get("publishToInterStateChannel");
        if (channelPublishes != null) {
            for (Object pub : channelPublishes) {
                if (pub instanceof Map) {
                    Map<String, Object> pubMap = (Map<String, Object>) pub;
                    String channelName = (String) pubMap.get("channelName");
                    Object value = pubMap.get("value");
                    if (channelName != null) {
                        internalChannels.computeIfAbsent(channelName, k -> Collections.synchronizedList(new ArrayList<>()))
                                .add(value);
                    }
                }
            }
        }
    }

    private void processCommands(StateExecution stateExec, WaitUntilResult waitResult) {
        // Process timer commands
        List<Object> timerCommands = waitResult.getTimerCommands();
        if (timerCommands != null && !timerCommands.isEmpty()) {
            for (Object timerCmd : timerCommands) {
                processTimerCommand(stateExec, (Map<String, Object>) timerCmd);
            }
        }
        
        // Process signal commands
        List<Object> signalCommands = waitResult.getSignalCommands();
        if (signalCommands != null && !signalCommands.isEmpty()) {
            for (Object signalCmd : signalCommands) {
                processSignalCommand(stateExec, (Map<String, Object>) signalCmd);
            }
        }
        
        // Process internal channel commands
        List<Object> channelCommands = waitResult.getInterStateChannelCommands();
        if (channelCommands != null && !channelCommands.isEmpty()) {
            for (Object channelCmd : channelCommands) {
                processInternalChannelCommand(stateExec, (Map<String, Object>) channelCmd);
            }
        }
    }

    private void processTimerCommand(StateExecution stateExec, Map<String, Object> timerCmd) {
        String commandId = (String) timerCmd.get("commandId");
        Long firingTimestamp = (Long) timerCmd.get("firingUnixTimestampSeconds");
        Long durationSeconds = (Long) timerCmd.get("durationSeconds");
        
        Duration duration;
        if (firingTimestamp != null) {
            long now = Workflow.currentTimeMillis() / 1000;
            long waitTime = firingTimestamp - now;
            duration = Duration.ofSeconds(Math.max(0, waitTime));
        } else if (durationSeconds != null) {
            duration = Duration.ofSeconds(durationSeconds);
        } else {
            duration = Duration.ZERO;
        }
        
        // Record timer info
        TimerInfo timerInfo = new TimerInfo(commandId, currentTimerInfos.size(), 
                System.currentTimeMillis() / 1000 + duration.getSeconds(), "SCHEDULED");
        currentTimerInfos.computeIfAbsent(stateExec.getStateExecutionId(), k -> new ArrayList<>())
                .add(timerInfo);
        
        // Wait for timer using Workflow.sleep
        Workflow.sleep(duration);
    }

    private void processSignalCommand(StateExecution stateExec, Map<String, Object> signalCmd) {
        String channelName = (String) signalCmd.get("signalChannelName");
        String commandId = (String) signalCmd.get("commandId");
        Integer atLeast = (Integer) signalCmd.get("atLeast");
        Integer atMost = (Integer) signalCmd.get("atMost");
        
        int minRequired = atLeast != null ? atLeast : 1;
        int maxAllowed = atMost != null ? atMost : minRequired;
        
        // Create a signal channel using Temporal's workflow semantics
        // In Temporal workflows, signals are received via @SignalMethod handlers
        // Here we store the signal requirement and the command results will be
        // populated when signals are received via the workflow's signal handler
        
        stateExec.getSignalRequirements().put(channelName, 
                new SignalRequirement(commandId, channelName, minRequired, maxAllowed, new ArrayList<>()));
        
        // Wait for required signals to arrive
        Workflow.await(() -> {
            SignalRequirement req = stateExec.getSignalRequirements().get(channelName);
            return req != null && req.getReceivedValues().size() >= req.getMinRequired();
        });
    }

    private void processInternalChannelCommand(StateExecution stateExec, Map<String, Object> channelCmd) {
        String channelName = (String) channelCmd.get("channelName");
        
        // Atomically check and get message from channel
        Object message = null;
        
        // First check if message already available
        List<Object> messages = internalChannels.get(channelName);
        if (messages != null && !messages.isEmpty()) {
            message = messages.remove(0);
        } else {
            // Wait for message to arrive, then retrieve it atomically
            Workflow.await(() -> {
                List<Object> msgs = internalChannels.get(channelName);
                return msgs != null && !msgs.isEmpty();
            });
            
            // Get the channel again after await and remove message
            messages = internalChannels.get(channelName);
            if (messages != null && !messages.isEmpty()) {
                message = messages.remove(0);
            }
        }
        
        // Store the received message in command results
        if (message != null) {
            stateExec.getChannelResults().put(channelName, message);
        }
    }

    private void processStateDecision(StateExecution stateExec, Object stateDecision) {
        if (!(stateDecision instanceof Map)) {
            return;
        }
        
        Map<String, Object> decision = (Map<String, Object>) stateDecision;
        
        // Process next states
        List<Object> nextStates = (List<Object>) decision.get("nextStates");
        if (nextStates != null) {
            for (Object nextState : nextStates) {
                if (nextState instanceof Map) {
                    Map<String, Object> ns = (Map<String, Object>) nextState;
                    String stateId = (String) ns.get("stateId");
                    Object stateInput = ns.get("stateInput");
                    Object stateOptions = ns.get("stateOptions");
                    String waitForKey = (String) ns.get("waitForKey");
                    
                    scheduleStateExecution(stateId, stateInput, stateOptions, waitForKey);
                }
            }
        }
        
        // Handle wait for completion notification
        if (waitForCompletionStateExecutionIds.contains(stateExec.getStateExecutionId()) ||
            waitForCompletionStateIds.contains(stateExec.getStateId())) {
            signalWaitForCompletion(stateExec);
        }
        
        // Record completion if needed
        if (shouldRecordCompletion(stateExec)) {
            addCompletionOutput(stateExec);
        }
    }

    private void addCompletionOutput(StateExecution stateExec) {
        completionOutputs.add(InterpreterWorkflowOutput.StateCompletionOutput.builder()
                .completedStateId(stateExec.getStateId())
                .completedStateExecutionId(stateExec.getStateExecutionId())
                .completedStateOutput(stateExec.getStateInput())
                .build());
    }

    private void signalWaitForCompletion(StateExecution stateExec) {
        // Signal any waiting workflows about state completion
        // This would use Temporal's signalExternalWorkflow or similar
    }

    private boolean shouldRecordCompletion(StateExecution stateExec) {
        return stateExec.getStateId().equals(IwfConstants.STATE_ID_GRACEFUL_COMPLETING_WORKFLOW) ||
               stateExec.getStateId().equals(IwfConstants.STATE_ID_FORCE_COMPLETING_WORKFLOW);
    }

    private boolean shouldSkipWaitUntil(Object stateOptions) {
        if (stateOptions instanceof Map) {
            return Boolean.TRUE.equals(((Map<String, Object>) stateOptions).get("skipWaitUntil"));
        }
        return false;
    }

    private void handleStateApiFailure(StateExecution stateExec, Exception e, boolean isWaitUntil) {
        // Handle based on failure policy
        logger.error("State API failure for {}: {}", stateExec.getStateExecutionId(), e.getMessage());
        // Could retry, proceed to next state, or fail workflow based on policy
    }

    private List<Object> getSearchAttributesForWorker() {
        return new ArrayList<>(searchAttributes.values());
    }

    private List<Object> getDataObjectsForWorker(List<String> keys) {
        List<Object> result = new ArrayList<>();
        Set<String> keySet = keys != null ? new HashSet<>(keys) : null;
        
        for (Map.Entry<String, Object> entry : dataObjects.entrySet()) {
            if (keySet == null || keySet.isEmpty() || keySet.contains(entry.getKey())) {
                result.add(Map.of("key", entry.getKey(), "value", entry.getValue()));
            }
        }
        
        return result;
    }

    private boolean shouldTriggerContinueAsNew() {
        return continueAsNewTriggered || 
               (config.getContinueAsNewThreshold() > 0 && 
                stateExecutionCounter.get() >= config.getContinueAsNewThreshold());
    }

    private InterpreterWorkflowOutput executeContinueAsNew() {
        // Prepare continue-as-new input
        InterpreterWorkflowInput newInput = InterpreterWorkflowInput.builder()
                .iwfWorkflowType(iwfWorkflowType)
                .iwfWorkerUrl(iwfWorkerUrl)
                .config(config)
                .useMemoForDataAttributes(useMemoForDataAttributes)
                .isResumeFromContinueAsNew(true)
                .continueAsNewInput(InterpreterWorkflowInput.ContinueAsNewInput.builder()
                        .previousInternalRunId(Workflow.getInfo().getRunId())
                        .build())
                .build();
        
        Workflow.continueAsNew(newInput);
        return null; // Never reached
    }

    private void restoreStateFromContinueAsNew(InterpreterWorkflowInput.ContinueAsNewInput continueAsNewInput) {
        // Restore state from previous run
        // This would query the previous workflow for its state
        logger.info("Restoring state from previous run: {}", continueAsNewInput.getPreviousInternalRunId());
    }

    private boolean isWorkflowComplete() {
        // Workflow is complete when there are no pending state executions
        // OR when no initial state was provided (empty workflow)
        return pendingStateExecutions.isEmpty();
    }

    // Query Handlers

    @Override
    public GetDataAttributesResponse getDataAttributes(GetDataAttributesRequest request) {
        List<GetDataAttributesResponse.KeyValue> attributes = new ArrayList<>();
        Set<String> requestedKeys = request.getKeys() != null ? new HashSet<>(request.getKeys()) : null;
        
        for (Map.Entry<String, Object> entry : dataObjects.entrySet()) {
            if (requestedKeys == null || requestedKeys.isEmpty() || requestedKeys.contains(entry.getKey())) {
                attributes.add(GetDataAttributesResponse.KeyValue.builder()
                        .key(entry.getKey())
                        .value(entry.getValue())
                        .build());
            }
        }
        
        return GetDataAttributesResponse.builder()
                .dataAttributes(attributes)
                .build();
    }

    @Override
    public GetCurrentTimerInfosResponse getCurrentTimerInfos() {
        Map<String, List<GetCurrentTimerInfosResponse.TimerInfo>> result = new HashMap<>();
        
        for (Map.Entry<String, List<TimerInfo>> entry : currentTimerInfos.entrySet()) {
            List<GetCurrentTimerInfosResponse.TimerInfo> timerInfos = new ArrayList<>();
            for (TimerInfo ti : entry.getValue()) {
                timerInfos.add(GetCurrentTimerInfosResponse.TimerInfo.builder()
                        .commandId(ti.commandId)
                        .commandIndex(ti.commandIndex)
                        .firingUnixTimestampSeconds(ti.firingUnixTimestampSeconds)
                        .status(ti.status)
                        .build());
            }
            result.put(entry.getKey(), timerInfos);
        }
        
        return GetCurrentTimerInfosResponse.builder()
                .stateExecutionCurrentTimerInfos(result)
                .build();
    }

    @Override
    public PrepareRpcResponse prepareRpc(PrepareRpcRequest request) {
        Map<String, Integer> signalInfos = new HashMap<>();
        Map<String, Integer> internalInfos = new HashMap<>();
        
        for (Map.Entry<String, List<Object>> entry : internalChannels.entrySet()) {
            internalInfos.put(entry.getKey(), entry.getValue().size());
        }
        
        return PrepareRpcResponse.builder()
                .iwfWorkflowType(iwfWorkflowType)
                .iwfWorkerUrl(iwfWorkerUrl)
                .searchAttributes(getSearchAttributesForWorker())
                .dataAttributes(getDataObjectsForWorker(null))
                .signalChannelInfos(signalInfos)
                .internalChannelInfos(internalInfos)
                .build();
    }

    @Override
    public ContinueAsNewDumpResponse continueAsNewDump(ContinueAsNewDumpRequest request) {
        // Serialize workflow state
        String jsonData = "{}"; // Would serialize actual state
        return ContinueAsNewDumpResponse.builder()
                .checksum(String.valueOf(jsonData.hashCode()))
                .totalPages(1)
                .jsonData(jsonData)
                .build();
    }

    // Signal Handlers

    @Override
    public void skipTimer(SkipTimerRequest request) {
        // Find and cancel the timer
        List<TimerInfo> timers = currentTimerInfos.get(request.getStateExecutionId());
        if (timers != null) {
            for (TimerInfo timer : timers) {
                if ((request.getCommandId() != null && request.getCommandId().equals(timer.commandId)) ||
                    request.getCommandIndex() == timer.commandIndex) {
                    timer.status = "SKIPPED";
                    break;
                }
            }
        }
    }

    @Override
    public void failWorkflow(FailWorkflowRequest request) {
        throw new WorkflowFailureException(request.getReason());
    }

    @Override
    public void executeRpcSignal(ExecuteRpcSignalRequest request) {
        // Process RPC mutations
        if (request.getUpsertDataObjects() != null) {
            for (Object da : request.getUpsertDataObjects()) {
                if (da instanceof Map) {
                    Map<String, Object> daMap = (Map<String, Object>) da;
                    String key = (String) daMap.get("key");
                    if (key != null) {
                        dataObjects.put(key, daMap.get("value"));
                    }
                }
            }
        }
        
        if (request.getUpsertSearchAttributes() != null) {
            for (Object sa : request.getUpsertSearchAttributes()) {
                if (sa instanceof Map) {
                    Map<String, Object> saMap = (Map<String, Object>) sa;
                    String key = (String) saMap.get("key");
                    if (key != null) {
                        searchAttributes.put(key, saMap);
                    }
                }
            }
        }
        
        if (request.getInterStateChannelPublishing() != null) {
            for (Object pub : request.getInterStateChannelPublishing()) {
                if (pub instanceof Map) {
                    Map<String, Object> pubMap = (Map<String, Object>) pub;
                    String channelName = (String) pubMap.get("channelName");
                    Object value = pubMap.get("value");
                    if (channelName != null) {
                        internalChannels.computeIfAbsent(channelName, k -> Collections.synchronizedList(new ArrayList<>()))
                                .add(value);
                    }
                }
            }
        }
        
        // Process state decision from RPC
        if (request.getStateDecision() != null) {
            processStateDecision(null, request.getStateDecision());
        }
    }

    @Override
    public void updateConfig(WorkflowConfigSignal configSignal) {
        if (configSignal.getWorkflowConfig() != null) {
            WorkflowConfig newConfig = configSignal.getWorkflowConfig();
            if (newConfig.getContinueAsNewThreshold() != null) {
                config.setContinueAsNewThreshold(newConfig.getContinueAsNewThreshold());
            }
            if (newConfig.getOptimizeActivity() != null) {
                config.setOptimizeActivity(newConfig.getOptimizeActivity());
            }
            if (newConfig.getOptimizeTimer() != null) {
                config.setOptimizeTimer(newConfig.getOptimizeTimer());
            }
        }
    }

    @Override
    public void triggerContinueAsNew() {
        continueAsNewTriggered = true;
    }

    // Update Handler

    @Override
    public ExecuteRpcUpdateResponse executeOptimisticLockingRpc(ExecuteRpcUpdateRequest request) {
        // Execute RPC with locking
        // This allows atomic read-modify-write operations
        
        try {
            // Prepare RPC data
            PrepareRpcResponse prepData = prepareRpc(PrepareRpcRequest.builder()
                    .dataObjectsLoadingPolicy(request.getDataAttributesLoadingPolicy())
                    .searchAttributesLoadingPolicy(request.getSearchAttributesLoadingPolicy())
                    .build());
            
            // Call worker RPC API
            Map<String, Object> workerRequest = new HashMap<>();
            workerRequest.put("rpcName", request.getRpcName());
            workerRequest.put("input", request.getInput());
            workerRequest.put("searchAttributes", prepData.getSearchAttributes());
            workerRequest.put("dataAttributes", prepData.getDataAttributes());
            
            Map<String, Object> workerResponse = stateApiActivities.callRpcApi(
                    iwfWorkerUrl,
                    iwfWorkflowType,
                    request.getRpcName(),
                    workerRequest
            );
            
            // Process mutations
            processUpserts(workerResponse);
            
            // Process state decision
            Object stateDecision = workerResponse.get("stateDecision");
            if (stateDecision != null) {
                processStateDecision(null, stateDecision);
            }
            
            return ExecuteRpcUpdateResponse.builder()
                    .output(workerResponse.get("output"))
                    .build();
        } catch (Exception e) {
            return ExecuteRpcUpdateResponse.builder()
                    .statusError(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    // Helper classes

    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    private static class StateExecution {
        private String stateId;
        private String stateExecutionId;
        private Object stateInput;
        private Object stateOptions;
        private String waitForKey;
        @lombok.Builder.Default
        private Map<String, SignalRequirement> signalRequirements = new HashMap<>();
        @lombok.Builder.Default
        private Map<String, Object> channelResults = new HashMap<>();
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    private static class TimerInfo {
        private String commandId;
        private int commandIndex;
        private long firingUnixTimestampSeconds;
        private String status;
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    private static class SignalRequirement {
        private String commandId;
        private String channelName;
        private int minRequired;
        private int maxAllowed;
        private List<Object> receivedValues;
    }

    @lombok.Data
    @lombok.Builder
    private static class WaitUntilResult {
        private Object commandRequest;
        private List<Object> timerCommands;
        private List<Object> signalCommands;
        private List<Object> interStateChannelCommands;
        
        static WaitUntilResult fromResponse(Map<String, Object> response) {
            Map<String, Object> cmdRequest = (Map<String, Object>) response.get("commandRequest");
            return WaitUntilResult.builder()
                    .commandRequest(cmdRequest)
                    .timerCommands(cmdRequest != null ? (List<Object>) cmdRequest.get("timerCommands") : null)
                    .signalCommands(cmdRequest != null ? (List<Object>) cmdRequest.get("signalCommands") : null)
                    .interStateChannelCommands(cmdRequest != null ? (List<Object>) cmdRequest.get("interStateChannelCommands") : null)
                    .build();
        }
    }

    @lombok.Data
    @lombok.Builder
    private static class ExecuteResult {
        private Object stateDecision;
        
        static ExecuteResult fromResponse(Map<String, Object> response) {
            return ExecuteResult.builder()
                    .stateDecision(response.get("stateDecision"))
                    .build();
        }
    }

    public static class WorkflowFailureException extends RuntimeException {
        public WorkflowFailureException(String reason) {
            super(reason);
        }
        
        public WorkflowFailureException(StateExecution stateExec) {
            super("Workflow failed at state: " + stateExec.getStateId());
        }
    }
}
