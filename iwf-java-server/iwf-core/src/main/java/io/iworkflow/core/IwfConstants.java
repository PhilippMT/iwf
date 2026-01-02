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
package io.iworkflow.core;

/**
 * Constants used throughout the iWF framework.
 */
public final class IwfConstants {

    private IwfConstants() {
        // Utility class
    }

    // Task Queue
    public static final String DEFAULT_TASK_QUEUE = "iwf-task-queue";

    // Search Attribute Keys
    public static final String SEARCH_ATTRIBUTE_IWF_WORKFLOW_TYPE = "IwfWorkflowType";
    public static final String SEARCH_ATTRIBUTE_IWF_GLOBAL_VERSION = "IwfGlobalWorkflowVersion";
    public static final String SEARCH_ATTRIBUTE_IWF_EXECUTING_STATE_IDS = "IwfExecutingStateIds";

    // Memo Keys
    public static final String MEMO_KEY_WORKER_URL = "IwfWorkerUrl";
    public static final String MEMO_KEY_USE_MEMO_FOR_DATA_ATTRIBUTES = "IwfUseMemoForDataAttributes";
    public static final String MEMO_KEY_WORKFLOW_REQUEST_ID = "IwfWorkflowRequestId";
    public static final String IWF_SYSTEM_CONST_PREFIX = "Iwf";

    // Signal Channel Names
    public static final String SIGNAL_CHANNEL_SKIP_TIMER = "IwfSkipTimerSignal";
    public static final String SIGNAL_CHANNEL_FAIL_WORKFLOW = "IwfFailWorkflowSignal";
    public static final String SIGNAL_CHANNEL_EXECUTE_RPC = "IwfExecuteRpcSignal";
    public static final String SIGNAL_CHANNEL_UPDATE_CONFIG = "IwfUpdateConfigSignal";
    public static final String SIGNAL_CHANNEL_TRIGGER_CONTINUE_AS_NEW = "IwfTriggerContinueAsNewSignal";
    public static final String SIGNAL_CHANNEL_STATE_COMPLETION = "IwfStateCompletionSignal";

    // Query Types
    public static final String QUERY_TYPE_GET_DATA_ATTRIBUTES = "IwfGetDataAttributes";
    public static final String QUERY_TYPE_GET_CURRENT_TIMER_INFOS = "IwfGetCurrentTimerInfos";
    public static final String QUERY_TYPE_PREPARE_RPC = "IwfPrepareRpc";
    public static final String QUERY_TYPE_CONTINUE_AS_NEW_DUMP = "IwfContinueAsNewDump";

    // Update Types
    public static final String UPDATE_TYPE_EXECUTE_OPTIMISTIC_LOCKING_RPC = "IwfExecuteOptimisticLockingRpc";

    // Special State IDs for workflow completion
    public static final String STATE_ID_GRACEFUL_COMPLETING_WORKFLOW = "_SYS_GRACEFUL_COMPLETING_WORKFLOW";
    public static final String STATE_ID_FORCE_COMPLETING_WORKFLOW = "_SYS_FORCE_COMPLETING_WORKFLOW";
    public static final String STATE_ID_FORCE_FAILING_WORKFLOW = "_SYS_FORCE_FAILING_WORKFLOW";
    public static final String STATE_ID_DEAD_END = "_SYS_DEAD_END";

    // HTTP Status Codes
    public static final int HTTP_STATUS_SPECIAL_4XX_ERROR_1 = 420;
    public static final int HTTP_STATUS_SPECIAL_4XX_ERROR_2 = 450;

    // Activity Names
    public static final String ACTIVITY_STATE_WAIT_UNTIL = "StateApiWaitUntil";
    public static final String ACTIVITY_STATE_EXECUTE = "StateApiExecute";
    public static final String ACTIVITY_CLEANUP_BLOBSTORE = "CleanupBlobStore";
    public static final String ACTIVITY_DUMP_WORKFLOW_INTERNAL = "DumpWorkflowInternal";

    // Legacy Activity Names (for backward compatibility)
    public static final String ACTIVITY_STATE_START_LEGACY = "StateStart";
    public static final String ACTIVITY_STATE_DECIDE_LEGACY = "StateDecide";

    // Global Workflow Versions
    public static final int GLOBAL_VERSION_CURRENT = 10;
    public static final int GLOBAL_VERSION_RENAMED_STATE_API = 1;
    public static final int GLOBAL_VERSION_CONTINUE_AS_NEW_ON_NO_STATES = 2;
    public static final int GLOBAL_VERSION_OPTIMIZED_UPSERT_SEARCH_ATTRIBUTE = 3;
    public static final int GLOBAL_VERSION_YIELD_ON_CONDITIONAL_COMPLETE = 4;
    public static final int GLOBAL_VERSION_WAITING_COMMAND_THREADS = 5;
}
