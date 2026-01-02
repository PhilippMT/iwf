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
import java.util.Map;

// Query/Signal/Update DTOs for InterpreterWorkflow

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class GetDataAttributesRequest {
    private List<String> keys;
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class GetDataAttributesResponse {
    private List<KeyValue> dataAttributes;
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class KeyValue {
        private String key;
        private Object value;
    }
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class GetCurrentTimerInfosResponse {
    private Map<String, List<TimerInfo>> stateExecutionCurrentTimerInfos;
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TimerInfo {
        private String commandId;
        private int commandIndex;
        private long firingUnixTimestampSeconds;
        private String status;
    }
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class PrepareRpcRequest {
    private Object dataObjectsLoadingPolicy;
    private Object searchAttributesLoadingPolicy;
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class PrepareRpcResponse {
    private String iwfWorkflowType;
    private String iwfWorkerUrl;
    private List<Object> searchAttributes;
    private List<Object> dataAttributes;
    private Map<String, Integer> signalChannelInfos;
    private Map<String, Integer> internalChannelInfos;
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class ContinueAsNewDumpRequest {
    private int pageSizeInBytes;
    private int pageNum;
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class ContinueAsNewDumpResponse {
    private String checksum;
    private int totalPages;
    private String jsonData;
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class SkipTimerRequest {
    private String stateExecutionId;
    private String commandId;
    private int commandIndex;
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class FailWorkflowRequest {
    private String reason;
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class ExecuteRpcSignalRequest {
    private Object rpcInput;
    private Object rpcOutput;
    private List<Object> upsertDataObjects;
    private List<Object> upsertSearchAttributes;
    private Object stateDecision;
    private List<Object> recordEvents;
    private List<Object> interStateChannelPublishing;
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class WorkflowConfigSignal {
    private WorkflowConfig workflowConfig;
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class ExecuteRpcUpdateRequest {
    private String rpcName;
    private Object input;
    private Object searchAttributesLoadingPolicy;
    private Object dataAttributesLoadingPolicy;
    private Integer timeoutSeconds;
    private boolean useMemoForDataAttributes;
    private List<Object> searchAttributes;
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class ExecuteRpcUpdateResponse {
    private Object output;
    private Object statusError;
}
