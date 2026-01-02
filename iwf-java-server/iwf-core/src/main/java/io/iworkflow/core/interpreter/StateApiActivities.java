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

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import java.util.Map;

/**
 * Activities for calling worker state APIs.
 */
@ActivityInterface
public interface StateApiActivities {

    /**
     * Call the worker's waitUntil (start) API.
     * 
     * @param workerUrl URL of the worker
     * @param workflowType iWF workflow type
     * @param stateId state ID being executed
     * @param request the request payload
     * @return response from worker
     */
    @ActivityMethod(name = "StateApiWaitUntil")
    Map<String, Object> callWaitUntilApi(String workerUrl, String workflowType, 
                                          String stateId, Map<String, Object> request);

    /**
     * Call the worker's execute (decide) API.
     * 
     * @param workerUrl URL of the worker
     * @param workflowType iWF workflow type
     * @param stateId state ID being executed
     * @param request the request payload
     * @return response from worker
     */
    @ActivityMethod(name = "StateApiExecute")
    Map<String, Object> callExecuteApi(String workerUrl, String workflowType,
                                        String stateId, Map<String, Object> request);

    /**
     * Call the worker's RPC API.
     * 
     * @param workerUrl URL of the worker
     * @param workflowType iWF workflow type
     * @param rpcName name of the RPC
     * @param request the request payload
     * @return response from worker
     */
    @ActivityMethod(name = "WorkerRpc")
    Map<String, Object> callRpcApi(String workerUrl, String workflowType,
                                    String rpcName, Map<String, Object> request);
}
