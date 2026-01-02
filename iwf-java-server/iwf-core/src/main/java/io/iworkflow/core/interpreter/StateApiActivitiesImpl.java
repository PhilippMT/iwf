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

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of StateApiActivities that calls worker HTTP endpoints.
 */
@Slf4j
@Component
public class StateApiActivitiesImpl implements StateApiActivities {

    private final RestTemplate restTemplate;

    public StateApiActivitiesImpl() {
        this.restTemplate = new RestTemplate();
    }

    @Override
    public Map<String, Object> callWaitUntilApi(String workerUrl, String workflowType,
                                                  String stateId, Map<String, Object> request) {
        String url = buildUrl(workerUrl, "/api/v1/workflowState/start");
        log.debug("Calling waitUntil API at {} for state {}", url, stateId);
        
        return callWorkerApi(url, request);
    }

    @Override
    public Map<String, Object> callExecuteApi(String workerUrl, String workflowType,
                                               String stateId, Map<String, Object> request) {
        String url = buildUrl(workerUrl, "/api/v1/workflowState/decide");
        log.debug("Calling execute API at {} for state {}", url, stateId);
        
        return callWorkerApi(url, request);
    }

    @Override
    public Map<String, Object> callRpcApi(String workerUrl, String workflowType,
                                           String rpcName, Map<String, Object> request) {
        String url = buildUrl(workerUrl, "/api/v1/workflowWorker/rpc");
        log.debug("Calling RPC API at {} for RPC {}", url, rpcName);
        
        return callWorkerApi(url, request);
    }

    private Map<String, Object> callWorkerApi(String url, Map<String, Object> request) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);
        
        try {
            ResponseEntity<Map> response = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    entity,
                    Map.class
            );
            
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return new HashMap<>(response.getBody());
            } else {
                throw new WorkerApiException("Worker API returned non-success status: " + 
                        response.getStatusCode());
            }
        } catch (Exception e) {
            log.error("Worker API call failed: {}", e.getMessage(), e);
            throw new WorkerApiException("Worker API call failed: " + e.getMessage(), e);
        }
    }

    private String buildUrl(String workerUrl, String path) {
        String baseUrl = workerUrl.endsWith("/") ? workerUrl.substring(0, workerUrl.length() - 1) : workerUrl;
        return baseUrl + path;
    }

    public static class WorkerApiException extends RuntimeException {
        public WorkerApiException(String message) {
            super(message);
        }
        
        public WorkerApiException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
