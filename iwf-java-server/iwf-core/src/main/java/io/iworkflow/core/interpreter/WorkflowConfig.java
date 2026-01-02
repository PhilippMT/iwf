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

/**
 * Workflow configuration that can be dynamically updated.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowConfig {
    
    /**
     * Threshold for continue-as-new (number of state executions)
     */
    private Integer continueAsNewThreshold;
    
    /**
     * Page size in bytes for continue-as-new data
     */
    private Integer continueAsNewPageSizeInBytes;
    
    /**
     * Whether to disable system search attributes
     */
    private Boolean disableSystemSearchAttribute;
    
    /**
     * Mode for executing state IDs search attribute
     */
    private String executingStateIdMode;
    
    /**
     * Whether to optimize activity execution
     */
    private Boolean optimizeActivity;
    
    /**
     * Whether to optimize timer handling
     */
    private Boolean optimizeTimer;
    
    // Helper methods with defaults
    
    public int getContinueAsNewThreshold() {
        return continueAsNewThreshold != null ? continueAsNewThreshold : 100;
    }
    
    public int getContinueAsNewPageSizeInBytes() {
        return continueAsNewPageSizeInBytes != null ? continueAsNewPageSizeInBytes : 1024 * 1024;
    }
    
    public boolean getDisableSystemSearchAttribute() {
        return disableSystemSearchAttribute != null && disableSystemSearchAttribute;
    }
    
    public boolean getOptimizeActivity() {
        return optimizeActivity == null || optimizeActivity;
    }
    
    public boolean getOptimizeTimer() {
        return optimizeTimer == null || optimizeTimer;
    }
}
