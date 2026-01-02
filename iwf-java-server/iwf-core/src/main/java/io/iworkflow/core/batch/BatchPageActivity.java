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
package io.iworkflow.core.batch;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Activity interface for processing batch pages.
 * 
 * <p>This activity is called by the BatchOrchestratorWorkflow for each page.
 * It delegates to the registered page processor and handles the pipelined
 * pagination pattern.</p>
 */
@ActivityInterface
public interface BatchPageActivity {
    
    /**
     * Process a single page of the batch.
     * 
     * @param pageProcessorName name of the registered page processor
     * @param batchId batch identifier
     * @param page the page to process
     * @param args optional arguments for the processor
     * @param isRetry whether this is a retry attempt
     * @return result of processing
     */
    @ActivityMethod(name = "ProcessBatchPage")
    BatchOrchestratorWorkflowImpl.PageResult processPage(
            String pageProcessorName,
            String batchId,
            BatchPage page,
            String args,
            boolean isRetry
    );
}
