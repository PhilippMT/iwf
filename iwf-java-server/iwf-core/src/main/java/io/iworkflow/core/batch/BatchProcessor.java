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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to register a class as a batch page processor.
 * 
 * <p>Classes annotated with @BatchProcessor must implement {@link PageProcessor}
 * and will be automatically registered with the batch orchestrator.</p>
 * 
 * <h3>Example:</h3>
 * <pre>{@code
 * @BatchProcessor(
 *     name = "processOrders",
 *     retryMode = PageProcessor.RetryMode.AT_LEAST_ONCE,
 *     maxRetries = 5
 * )
 * public class OrderProcessor implements PageProcessor {
 *     @Override
 *     public void process(BatchProcessorContext context) {
 *         // Processing logic
 *     }
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface BatchProcessor {
    
    /**
     * Unique name for this processor.
     * Used to reference this processor in batch configurations.
     */
    String name();
    
    /**
     * Description of what this processor does.
     */
    String description() default "";
    
    /**
     * Retry mode for this processor.
     */
    PageProcessor.RetryMode retryMode() default PageProcessor.RetryMode.AT_LEAST_ONCE;
    
    /**
     * Maximum initial retry attempts.
     */
    int maxRetries() default 10;
    
    /**
     * Whether to use extended retries.
     */
    boolean useExtendedRetries() default true;
    
    /**
     * Extended retry interval in seconds.
     */
    int extendedRetryIntervalSeconds() default 300;
}
