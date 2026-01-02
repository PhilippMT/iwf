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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Represents a page of work in a batch processing operation.
 * 
 * <p>Each page contains a cursor for pagination and a size indicating
 * how many items to process in this page. Pages are processed in parallel
 * using pipelined pagination, where the cursor for the next page is
 * determined before processing the current page.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BatchPage implements Serializable {
    
    /**
     * Cursor string for this page. Can be:
     * - JSON serialized object
     * - Database offset/key
     * - Timestamp
     * - Any serializable pagination token
     */
    private String cursorStr;
    
    /**
     * Number of items to process in this page.
     */
    private int size;
    
    /**
     * Page number (0-indexed).
     */
    private int pageNum;
    
    /**
     * Whether this is the last page.
     */
    private boolean isLastPage;
    
    /**
     * Create a page with cursor and size.
     */
    public static BatchPage of(String cursor, int size) {
        return BatchPage.builder()
                .cursorStr(cursor)
                .size(size)
                .build();
    }
    
    /**
     * Create the first page of a batch.
     */
    public static BatchPage first(int size) {
        return BatchPage.builder()
                .cursorStr("")
                .size(size)
                .pageNum(0)
                .build();
    }
}
