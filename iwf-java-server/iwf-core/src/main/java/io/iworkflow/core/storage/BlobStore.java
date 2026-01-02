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
package io.iworkflow.core.storage;

/**
 * Interface for external blob storage.
 */
public interface BlobStore {

    /**
     * Check if external storage is enabled.
     */
    boolean isEnabled();

    /**
     * Get the storage identifier.
     */
    String getStorageId();

    /**
     * Write data to external storage.
     * 
     * @param workflowId the workflow ID (used for organization)
     * @param data the data to store
     * @return result containing storage ID and path
     */
    WriteResult write(String workflowId, String data);

    /**
     * Read data from external storage.
     * 
     * @param storeId the storage ID
     * @param path the path/key of the data
     * @return the stored data
     */
    String read(String storeId, String path);

    /**
     * Delete data from external storage.
     * 
     * @param storeId the storage ID
     * @param path the path/key of the data
     */
    void delete(String storeId, String path);

    /**
     * Result of a write operation.
     */
    record WriteResult(String storeId, String path) {}

    /**
     * Exception for blob store operations.
     */
    class BlobStoreException extends RuntimeException {
        public BlobStoreException(String message) {
            super(message);
        }
        
        public BlobStoreException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
