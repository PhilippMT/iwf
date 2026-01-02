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

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * BlobStore implementation using AWS S3 for external storage.
 */
@Slf4j
@Component
public class S3BlobStore implements BlobStore {

    private S3Client s3Client;
    private String bucketName;
    private String storageId;
    private boolean enabled;

    public S3BlobStore() {
        // Default constructor - configuration will be set via configure()
        this.enabled = false;
    }

    /**
     * Configure the S3 blob store with the given settings.
     */
    public void configure(BlobStoreConfig config) {
        this.enabled = config.isEnabled();
        
        if (!enabled) {
            log.info("S3 BlobStore is disabled");
            return;
        }
        
        this.storageId = config.getStorageId();
        this.bucketName = config.getBucket();
        
        AwsBasicCredentials credentials = AwsBasicCredentials.create(
                config.getAccessKey(),
                config.getSecretKey()
        );
        
        S3Client.Builder builder = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .region(Region.of(config.getRegion()));
        
        if (config.getEndpoint() != null && !config.getEndpoint().isEmpty()) {
            builder.endpointOverride(URI.create(config.getEndpoint()))
                   .forcePathStyle(true);
        }
        
        this.s3Client = builder.build();
        
        // Create bucket if it doesn't exist
        ensureBucketExists();
        
        log.info("S3 BlobStore configured with bucket: {}", bucketName);
    }

    private void ensureBucketExists() {
        try {
            s3Client.headBucket(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            log.debug("Bucket {} already exists", bucketName);
        } catch (NoSuchBucketException e) {
            log.info("Creating bucket: {}", bucketName);
            try {
                s3Client.createBucket(CreateBucketRequest.builder()
                        .bucket(bucketName)
                        .build());
            } catch (Exception createException) {
                log.error("Failed to create bucket: {}", bucketName, createException);
                throw createException;
            }
        }
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String getStorageId() {
        return storageId;
    }

    @Override
    public WriteResult write(String workflowId, String data) {
        if (!enabled) {
            throw new IllegalStateException("BlobStore is not enabled");
        }
        
        String objectKey = generateObjectKey(workflowId);
        
        try {
            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(objectKey)
                            .contentType("application/octet-stream")
                            .build(),
                    RequestBody.fromBytes(data.getBytes(StandardCharsets.UTF_8))
            );
            
            log.debug("Stored object {} in bucket {}", objectKey, bucketName);
            return new WriteResult(storageId, objectKey);
        } catch (Exception e) {
            log.error("Failed to write to S3: {}", e.getMessage(), e);
            throw new BlobStoreException("Failed to write to S3", e);
        }
    }

    @Override
    public String read(String storeId, String path) {
        if (!enabled) {
            throw new IllegalStateException("BlobStore is not enabled");
        }
        
        // Verify store ID matches
        if (!storageId.equals(storeId)) {
            throw new BlobStoreException("Storage ID mismatch: expected " + storageId + ", got " + storeId);
        }
        
        try {
            var response = s3Client.getObject(GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(path)
                    .build());
            
            return new String(response.readAllBytes(), StandardCharsets.UTF_8);
        } catch (NoSuchKeyException e) {
            throw new BlobStoreException("Object not found: " + path, e);
        } catch (Exception e) {
            log.error("Failed to read from S3: {}", e.getMessage(), e);
            throw new BlobStoreException("Failed to read from S3", e);
        }
    }

    @Override
    public void delete(String storeId, String path) {
        if (!enabled) {
            throw new IllegalStateException("BlobStore is not enabled");
        }
        
        if (!storageId.equals(storeId)) {
            throw new BlobStoreException("Storage ID mismatch: expected " + storageId + ", got " + storeId);
        }
        
        try {
            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(path)
                    .build());
            log.debug("Deleted object {} from bucket {}", path, bucketName);
        } catch (Exception e) {
            log.error("Failed to delete from S3: {}", e.getMessage(), e);
            throw new BlobStoreException("Failed to delete from S3", e);
        }
    }

    private String generateObjectKey(String workflowId) {
        // Validate and sanitize workflow ID first
        validateWorkflowId(workflowId);
        // Format: workflowId/timestamp-uuid
        return sanitizeForS3Key(workflowId) + "/" + System.currentTimeMillis() + "-" + UUID.randomUUID();
    }

    /**
     * Validate that a workflow ID is compatible with external storage.
     * Checks for path traversal attacks and S3 key requirements.
     */
    public static void validateWorkflowId(String workflowId) {
        if (workflowId == null || workflowId.isEmpty()) {
            throw new IllegalArgumentException("Workflow ID cannot be null or empty");
        }
        
        // Check for path traversal attacks
        if (workflowId.contains("..") || 
            workflowId.contains("./") || 
            workflowId.contains("/.") ||
            workflowId.startsWith("/") ||
            workflowId.startsWith("\\") ||
            workflowId.contains("\\")) {
            throw new IllegalArgumentException("Invalid workflow ID - potential path traversal: " + workflowId);
        }
        
        // Check S3 key length limit (1024 bytes)
        if (workflowId.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 900) {
            throw new IllegalArgumentException("Workflow ID too long for S3 storage: " + workflowId.length());
        }
        
        // Check for null bytes which can cause issues
        if (workflowId.contains("\0")) {
            throw new IllegalArgumentException("Workflow ID contains invalid null byte");
        }
    }

    /**
     * Sanitize workflow ID for safe use as S3 key component.
     */
    private static String sanitizeForS3Key(String workflowId) {
        // Replace any remaining problematic characters
        return workflowId
                .replace(' ', '_')
                .replace('\t', '_')
                .replace('\n', '_')
                .replace('\r', '_');
    }

    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class BlobStoreConfig {
        private boolean enabled;
        private String storageId;
        private String storageType;
        private String endpoint;
        private String bucket;
        private String region;
        private String accessKey;
        private String secretKey;
        private int thresholdInBytes;
    }
}
