# iWF Batch Processing Module

A high-performance, fault-tolerant batch processing framework for iWF built on Temporal.io. Inspired by [batch-orchestra](https://github.com/drewhoskins/batch-orchestra) and optimized for large-scale data operations.

## Overview

The iWF Batch Processing module enables processing large datasets (millions of records) with:

- **Controlled Parallelism**: Process multiple pages concurrently with configurable limits
- **Pipelined Pagination**: No pre-scanning required, pages are processed as they're discovered
- **Fault Tolerance**: Automatic retries with extended retry for stuck pages
- **Progress Tracking**: Real-time progress queries and custom tracking
- **Dynamic Control**: Pause, resume, and adjust parallelism at runtime
- **Infinite Scalability**: Uses Temporal's continue-as-new for unlimited batch sizes

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Batch Orchestrator Workflow                   │
│                                                                 │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐                │
│  │  Page 1  │     │  Page 2  │     │  Page 3  │  ...           │
│  │Processing│ --> │Processing│ --> │Processing│                │
│  └──────────┘     └──────────┘     └──────────┘                │
│        │               │               │                        │
│        v               v               v                        │
│  ┌──────────────────────────────────────────┐                  │
│  │          Page Tracker State              │                  │
│  │  - Pending, Processing, Completed        │                  │
│  │  - Stuck (for extended retries)          │                  │
│  │  - Failed (permanent failures)           │                  │
│  └──────────────────────────────────────────┘                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              v
┌─────────────────────────────────────────────────────────────────┐
│                    Page Processor Activity                       │
│                                                                 │
│  1. Fetch page data using cursor                                │
│  2. Signal next page cursor (BEFORE processing)                 │
│  3. Process items in page                                       │
│  4. Return results                                              │
└─────────────────────────────────────────────────────────────────┘
```

## Key Concept: Pipelined Pagination

Traditional parallel pagination requires pre-scanning data to determine page boundaries. This is expensive and often impractical for large datasets.

**Pipelined pagination** inverts this:

1. Start processing the first page
2. Page processor determines the next cursor during processing
3. Signal the orchestrator with the next cursor **before** processing items
4. Orchestrator starts the next page immediately (up to max parallelism)
5. Repeat until all data is processed

Benefits:
- Single scan through data
- Even page sizes
- Maximum parallelism achieved quickly
- No pre-computation required

## Quick Start

### 1. Create a Page Processor

```java
@BatchProcessor(name = "userMigration")
public class UserMigrationProcessor implements PageProcessor {
    
    @Autowired
    private UserRepository userRepo;
    
    @Autowired
    private NewUserService newService;
    
    @Override
    public void process(BatchProcessorContext context) throws Exception {
        BatchPage page = context.getCurrentPage();
        
        // Parse cursor (could be JSON, offset, key, etc.)
        long lastUserId = page.getCursorStr().isEmpty() 
            ? 0 
            : Long.parseLong(page.getCursorStr());
        
        // Fetch page of users
        List<User> users = userRepo.findUsersAfterId(lastUserId, page.getSize());
        
        // IMPORTANT: Enqueue next page FIRST (enables parallelism)
        if (users.size() == page.getSize()) {
            long nextId = users.get(users.size() - 1).getId();
            context.enqueueNextPage(BatchPage.of(String.valueOf(nextId), page.getSize()));
        } else {
            context.markAsLastPage();
        }
        
        // Process each user
        for (User user : users) {
            context.heartbeat(); // Prevent timeout on long operations
            newService.migrateUser(user);
            context.incrementItemsProcessed();
        }
    }
}
```

### 2. Register the Processor

```java
@Configuration
public class BatchConfig {
    
    @Bean
    public CommandLineRunner registerProcessors(UserMigrationProcessor processor) {
        return args -> {
            BatchPageActivityImpl.registerProcessor("userMigration", processor);
        };
    }
}
```

### 3. Start a Batch Job

```java
@Autowired
BatchClient batchClient;

public void startMigration() {
    BatchConfig config = BatchConfig.builder()
        .batchId("user-migration-2024")
        .pageProcessorName("userMigration")
        .maxParallelism(10)       // 10 pages at once
        .pageSize(100)            // 100 users per page
        .pageTimeout(Duration.ofMinutes(5))
        .build();
    
    String workflowId = batchClient.startBatch(config);
    System.out.println("Started batch: " + workflowId);
}
```

### 4. Monitor Progress

```java
// Query progress anytime
BatchProgress progress = batchClient.getProgress(workflowId);
System.out.println("Completed: " + progress.getCompletedPages());
System.out.println("Processing: " + progress.getProcessingPages());
System.out.println("Throughput: " + progress.getThroughput() + " items/sec");

// Pause if system is under load
batchClient.pause(workflowId);

// Resume when ready
batchClient.resume(workflowId);

// Wait for completion
BatchProgress result = batchClient.waitForCompletion(workflowId);
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `batchId` | required | Unique identifier for the batch |
| `pageProcessorName` | required | Name of the registered processor |
| `maxParallelism` | 10 | Max concurrent pages |
| `pageSize` | 100 | Items per page |
| `pageTimeout` | 5 min | Timeout per page |
| `maxInitialRetries` | 10 | Retries before extended retry |
| `extendedRetryInterval` | 5 min | Time between extended retries |
| `useExtendedRetries` | true | Enable extended retry phase |
| `pagesPerRun` | 500 | Pages before continue-as-new |
| `enableDynamicParallelism` | false | Auto-adjust parallelism |
| `targetThroughput` | 100 | Target items/sec for dynamic mode |

## Retry Behavior

### Initial Retry Phase
- Uses exponential backoff (default: 10 attempts)
- Quick retries for transient failures
- Pages that still fail move to stuck state

### Extended Retry Phase
- Runs after all pages complete initial phase
- Stuck pages retry at fixed intervals (default: 5 min)
- Continues until success or workflow timeout
- Allows code fixes to resolve stuck pages

### Non-Retryable Errors
- Configure via `getNonRetryableErrors()` in processor
- Immediately fail without retry
- Useful for validation errors, bad data, etc.

## Progress Tracking

### Built-in Progress Query

```java
BatchProgress progress = batchClient.getProgress(workflowId);

// Basic stats
int completed = progress.getCompletedPages();
int processing = progress.getProcessingPages();
int pending = progress.getPendingPages();
int stuck = progress.getStuckPages();
int failed = progress.getFailedPages();

// Performance
double throughput = progress.getThroughput();
long itemsProcessed = progress.getItemsProcessed();

// Status
boolean hasIssues = progress.hasIssues();
double completionPct = progress.getCompletionPercentage();
```

### Custom Progress Tracker

```java
@BatchTracker(name = "slackTracker")
public class SlackProgressTracker implements ProgressTracker {
    
    @Override
    public void onProgress(BatchProgress progress, String args) {
        if (progress.isFinished()) {
            sendSlackMessage("#batch-jobs", 
                "Batch " + progress.getBatchId() + " completed!");
        }
    }
}

// Use in config
BatchConfig config = BatchConfig.builder()
    .trackerName("slackTracker")
    .trackerPollingInterval(Duration.ofMinutes(1))
    .build();
```

## Best Practices

### 1. Make Operations Idempotent

Since pages may be retried, ensure your operations are idempotent:

```java
// BAD - creates duplicate records on retry
userRepo.insert(newUser);

// GOOD - uses unique key to prevent duplicates
userRepo.upsert(newUser, "email");
```

### 2. Signal Next Page Early

Always determine and signal the next page cursor **before** processing items:

```java
@Override
public void process(BatchProcessorContext context) {
    List<Item> items = fetchPage(context.getCurrentPage());
    
    // ✅ Signal first - enables parallelism
    if (!items.isEmpty()) {
        context.enqueueNextPage(BatchPage.of(items.getLast().getId(), pageSize));
    }
    
    // ✅ Then process
    for (Item item : items) {
        process(item);
    }
}
```

### 3. Use Heartbeats for Long Operations

Prevent activity timeouts on long-running page processing:

```java
for (Item item : items) {
    context.heartbeat(); // Reset timeout
    slowOperation(item);
}
```

### 4. Handle Resume State

Use heartbeat details to resume mid-page after failures:

```java
@Override
public void process(BatchProcessorContext context) {
    // Check for resume state
    ResumeState state = context.getHeartbeatDetails(ResumeState.class);
    int startIndex = state != null ? state.getLastProcessedIndex() : 0;
    
    List<Item> items = fetchPage(context.getCurrentPage());
    
    for (int i = startIndex; i < items.size(); i++) {
        process(items.get(i));
        context.heartbeat(new ResumeState(i)); // Save progress
    }
}
```

### 5. Choose Appropriate Page Size

- **Too small**: Overhead from pagination and signaling
- **Too large**: Long processing time, harder to parallelize
- **Sweet spot**: 100-1000 items per page, <5 minutes processing

## Advanced Features

### Dynamic Parallelism

Automatically adjust parallelism based on throughput:

```java
BatchConfig config = BatchConfig.builder()
    .enableDynamicParallelism(true)
    .targetThroughput(1000.0) // items/sec
    .maxParallelism(50)       // upper limit
    .build();
```

### Continue-As-New

For very large batches, the workflow automatically uses Temporal's continue-as-new to manage history size:

```java
BatchConfig config = BatchConfig.builder()
    .pagesPerRun(500) // trigger continue-as-new every 500 pages
    .build();
```

### Gradual Ramp-Up

Start slow and increase parallelism:

```java
// Start with low parallelism
String workflowId = batchClient.startBatch(config);

// Gradually increase
batchClient.setParallelism(workflowId, 5);
Thread.sleep(60000);
batchClient.setParallelism(workflowId, 10);
Thread.sleep(60000);
batchClient.setParallelism(workflowId, 20);
```

## Error Handling

### Permanent Failures

Mark pages as permanently failed (no more retries):

```java
@Override
public void process(BatchProcessorContext context) {
    try {
        // processing
    } catch (ValidationException e) {
        // This will fail permanently
        throw ApplicationFailure.newNonRetryableFailure(
            e.getMessage(), 
            "VALIDATION_ERROR"
        );
    }
}
```

### Stuck Page Handling

Query and handle stuck pages:

```java
BatchProgress progress = batchClient.getProgress(workflowId);

if (progress.getStuckPages() > 0) {
    System.out.println("Stuck pages: " + progress.getStuckPageNumbers());
    
    // Option 1: Wait for extended retries
    // Option 2: Fix code and deploy (extended retries will pick up fix)
    // Option 3: Cancel and restart with different config
}
```

## Integration with iWF

The batch module can trigger iWF workflows for each item:

```java
@BatchProcessor(name = "processOrders")
public class OrderBatchProcessor implements PageProcessor {
    
    @Autowired
    private IwfClient iwfClient;
    
    @Override
    public void process(BatchProcessorContext context) {
        List<Order> orders = fetchOrders(context.getCurrentPage());
        
        // Signal next page
        if (!orders.isEmpty()) {
            context.enqueueNextPage(nextPage);
        }
        
        // Start iWF workflow for each order
        for (Order order : orders) {
            iwfClient.startWorkflow(
                OrderProcessingWorkflow.class,
                "order-" + order.getId(),
                order
            );
            context.incrementItemsProcessed();
        }
    }
}
```

## Comparison with Alternatives

| Feature | iWF Batch | Temporal Direct | Spring Batch |
|---------|-----------|-----------------|--------------|
| Pipelined Pagination | ✅ | ❌ | ❌ |
| Controlled Parallelism | ✅ | Manual | ✅ |
| Dynamic Pause/Resume | ✅ | Manual | Limited |
| Extended Retries | ✅ | Manual | ❌ |
| Progress Tracking | ✅ | Manual | ✅ |
| Continue-as-New | ✅ Auto | Manual | N/A |
| Infinite Scale | ✅ | ✅ | Limited |

## License

Apache 2.0 - See LICENSE file for details.
