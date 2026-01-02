# iWF Java Server

This is the Java implementation of the iWF (Indeed Workflow Framework) server, providing 100% API compatibility with the original Go implementation.

## Overview

iWF Java Server is built with:
- **Java 21** (LTS) with preview features enabled
- **Spring Boot 3.4** for the application framework
- **Temporal Java SDK 1.32.x** for workflow orchestration
- **OpenAPI Generator** for API model generation from iwf-idl

## Project Structure

```
iwf-java-server/
├── iwf-api/          # Generated API models from OpenAPI spec
├── iwf-core/         # Core workflow interpreter and client
├── iwf-server/       # Spring Boot application
└── iwf-idl/          # OpenAPI specification (copied from submodule)
```

## Building

### Prerequisites
- JDK 21 or later
- Maven 3.9+
- Docker (optional, for containerized builds)

### Build Commands

```bash
# Build all modules
cd iwf-java-server
mvn clean package

# Build with tests
mvn clean verify

# Build Docker image
docker build -f ../Dockerfile.java -t iwf-java-server:latest ..
```

## Running

### Local Development

```bash
# Start with default configuration
java -jar iwf-server/target/iwf-server-1.0.0-SNAPSHOT.jar

# With custom configuration
java -jar iwf-server/target/iwf-server-1.0.0-SNAPSHOT.jar \
    --iwf.interpreter.temporal.host-port=localhost:7233 \
    --iwf.interpreter.temporal.namespace=default
```

### Docker

```bash
docker run -p 8801:8801 \
    -e IWF_INTERPRETER_TEMPORAL_HOSTPORT=temporal:7233 \
    iwf-java-server:latest
```

### Docker Compose

Use with the existing docker-compose.yaml by specifying the Java image:

```yaml
services:
  iwf-server:
    image: iwf-java-server:latest
    # ... rest of configuration
```

## Configuration

Configuration can be provided via:
1. `application.yml` file
2. Environment variables
3. Command-line arguments

### Key Configuration Properties

| Property | Default | Description |
|----------|---------|-------------|
| `iwf.api.port` | 8801 | REST API port |
| `iwf.interpreter.temporal.host-port` | localhost:7233 | Temporal server address |
| `iwf.interpreter.temporal.namespace` | default | Temporal namespace |
| `iwf.interpreter.temporal.task-queue` | iwf-task-queue | Worker task queue |
| `iwf.interpreter.temporal.cloud-api-key` | - | Temporal Cloud API key |

## API Endpoints

All endpoints are 100% compatible with the Go implementation:

### Workflow Lifecycle
- `POST /api/v1/workflow/start` - Start a workflow
- `POST /api/v1/workflow/signal` - Signal a workflow
- `POST /api/v1/workflow/stop` - Stop a workflow
- `POST /api/v1/workflow/get` - Get workflow status
- `POST /api/v1/workflow/getWithWait` - Get workflow with wait

### Data Management
- `POST /api/v1/workflow/dataobjects/get` - Get data objects
- `POST /api/v1/workflow/dataobjects/set` - Set data objects
- `POST /api/v1/workflow/searchattributes/get` - Get search attributes
- `POST /api/v1/workflow/searchattributes/set` - Set search attributes

### RPC & Control
- `POST /api/v1/workflow/rpc` - Execute RPC
- `POST /api/v1/workflow/timer/skip` - Skip timer
- `POST /api/v1/workflow/config/update` - Update config

### Health
- `GET /info/healthcheck` - Health check

## Temporal Features

This implementation leverages modern Temporal features:

### Worker Versioning
Supports safe deployments with Worker Versioning for gradual rollouts.

### Workflow Update
Uses Temporal Updates for RPC operations requiring optimistic locking.

### Nexus Integration (Planned)
Future support for Nexus to enable cross-namespace operations.

## Metrics

Prometheus metrics are exposed at `/actuator/prometheus` including:
- Workflow execution counts and latencies
- Activity execution metrics
- Worker health metrics

## Contributing

See the main repository's CONTRIBUTING.md for guidelines.

## License

Apache 2.0 - See LICENSE file for details.
