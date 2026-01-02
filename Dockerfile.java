# iWF Java Server Dockerfile
# Multi-stage build for optimized container size

# Stage 1: Build
FROM eclipse-temurin:21-jdk as builder

WORKDIR /build

# Copy Maven wrapper and pom files first for better caching
COPY iwf-java-server/pom.xml ./
COPY iwf-java-server/iwf-api/pom.xml ./iwf-api/
COPY iwf-java-server/iwf-core/pom.xml ./iwf-core/
COPY iwf-java-server/iwf-server/pom.xml ./iwf-server/
COPY iwf-java-server/iwf-idl/ ./iwf-idl/

# Install Maven
RUN apt-get update && apt-get install -y maven && rm -rf /var/lib/apt/lists/*

# Download dependencies
RUN mvn dependency:go-offline -B || true

# Copy source code
COPY iwf-java-server/iwf-api/src/ ./iwf-api/src/
COPY iwf-java-server/iwf-core/src/ ./iwf-core/src/
COPY iwf-java-server/iwf-server/src/ ./iwf-server/src/

# Build the application
RUN mvn clean package -DskipTests -B

# Stage 2: Runtime
FROM eclipse-temurin:21-jre

LABEL maintainer="iWF Authors"
LABEL description="iWF Java Server - Indeed Workflow Framework"

# Create non-root user for security
RUN groupadd -r iwf && useradd -r -g iwf iwf

WORKDIR /app

# Copy the built JAR
COPY --from=builder /build/iwf-server/target/iwf-server-*.jar ./iwf-server.jar

# Copy default configuration
COPY iwf-java-server/iwf-server/src/main/resources/application.yml ./config/

# Set ownership
RUN chown -R iwf:iwf /app

USER iwf

# Expose ports
# 8801: Main API server
# 9090: Prometheus metrics (via actuator)
EXPOSE 8801 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8801/info/healthcheck || exit 1

# JVM options for container environment
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError"

# Run the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar iwf-server.jar --spring.config.location=file:./config/"]
