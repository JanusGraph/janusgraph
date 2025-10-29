# JanusGraph CDC Module

This module provides Change Data Capture (CDC) support for JanusGraph mixed index mutations to ensure eventual consistency between the storage backend (Cassandra) and mixed indexes (ElasticSearch, Solr, etc.).

## Overview

When using JanusGraph with external index backends, there's a risk of inconsistency if the index update fails while the graph data is successfully written to the storage backend. CDC addresses this by:

1. Publishing all mixed index mutations to a Kafka topic
2. Running separate CDC workers that consume these events and apply them to the index
3. Ensuring eventual consistency even in failure scenarios

## Architecture

### Components

1. **CdcProducer**: Publishes index mutation events to Kafka
2. **CdcWorker**: Consumes events from Kafka and applies them to indexes
3. **CdcIndexTransaction**: Wraps IndexTransaction to capture mutations
4. **CdcConfiguration**: Manages CDC settings

### CDC Modes

- **DUAL** (default): Write to index during transaction AND publish to CDC topic for redundancy
- **SKIP**: Skip index writes during transaction, rely entirely on CDC
- **CDC_ONLY**: Same as SKIP (deprecated naming)

## Configuration

Add the following to your JanusGraph configuration:

```properties
# Enable CDC for mixed indexes
index.search.cdc.enabled=true

# CDC mode (dual, skip, or cdc-only)
index.search.cdc.mode=dual

# Kafka bootstrap servers
index.search.cdc.kafka-bootstrap-servers=localhost:9092

# Kafka topic for CDC events
index.search.cdc.kafka-topic=janusgraph-cdc-index-mutations
```

## Usage

### Running CDC Workers

CDC workers should be run as separate processes:

```java
// Create index provider and retriever
IndexProvider indexProvider = ...;
KeyInformation.IndexRetriever indexRetriever = ...;

// Create and start CDC worker
CdcWorker worker = new CdcWorker(
    "localhost:9092",           // Kafka bootstrap servers
    "janusgraph-cdc-index-mutations",  // Topic name
    "janusgraph-cdc-group",     // Consumer group ID
    indexProvider,
    indexRetriever
);

worker.start();

// Keep worker running...

// Shutdown gracefully
worker.stop();
worker.close();
```

### Integration with JanusGraph

CDC can be integrated programmatically using the CdcIndexTransactionFactory:

```java
Configuration config = ...; // Your JanusGraph configuration
CdcIndexTransactionFactory cdcFactory = new CdcIndexTransactionFactory(config);

// Wrap index transactions with CDC support
IndexTransaction indexTx = ...; // Original index transaction
IndexTransaction wrappedTx = cdcFactory.wrapIfEnabled(indexTx);

// Use wrappedTx as normal
// Mutations will be captured and published to Kafka
```

## Benefits

1. **Eventual Consistency**: Guarantees that index and storage backend will eventually be consistent
2. **Failure Recovery**: Automatic recovery from index update failures
3. **Operational Flexibility**: CDC workers can be scaled independently
4. **Minimal Performance Impact**: Asynchronous processing offloads index updates

## Dependencies

- Apache Kafka 3.6.1+
- Debezium Core 2.5.0+
- Jackson for JSON serialization

## Limitations

- Requires Kafka infrastructure
- Eventual consistency means slight delay in index updates
- CDC workers must be managed separately from JanusGraph instances

## Future Enhancements

- Automatic integration with Backend
- Support for other message brokers (RabbitMQ, etc.)
- Built-in CDC worker management
- Metrics and monitoring integration
