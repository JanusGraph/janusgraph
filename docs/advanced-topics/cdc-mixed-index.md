# CDC-based Mixed Index Synchronization

Mixed indexes (ElasticSearch, Solr, Lucene) live in an external system. During normal operation JanusGraph performs
two independent writes when a transaction commits: the graph data is written to the primary storage backend (e.g.
Cassandra) and the mixed-index documents are written to the index backend. If the first write succeeds but the second
fails (index backend unavailable, network partition, JVM crash in between), the index becomes
[stale](./stale-index.md) and — unless a [transaction recovery](../operations/recovery.md) process happens to be
running — permanently inconsistent with the graph.

Change-Data-Capture (CDC) based synchronization removes this dual-write hazard. Instead of (or in addition to) writing
the mixed index during the transaction, the index is updated **asynchronously** from a change stream that is derived
from the *same* committed storage write. Because the change stream is a faithful downstream of what was durably
committed to Cassandra, there is no second write that can diverge: every committed graph change is eventually
reflected in the mixed index.

This feature targets **Apache Cassandra** as the storage backend and uses Cassandra's native CDC together with the
[Debezium Cassandra connector](https://debezium.io/documentation/reference/stable/connectors/cassandra.html) and
Apache Kafka.

## Architecture

```
   commit (graph data only, mixed-index write optionally skipped)
        │
        ▼
   Cassandra  edgestore (cdc = true)
        │   commit-log CDC
        ▼
   Debezium Cassandra connector  ──►  Kafka topic  (cassandra.<keyspace>.edgestore)
                                          │
              CdcIndexUpdateWorker consumer group (scales horizontally)
                                          │
                                  reindex affected elements from current graph state
                                          │
                                          ▼
                                   ElasticSearch  ( _bulk )
```

- **Capture** — Cassandra writes commit-log CDC segments for the `edgestore` table (which holds all vertex
  properties and edges). The Debezium Cassandra connector reads those segments and publishes one change event per
  mutated `edgestore` row to Kafka, keyed by the Cassandra partition key (the JanusGraph vertex id).
- **Apply** — the `CdcIndexUpdateWorker` (shipped in `janusgraph-cdc`) consumes the change events, determines which
  graph elements changed, and **reindexes each element from its current graph state** into the mixed index, issuing
  one ElasticSearch `_bulk` request per batch.

### Consistency model

The worker never applies field-level deltas from an event. For each changed element it reads the element's *current*
state from the graph and fully replaces (or removes) its index document — the same technique JanusGraph's transaction
recovery uses. This makes index updates:

- **idempotent** — replaying an event produces the same document;
- **order-independent** — events processed out of order, or more than once, all converge to the current state, because
  every application reads the live graph. Applying a "stale" event after a newer change still reads the newer state, so
  a stale value can never overwrite a fresh one.

The only requirement for convergence is that every committed change is eventually processed at least once, which the
CDC pipeline (commit-log → Debezium → Kafka with offset-after-success) guarantees. The index therefore lags the graph
by the CDC propagation latency and is **eventually consistent** with it.

### Scaling and ordering

The Debezium connector keys Kafka records by the Cassandra partition key (the vertex id), so all events for a given
vertex land in the same partition and are consumed in order by a single worker in the consumer group. Add partitions
and workers to scale out; work is distributed by vertex-id hash. (Correctness does not depend on ordering — see above —
but per-element partitioning avoids two workers redundantly reindexing the same element.)

## Enabling CDC

### 1. Cassandra: enable CDC on the JanusGraph tables

Set the storage option so JanusGraph creates its `edgestore` table with the Cassandra `cdc=true` table option:

```properties
storage.backend=cql
storage.cql.cdc=true
```

!!! warning "Existing deployments: alter the table manually"
    `storage.cql.cdc` only takes effect when JanusGraph **creates** the `edgestore` table. On an existing graph the
    table already exists, so additionally run the following once against the cluster:

    ```sql
    ALTER TABLE <keyspace>.edgestore WITH cdc = true;
    ```

    Note that the option is `GLOBAL_OFFLINE`: on an existing graph it must be changed via the management API
    (`mgmt.set("storage.cql.cdc", true)`) followed by a restart — a value that only appears in the local properties
    file is overridden by the stored global setting.

The Cassandra cluster itself must be started with CDC enabled (`cassandra.yaml`):

```yaml
cdc_enabled: true
cdc_raw_directory: /var/lib/cassandra/cdc_raw
commitlog_sync: periodic
commitlog_sync_period_in_ms: 1000
```

Smaller commit-log segments (`commitlog_segment_size_in_mb`) reduce the latency before a change surfaces into
`cdc_raw`, **but Cassandra caps the maximum mutation size at half the segment size** — e.g. 1&nbsp;MB segments reject
any mutation over 512&nbsp;KB cluster-wide (large JanusGraph batches, supernode partitions, bulk loading). Keep the
default segment size unless you have measured your largest mutations and accept the cap.

Only `edgestore` is marked for CDC — composite-index data (`graphindex`) is internal to storage and already consistent,
and the other system tables are not relevant to mixed-index synchronization.

### 2. Per-index mode

CDC management is configured per mixed-index backend under the `index.[X].cdc` namespace:

| Option | Default | Meaning |
|---|---|---|
| `index.[X].cdc.enabled` | `false` | This backing index is maintained via the CDC pipeline. |
| `index.[X].cdc.synchronous` | `true` | When CDC is enabled, also write the index synchronously during the transaction. |

- `cdc.enabled=true`, `cdc.synchronous=true` — **dual mode**. The index is written both synchronously and via CDC. Safe
  and redundant: CDC repairs any synchronous failure. Recommended when first adopting CDC, and as a migration step.
- `cdc.enabled=true`, `cdc.synchronous=false` — **cdc-only mode**. The synchronous mixed-index write is skipped and the
  index is updated exclusively by the CDC pipeline. Maximum efficiency and the purest eventual-consistency model; the
  index lags the graph by the CDC latency.
- `cdc.enabled=false` (default) — today's behavior, synchronous index writes only.

!!! warning "cdc-only mode is not validated"
    JanusGraph cannot verify that a capture pipeline is actually running. With `cdc.synchronous=false` configured and
    no working pipeline, the affected mixed indexes silently stop being maintained (the graph logs a warning at
    startup). Adopt CDC in dual mode first and switch to cdc-only after verifying end-to-end delivery.

```properties
index.search.backend=elasticsearch
index.search.hostname=127.0.0.1
index.search.cdc.enabled=true
index.search.cdc.synchronous=false
```

### 3. Debezium Cassandra connector

The Debezium Cassandra connector runs **co-located with each Cassandra node** (it reads the node's `cdc_raw` commit-log
directory) and publishes to Kafka. It is *not* a Kafka Connect plugin and is *not* embedded in JanusGraph — it is a
separate process. See the
[Debezium Cassandra documentation](https://debezium.io/documentation/reference/stable/connectors/cassandra.html) for
installation. Key settings:

```properties
connector.name=janusgraph-cdc
topic.prefix=cassandra
cassandra.config=/etc/cassandra/cassandra.yaml
commit.log.real.time.processing.enabled=true   # surface changes promptly (Cassandra 4)
snapshot.mode=NEVER                              # capture ongoing changes (use INITIAL to also backfill)
# Kafka + converters
kafka.producer.bootstrap.servers=kafka:9092
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
```

The connector publishes to the topic `<topic.prefix>.<keyspace>.edgestore`. Blob columns (JanusGraph's keys/values)
are Base64-encoded in the JSON events; the worker's decoder decodes them with JanusGraph's own serialization.

### 4. Run the CDC index-update worker

The worker is provided by the `janusgraph-cdc` module, which is **a separate Maven artifact and not part of the
JanusGraph distribution** (so deployments that don't use CDC don't pull in Kafka):

```xml
<dependency>
    <groupId>org.janusgraph</groupId>
    <artifactId>janusgraph-cdc</artifactId>
    <version>{{ latest_version }}</version>
</dependency>
```

Assemble it (with its dependencies and your storage/index backend modules) onto a classpath and run one or more
instances (in the same Kafka consumer group, in separate processes, for horizontal scale):

```bash
java org.janusgraph.cdc.CdcIndexUpdateWorkerMain cdc.properties
```

`cdc.properties`:

```properties
cdc.graph-config=/etc/janusgraph/janusgraph.properties   # the JanusGraph config (CQL + ElasticSearch)
cdc.bootstrap-servers=kafka:9092
cdc.topics=cassandra.janusgraph.edgestore
cdc.group-id=janusgraph-cdc
cdc.worker-threads=1
cdc.max-poll-records=500
# Retry/backoff used when applying index updates fails transiently:
cdc.retry.limit=5
cdc.retry.initial-wait-ms=100
cdc.retry.max-wait-ms=30000
# Any cdc.consumer.* key is passed to the KafkaConsumer with the prefix stripped, e.g. for a secured cluster:
#cdc.consumer.security.protocol=SASL_SSL
#cdc.consumer.sasl.mechanism=SCRAM-SHA-512
```

Unrecognized `cdc.*` keys are logged and ignored (so a typo does not silently run with defaults). The worker's
offset-management settings (`enable.auto.commit=false` and the byte-array deserializers) cannot be overridden via
`cdc.consumer.*` — the at-least-once guarantee depends on them.

The worker opens a read connection to the graph (to read current element state and to obtain the ElasticSearch index
transaction), so it must be able to reach both Cassandra and ElasticSearch. Offsets are committed only after a batch is
durably applied (at-least-once); if a batch cannot be applied after the retry budget it is reprocessed rather than
skipped, so the index eventually catches up instead of silently going stale.

## Operational notes

- **Latency** — the index trails the graph by roughly the commit-log flush + Debezium read + Kafka + worker + ES refresh
  time. Tune the Cassandra commit-log settings above and the worker poll/batch settings for your throughput.
- **Supported elements** — vertices, vertex properties, and edges (including edge properties) are all reindexed.
- **Delivery** — at-least-once; the reindex-from-current-state model makes duplicates and out-of-order delivery safe.
- **Adding a mixed index later** — the worker discovers CDC-managed indexes at startup. After creating a new mixed
  index on a CDC backing, restart the workers and run a `REINDEX` (required for pre-existing data anyway); the reindex
  also covers any changes consumed between index creation and the restart.

### cdc-only deletion limits for relation-element indexes

A Cassandra delete is a tombstone that carries **no value bytes**, and JanusGraph stores some relation identities in
the value region. Consequently, in **cdc-only** mode the CDC events alone cannot identify the deleted element's index
document for:

- **edges of constrained-multiplicity labels** (`SIMPLE`, `MANY2ONE`, `ONE2MANY`, `ONE2ONE`) in **edge** mixed indexes
  (multi-cardinality edge deletions are fully supported — their identity lives in the column);
- **removed vertex properties** in **property-element (meta-property)** mixed indexes (the owning vertex's own
  documents are always maintained correctly).

For schemas using those combinations, run the affected indexes in **dual mode** (`cdc.synchronous=true`, where the
synchronous write handles the removal and CDC repairs failures) or schedule periodic reindexes. Vertex mixed indexes —
the common case — have no such limitation.

### cdc-only and document TTL (Solr)

Document TTL — Solr is the only in-tree index backend that supports it — is attached to mixed-index documents only by
the **synchronous** write path. The CDC worker's reindex-from-current-state writes documents without a TTL, and a
storage-level TTL expiry produces **no CDC event** (Cassandra expires the cells silently), so in cdc-only mode the
index documents of TTL'd elements are written without an expiry and are never removed when the graph data expires.
Keep Solr mixed indexes over element types with a TTL (`mgmt.setTTL(...)`) in **dual mode**, or schedule periodic
reindexes. (The same gap exists in JanusGraph's transaction-recovery restore path; cdc-only merely makes the restore
path the *only* writer.)

## Testing

The JanusGraph-owned half of the pipeline (decode → worker → reindex → ElasticSearch `_bulk`) is verified end-to-end
against a **real Kafka and real ElasticSearch** (Testcontainers) in `CdcKafkaElasticsearchTest`, covering vertex and
edge add/update/remove and out-of-order delivery. The unit/component suite additionally verifies the decoder against
real JanusGraph-serialized bytes, the reindex engine over a real graph + Lucene, the consumer loop (dedup, retry,
offset-after-success), and the commit-side skip behavior.

The complete pipeline — including the Cassandra → Debezium capture hop — is exercised by
`CdcCassandraDebeziumElasticsearchTest`: it starts a real Cassandra (CDC enabled), Kafka, and ElasticSearch via
Testcontainers, runs the real Debezium Cassandra connector, and asserts ElasticSearch converges to the graph for
vertex add/update/remove. It is gated behind the `cassandra-cdc-e2e` Maven profile, which **auto-activates on Java
17+** (required by Debezium 3.x and Testcontainers 2.x; `cassandra-all` 4.1.7 is not Java-24-compatible, so use Java
17). The profile supplies the Debezium dependency, pins SnakeYAML to 1.x for `cassandra-all`, and adds the JVM
`--add-opens`/`--add-exports` flags `cassandra-all` needs. Run it on a JDK 17 with Docker available:

```bash
mvn -pl janusgraph-cdc -am test -Dtest=CdcCassandraDebeziumElasticsearchTest
```
