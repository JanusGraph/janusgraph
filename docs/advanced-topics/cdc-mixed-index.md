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

Set the storage option so JanusGraph creates its `edgestore` table with the Cassandra `cdc=true` table option
(**Apache Cassandra only** — ScyllaDB's CDC is a different mechanism, incompatible with both this table option and
the Debezium Cassandra connector):

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

    JanusGraph logs a startup warning when it detects this divergence (the option enabled but the live table
    still without `cdc=true`), since the misconfiguration is otherwise silent — commits succeed and no events
    are captured.

    Note that the option is `GLOBAL_OFFLINE`: on an existing graph it must be changed via the management API
    (`mgmt.set("storage.cql.cdc", true)`) followed by a restart — a value that only appears in the local properties
    file is overridden by the stored global setting.

!!! warning "CDC couples write availability to the capture pipeline"
    Cassandra retains CDC commit-log segments in `cdc_raw_directory` until a consumer (the Debezium connector)
    processes and deletes them, capped by `cdc_total_space` (named `cdc_total_space_in_mb` before Cassandra 4.1;
    default 4 GiB or 1/8 of the disk). If the connector stalls long enough to fill that cap, Cassandra **rejects
    further writes to every CDC-enabled table** — i.e. the whole graph stops accepting mutations — until space is
    freed (`cdc_block_writes: true`, the default; the option exists from Cassandra 4.1 — on earlier versions writes
    always block on overflow). Setting `cdc_block_writes: false` keeps writes flowing but makes Cassandra
    **silently drop CDC data** instead, which permanently breaks the pipeline's every-change-is-delivered guarantee
    (a `REINDEX` restores missed additions, but it scans the *graph* — it never visits, and therefore never removes,
    documents of already-deleted elements; those stay orphaned). Size the total-space cap for the longest connector
    outage you need to tolerate, and monitor the `cdc_raw` directory's disk usage.

!!! warning "Disabling CDC: alter the table back first"
    The same one-way behavior applies when **decommissioning** the pipeline: setting `storage.cql.cdc=false` (or
    disabling `index.[X].cdc.enabled`) never alters the existing table, so the edgestore keeps `cdc=true` and
    Cassandra keeps accumulating `cdc_raw` segments that no longer have a consumer — eventually hitting the cap
    above and blocking all graph writes. When shutting the pipeline down permanently, first run
    `ALTER TABLE <keyspace>.edgestore WITH cdc = false;` against the cluster, then stop the Debezium connector and
    the workers, then flip the JanusGraph options.

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
- `cdc.enabled=true`, `cdc.synchronous=false` — **cdc-only mode**. Synchronous mixed-index additions and refreshes are
  skipped; the CDC pipeline performs them instead. The few relation-document deletions that change events cannot
  identify remain synchronous; see [Deletions in cdc-only mode](#deletions-in-cdc-only-mode). Maximum efficiency; the
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

Both options are managed **cluster-wide** (`GLOBAL_OFFLINE`, like `index.[X].backend` and `storage.cql.cdc`), so every
JanusGraph instance and the CDC worker read the same stored value and cannot disagree about who maintains an index. On
a **new** graph the values above are taken from the properties file at first startup. On an **existing** graph a
properties-file entry is ignored (with a warning); change the stored value via the management API while no other
instance is open, then restart:

```groovy
mgmt = graph.openManagement()
mgmt.set('index.search.cdc.enabled', true)
mgmt.set('index.search.cdc.synchronous', false)
mgmt.commit()
```

(Setting `cdc.synchronous=false` without `cdc.enabled=true` has no effect and is reported with a warning at startup.)

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

!!! warning "Debezium 3.6+: put the OpenTelemetry API on the connector classpath"
    Since Debezium 3.6 the connector calls the OpenTelemetry API for every Kafka record it produces, but its standalone
    `jar-with-dependencies` does not contain that API. Without it the connector reads the commit log, but its queue
    processor stops with a `NoClassDefFoundError` and no change reaches Kafka. Add `io.opentelemetry:opentelemetry-api`
    and its `opentelemetry-context` and `opentelemetry-common` dependencies to the classpath. `java -jar` ignores
    `-cp`, so start the connector with its main class instead:

    ```bash
    java -cp debezium-connector-cassandra-4-<version>-jar-with-dependencies.jar:\
    opentelemetry-api-<otel>.jar:opentelemetry-context-<otel>.jar:opentelemetry-common-<otel>.jar \
        io.debezium.connector.cassandra.CassandraConnectorTask cassandra-connector.properties
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

The worker uses the Apache Kafka 4 Java client, which works with Kafka brokers 2.1 or newer.

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
`cdc.consumer.*` — the at-least-once guarantee depends on them. `auto.offset.reset` is only *defaulted* to `earliest`
(a brand-new consumer group starts from the retained CDC backlog rather than silently skipping it). Prefer keeping
`earliest`: the reset strategy applies to **every assignment of a partition that has no committed offset yet** — not
just group creation — and a partition's first offset commit only happens after its first successfully applied
non-empty batch. Overriding to `latest` therefore risks permanently skipping events whenever a rebalance or restart
hits a partition before its first commit (for example, the index backend is down at initial rollout, or a second
worker joins during a staggered fleet start). Since reindexing is idempotent, replaying an already-`REINDEX`-covered
backlog under `earliest` is merely redundant work, not a correctness problem. `group.instance.id` (Kafka static
membership) may only be combined with `cdc.worker-threads=1`, and each worker process must then use a distinct value
— the worker refuses to start otherwise, because consumers sharing one static id perpetually fence each other.

The worker opens a read connection to the graph (to read current element state and to obtain the ElasticSearch index
transaction), so it must be able to reach both Cassandra and ElasticSearch. Offsets are committed only after a batch is
durably applied (at-least-once); if a batch cannot be applied after the retry budget it is reprocessed rather than
skipped, so the index eventually catches up instead of silently going stale.

## Operational notes

- **Latency** — the index trails the graph by roughly the commit-log flush + Debezium read + Kafka + worker + ES refresh
  time. Tune the Cassandra commit-log settings above and the worker poll/batch settings for your throughput.
- **Supported elements** — vertices, vertex properties, and edges (including edge properties) are all reindexed.
- **Delivery** — at-least-once; the reindex-from-current-state model makes duplicates and out-of-order delivery safe.
  With a replication factor above 1, duplication is *systematic*, not exceptional: every replica's commit log records
  each mutation and the per-node connectors each publish it, so every change is delivered (and reindexed) roughly RF
  times. Harmless for correctness, but size the Kafka topic, the workers, and the index backend for RF× the graph's
  mutation rate.
- **Why concurrent workers converge** — an edge's two row copies carry different Kafka keys, so two workers can
  process the same edge's events concurrently, and a write derived from an older read can transiently land after a
  fresher write. This can never end up permanently stale: the mutation that made a read stale also enqueued a newer
  event *behind* the stale one on **every** key-stream that stores a copy of the element (every edge mutation
  rewrites both row copies), each partition is processed in order, and each event's processing reads storage
  at-or-after its own mutation — so the last write for any document is always derived from a read taken at-or-after
  the element's final mutation. Document *existence* is the one dimension without such a successor event when a
  deletion is applied synchronously (no change event identifies it), which is exactly what the worker's post-write
  verification re-read covers.
- **Long index-backend outages** — while ElasticSearch is unreachable, a worker retries the in-flight batch
  (`cdc.retry.*`, each attempt itself retrying up to `storage.write-time`) without calling `poll()`. If the combined
  retry time exceeds Kafka's `max.poll.interval.ms` (default 5 minutes), Kafka evicts the worker from the consumer
  group and redelivers the batch after it rejoins — safe (reindexing is idempotent) but noisy, as workers cycle
  through eviction/rejoin until the backend recovers. For long outages either raise
  `cdc.consumer.max.poll.interval.ms` or lower `cdc.retry.limit`/`storage.write-time` so a failing batch is handed
  back to Kafka (rewind, no offsets committed) before the deadline.
- **Adding a mixed index later** — the worker discovers CDC-managed indexes at startup. After creating a new mixed
  index on a CDC backing, restart the workers and run a `REINDEX` (required for pre-existing data anyway); the reindex
  also covers any changes consumed between index creation and the restart.

### Deletions in cdc-only mode

cdc-only mode skips the synchronous mixed-index **additions and refreshes** — the CDC worker can always rebuild those
from the element's current state. Deletions split by one question: **can the change events identify the deleted
document?**

- **Event-identified deletions stay on the CDC path.** A removed **vertex document** is keyed by the vertex id, which
  *is* the partition key every event carries — even a whole-row partition delete. A removed **MULTI-multiplicity
  edge** whose other endpoint survives the transaction leaves an ordinary column tombstone on the surviving
  endpoint's row (its *mirror copy*), whose column carries the full edge identity. In particular, removing a
  super-node with `storage.drop-whole-row-on-vertex-removal` (the default on Cassandra) while its neighbors survive
  costs **one partition delete and zero synchronous index operations**: the worker removes the vertex document from
  the partition-delete event and every edge document from its mirror tombstone.
- **Event-unidentifiable deletions are written synchronously by the deleting transaction** — the only place their
  identities exist. These are: **constrained-multiplicity edges** (`SIMPLE`, `MANY2ONE`, `ONE2MANY`, `ONE2ONE`) and
  **meta-properties**, whose relation ids live in the storage *value* region that tombstones never carry; and edges
  with **no surviving mirror** — both endpoints removed in one transaction (e.g. dropping a connected subgraph),
  self-loops on removed vertices, and unidirected edges whose out-vertex is removed.

This is convergence-safe in both directions: the worker only ever writes from current graph state, so a late CDC
event for a removed relation results in the same removal (idempotent), never a resurrected document. The one
interleaving where a worker write could race a *synchronous* deletion — the worker reads a relation as live, a
concurrent transaction then deletes it and issues its synchronous document removal, and the worker's write lands
last — is closed by the worker itself: after writing, it re-reads every written relation on a fresh snapshot and
removes the documents of any that vanished, which decides every interleaving correctly.

Two operational notes follow from this design:

- If the index backend is unreachable while a transaction commits one of the **synchronous** deletions above, that
  document removal is lost until transaction-recovery repairs it — exactly the same exposure dual mode has for every
  synchronous write. For this reason, **cdc-only deployments should enable the transaction write-ahead log
  (`tx.log-tx=true`) and run the transaction-recovery process**: the WAL durably records every deleted relation's
  identity together with the transaction, and recovery re-derives the index state from it — closing the one gap the
  change stream cannot cover. Note that a `REINDEX` cannot repair a lost *removal*: it scans the graph, so it never
  visits documents of already-deleted elements.
- Relation **updates** never remove documents synchronously: an update re-adds the relation under the same id, so
  the synchronous delete is skipped and the worker refreshes the (briefly stale) document from current state —
  otherwise a delayed synchronous delete could erase the worker's rewrite with no later event to restore it.
  (Edge labels with `ConsistencyModifier.FORK` re-create the relation under a *new* id instead: the old document is
  removed synchronously when no event identifies it, and the new one is created by the worker.)

### CDC and document TTL (Solr)

Document TTL — Solr is the only in-tree index backend that supports it — is attached to mixed-index documents only by
the **synchronous** write path. The CDC worker's reindex-from-current-state (like JanusGraph's transaction-recovery
restore path, which shares the same `restore` SPI) writes documents **without** a TTL, and a storage-level TTL expiry
produces **no CDC event** (Cassandra expires the cells silently). This bites in *both* modes: in cdc-only mode the
documents of TTL'd elements are written without an expiry from the start, and even in dual mode the worker's very
first rewrite of a document replaces the synchronously-written TTL'd version with an expiry-less one. Either way the
document is never removed when the graph data expires, and a `REINDEX` cannot remove documents of already-expired
elements. **Do not enable CDC (either mode) for Solr mixed indexes over element types that carry a TTL**
(`mgmt.setTTL(...)`); keep those indexes fully synchronous.

## Testing

The JanusGraph-owned half of the pipeline (decode → worker → reindex → ElasticSearch `_bulk`) is verified end-to-end
against a **real Kafka and real ElasticSearch** (Testcontainers) in `CdcKafkaElasticsearchTest`, covering vertex
add/update/remove and edge add/remove. Duplicate, out-of-order, and stale-event convergence is verified in
`CdcWorkerConvergenceTest` and `MixedIndexUpdateApplierTest` (worker and applier over a real graph + Lucene). The
unit/component suite additionally verifies the decoder against real JanusGraph-serialized bytes, the consumer loop
(dedup, retry, rewind, offset-after-success), and the commit-side skip behavior.

The complete pipeline — including the Cassandra → Debezium capture hop — is exercised by
`CdcCassandraDebeziumElasticsearchTest`: it starts a real Cassandra (CDC enabled), Kafka, and ElasticSearch via
Testcontainers, runs the real Debezium Cassandra connector, and asserts ElasticSearch converges to the graph for the
vertex and edge lifecycle. It is gated behind the `cassandra-cdc-e2e` Maven profile, which **auto-activates on JDK
17 through 23** (Debezium 3.x requires Java 17+; the embedded `cassandra-all` 4.1.7 does not run on JDK 24+ — CI uses
17 and 21). The profile supplies the Debezium dependency together with the OpenTelemetry API and the Jetty 12 version
the embedded connector needs, pins SnakeYAML to 1.x for `cassandra-all`, and adds the JVM `--add-opens`/`--add-exports`
flags `cassandra-all` needs. Run it on a JDK 17 with Docker available (the first command builds the module's
dependencies; running `test` directly with `-am` would fail in the upstream modules, where no test matches the filter):

```bash
mvn clean install -DskipTests -pl janusgraph-cdc -am
mvn test -pl janusgraph-cdc -Dtest=CdcCassandraDebeziumElasticsearchTest
```
