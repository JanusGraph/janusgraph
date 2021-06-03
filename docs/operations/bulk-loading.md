# Bulk Loading

There are a number of configuration options and tools that make
ingesting large amounts of graph data into JanusGraph more efficient.
Such ingestion is referred to as *bulk loading* in contrast to the
default *transactional loading* where small amounts of data are added
through individual transactions.

There are a number of use cases for bulk loading data into JanusGraph,
including:

-   Introducing JanusGraph into an existing environment with existing
    data and migrating or duplicating this data into a new JanusGraph
    cluster.

-   Using JanusGraph as an end point of an
    [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load)
    process.

-   Adding an existing or external graph datasets (e.g. publicly
    available [RDF datasets](https://www.w3.org/wiki/DataSetRDFDumps)) to a running
    JanusGraph cluster.

-   Updating a JanusGraph graph with results from a graph analytics job.

This page describes configuration options and tools that make bulk
loading more efficient in JanusGraph. Please observe the limitations and
assumptions for each option carefully before proceeding to avoid data
loss or data corruption.

This documentation focuses on JanusGraph specific optimization. In
addition, consider improving the chosen storage backend and (optional)
index backend for high write performance. Please refer to the
documentation of the respective backend for more information.

## Configuration Options

### Batch Loading

Enabling the `storage.batch-loading` configuration option will have the
biggest positive impact on bulk loading times for most applications.
Enabling batch loading disables JanusGraph internal consistency checks
in a number of places. Most importantly, it disables locking. In other
words, JanusGraph assumes that the data to be loaded into JanusGraph is
consistent with the graph and hence disables its own checks in the
interest of performance.

In many bulk loading scenarios it is significantly cheaper to ensure
data consistency prior to loading the data than ensuring data
consistency while loading it into the database. The
`storage.batch-loading` configuration option exists because of this
observation.

For example, consider the use case of bulk loading existing user
profiles into JanusGraph. Furthermore, assume that the username property
key has a unique composite index defined on it, i.e. usernames must be
unique across the entire graph. If the user profiles are imported from
another database, username uniqueness might already guaranteed. If not,
it is simple to sort the profiles by name and filter out duplicates or
writing a Hadoop job that does such filtering. Now, we can enable
`storage.batch-loading` which significantly reduces the bulk loading
time because JanusGraph does not have to check for every added user
whether the name already exists in the database.

**Important**: Enabling `storage.batch-loading` requires the user to
ensure that the loaded data is internally consistent and consistent with
any data already in the graph. In particular, concurrent type creation
can lead to severe data integrity issues when batch loading is enabled.
Hence, we **strongly** encourage disabling automatic type creation by
setting `schema.default = none` in the graph configuration.

### Optimizing ID Allocation

#### ID Block Size

Each newly added vertex or edge is assigned a unique id. JanusGraph’s id
pool manager acquires ids in blocks for a particular JanusGraph
instance. The id block acquisition process is expensive because it needs
to guarantee globally unique assignment of blocks. Increasing
`ids.block-size` reduces the number of acquisitions but potentially
leaves many ids unassigned and hence wasted. For transactional workloads
the default block size is reasonable, but during bulk loading vertices
and edges are added much more frequently and in rapid succession. Hence,
it is generally advisable to increase the block size by a factor of 10
or more depending on the number of vertices to be added per machine.

**Rule of thumb**: Set `ids.block-size` to the number of vertices you
expect to add per JanusGraph instance per hour.

**Important:** All JanusGraph instances MUST be configured with the same
value for `ids.block-size` to ensure proper id allocation. Hence, be
careful to shut down all JanusGraph instances prior to changing this
value.

#### ID Acquisition Process

When id blocks are frequently allocated by many JanusGraph instances in
parallel, allocation conflicts between instances will inevitably arise
and slow down the allocation process. In addition, the increased write
load due to bulk loading may further slow down the process to the point
where JanusGraph considers it failed and throws an exception. There are
three configuration options that can be tuned to avoid this.

1) `ids.authority.wait-time` configures the time in milliseconds the id
pool manager waits for an id block application to be acknowledged by the
storage backend. The shorter this time, the more likely it is that an
application will fail on a congested storage cluster.

**Rule of thumb**: Set this to the sum of the 95th percentile read and
write times measured on the storage backend cluster under load.
**Important**: This value should be the same across all JanusGraph
instances.

2) `ids.renew-timeout` configures the number of milliseconds
JanusGraph’s id pool manager will wait in total while attempting to
acquire a new id block before failing.

**Rule of thumb**: Set this value to be as large feasible to not have to
wait too long for unrecoverable failures. The only downside of
increasing it is that JanusGraph will try for a long time on an
unavailable storage backend cluster.

### Optimizing Writes and Reads

#### Buffer Size

JanusGraph buffers writes and executes them in small batches to reduce
the number of requests against the storage backend. The size of these
batches is controlled by `storage.buffer-size`. When executing a lot of
writes in a short period of time, it is possible that the storage
backend can become overloaded with write requests. In that case,
increasing `storage.buffer-size` can avoid failure by increasing the
number of writes per request and thereby lowering the number of
requests.

However, increasing the buffer size increases the latency of the write
request and its likelihood of failure. Hence, it is not advisable to
increase this setting for transactional loads and one should carefully
experiment with this setting during bulk loading.

#### Read and Write Robustness

During bulk loading, the load on the cluster typically increases making
it more likely for read and write operations to fail (in particular if
the buffer size is increased as described above).
`storage.read-attempts` and `storage.write-attempts` configure how many
times JanusGraph will attempt to execute a read or write operation
against the storage backend before giving up. If it is expected that
there is a high load on the backend during bulk loading, it is generally
advisable to increase these configuration options.

`storage.attempt-wait` specifies the number of milliseconds that
JanusGraph will wait before re-attempting a failed backend operation. A
higher value can ensure that operation re-tries do not further increase
the load on the backend.

## Strategies

### Parallelizing the Load

By parallelizing the bulk loading across multiple machines, the load
time can be greatly reduced if JanusGraph’s storage backend cluster is
large enough to serve the additional requests. This is essentially the
approach [JanusGraph with TinkerPop’s Hadoop-Gremlin](../advanced-topics/hadoop.md) takes to bulk loading data into JanusGraph
using MapReduce.

If Hadoop cannot be used for parallelizing the bulk loading process,
here are some high level guidelines for effectively parallelizing the
loading process:

-   In some cases, the graph data can be decomposed into multiple
    disconnected subgraphs. Those subgraphs can be loaded independently
    in parallel across multiple machines (for instance, using BatchGraph
    as described above).

-   If the graph cannot be decomposed, it is often beneficial to load in
    multiple steps where the last two steps can be parallelized across
    multiple machines:

    1.  Make sure the vertex and edge data sets are de-duplicated and
        consistent.

    2.  Set `batch-loading=true`. Possibly optimize additional
        configuration settings described above.

    3.  Add all the vertices with their properties to the graph (but no
        edges). Maintain a (distributed) map from vertex id (as defined
        by the loaded data) to JanusGraph’s internal vertex id (i.e.
        `vertex.getId()`) which is a 64 bit long id.

    4.  Add all the edges using the map to look-up JanusGraph’s vertex
        id and retrieving the vertices using that id.

## Q&A

-   **What should I do to avoid the following exception during
    batch-loading:**
    `java.io.IOException: ID renewal thread on partition [X] did not complete in time.`?
    This exception is mostly likely caused by repeated time-outs during
    the id allocation phase due to highly stressed storage backend.
    Refer to the section on *ID Allocation Optimization* above.
