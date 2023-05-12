# Batch Processing

In order to answer queries, JanusGraph has to perform queries
against the storage backend.
In general, there are two ways of doing this:

- Once data from the backend is needed, execute a backend
  query and continue with the result.
- Maintain a list of what data is needed.
  Once the list reaches a certain size, execute a batched
  backend query to fetch all of it at once.

The first option tends to be more responsive and consume less
memory because the query can emit the first results very early
without waiting for larger batches of queries to complete.
It however sends many small queries to the storage backend for traversals
that traverse a high number of vertices which leads to poor performance.
That is why JanusGraph uses batch processing by default.
Both of these options are described in greater detail below, including
information about configuring batch processing.

!!! note
    The default setting was changed in version 1.0.0.
    Older versions of JanusGraph used no batch processing (first option)
    by default.

## No Batch Processing
In terms of graph traversals, the execution of queries is
loosely coupled to the principle of Depth-First-Search.

### Use this configuration in use cases where for example ...
- ... each query only accesses few vertices of the graph.
- ... your application does not need the _full_ result set
  immediately but rather requires a low latency for the first
  results to arrive.

### Possible limitations
- Traversing large neighborhoods can make the query slow.

### Steps to explicitly configure this option:
- Ensure `query.batch` is set to `false`

## Unrestricted Batch Processing

Using this configuration, each step which traverses the Graph
starting from a vertex (so e.g. `in()`, `outE()` and `values()`
but not `inV()` or `otherV()` and also not `valueMap()`, see
[#2444](https://github.com/JanusGraph/janusgraph/issues/2444))
becomes a blocking operator which means that it produces no
results until all the results of the previous step are known.
Only then, a single backend query is executed and the results
are passed to the next step. Manual `barrier()` steps do not
affect this in any meaningful way.
This way of execution can be thought of as a
Breadth-First-Search.

### Use this configuration in use cases where for example ...
- ... your queries are likely to access multiple vertices in
  each step.
- ... there is a significant network latency between JanusGraph
  and the storage backend.

### Possible limitations
- Increased memory consumption
- If limit steps occur late in the query, there might be an
  unnecessary overhead produced by the steps before the limit
  step.
- Performing very large backend queries could stress the
  storage backend.

### Steps to explicitly configure this option:
- Ensure `query.batch` is set to `true`
- Ensure `query.limited-batch` is set to `false`

## Limited Batch Processing

Using this configuration, each step which traverses the Graph
starting from a vertex (so e.g. `in()`, `outE()` and `values()`
but not `inV()` or `otherV()`) aggregates a number of vertices
first, before executing a batched backend query.
This aggregation phase and backend query phase will repeat
until all vertices are processed.
In contrast to _unrestricted batch processing_ where one batch
corresponds to one step in the query, this approach can
construct multiple batches per step.

This is the default configuration of JanusGraph since version 1.0.0.

### Configuring the batch size
Although batch size does not necessarily need to be configured,
it can provide an additional tuning parameter to improve the
performance of a query.
By default, the batch size for [TinkerPop's barrier step](https://tinkerpop.apache.org/docs/current/reference/#barrier-step)
will be provided by `LazyBarrierStrategy`, which is currently at `2500`. 
For batchable cases where `LazyBarrierStrategy` doesn't inject any `barrier` steps,
the barrier step will be ingected with the size configured via `query.limited-batch-size` 
(which defaults to `2500`, same as with `LazyBarrierStrategy`).  
The batch size of each vertex step can be individually
configured by prepending a `barrier(<size>)` step.
For example, in the query below, the first `out()` step would
use the default batch size of 2500 and the second `out()` step
would use a manually configured batch size of 1234:
```groovy
g.V(list_of_vertices).out().barrier(1234).out()
```
Using the same mechanism, the limit can also be increased or
even effectively disabled by configuring an arbitrarily high
value.

For local traversals which start with a vertex step, the limit
is best configured outside the local traversal, as seen below:
```groovy
g.V(list_of_vertices).out().barrier(1234).where(__.out())
```
The reason this is necessary is that traversers enter local
traversals one by one. As part of the local traversal, the
`barrier(1234)` step would not be allowed to aggregate multiple
traversers.

A special case applies to `repeat()` steps.
Because the local traversal of a `repeat()` step has two inputs
(first, the step before the `repeat()` step and second, the
last step of the repeated traversal, which feeds the result
back to the beginning), two limits can be configured here.
```groovy
g.V(list_of_vertices).barrier(1234).repeat(__.barrier(2345).out()).times(5)
```
Because the local traversal's output is also the input for the
next iteration, the `barrier(1234)` step in front of the local
traversal can only aggregate traversers once they enter the
`repeat` step for the first time. For each iteration, the inner
`barrier(2345)` is used to aggregate traversers from the
previous iteration.

### Use this configuration in use cases where for example ...
- ... you have a mixture of traversals that traverse a high number of vertices
  and traversals that only access few vertices of the graph.

### Possible limitations
- Increased memory consumption (compared to no batch processing)
- The performance of queries depends on the configured batch
  size.
  If you use this configuration, make sure that the
  latency and throughput of your queries meet your
  requirements and if not, tweak the batch size accordingly.

### Steps to explicitly configure this option:
- Ensure `query.batch` is set to `true`
- Ensure `query.limited-batch` is set to `true`
