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
- Ensure `query.batch.enabled` is set to `false`

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
- Ensure `query.batch.enabled` is set to `true`
- Ensure `query.batch.limited` is set to `false`

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
the barrier step will be ingected with the size configured via `query.batch.limited-size` 
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
- Ensure `query.batch.enabled` is set to `true`
- Ensure `query.batch.limited` is set to `true`

## Batched Query Processing Flow

Whenever `query.batch.enabled` is set to `true` steps compatible with batch processing are going to be 
executed in batched fashion. Each storage backend may differently execute such batches, but usually 
it means requesting data in parallel for multiple vertices which usually improves query performance 
when the query is accessing many vertices.  

Batched query processing takes into account two types of steps:

1.  Batch compatible step. This is the step which will execute batch requests. Currently, the list of such steps
    is the next: `out()`, `in()`, `both()`, `inE()`, `outE()`, `bothE()`, `has()`, `values()`, `properties()`, `valueMap()`,
    `propertyMap()`, `elementMap()`, `label()`.
2.  Parent step. This is a parent step which has local traversals with the same start. Such parent steps also implement the 
    interface `TraversalParent`. There are many such steps, but as for an example those could be: `and(...)`, `or(...)`, 
    `not(...)`, `order().by(...)`, `project("valueA", "valueB", "valueC").by(...).by(...).by(...)`, `union(..., ..., ...)`,
    `choose(..., ..., ...)`, `coalesce(..., ...)`, `where(...)`, etc. Start of such local steps should be the same, thus,
    the only exception currently are steps `repeat()` and `match()` (see below on how they are processed).  

Parent steps register their vertices for later processing with the batch compatible start step. For example,
```groovy
g.V(v1, v2, v3).union(out("knows"), in("follows"))
```
In the example above vertices `v1`, `v2`, and `v3` will be registered with `out("knows")` and `in("follows")` 
steps for batch processing because their parent step (`union`) registers any input with the batch compatible child start 
steps.    
Moreover, parent steps can register vertices for batch processing even with deep nested batch compatible start steps. 
For example,
```groovy
g.V(v1, v2, v3).
    and(
        union(out("edge1"), in("edge2")),
        or(
            union(out("edge3"), in("edge4").optional(out("edge5"))),
            optional(out("edge6")).in("edge7")))
```
In the example above vertices `v1`, `v2`, and `v3` will be registered with `out("edge1")`, `in("edge2")`, `out("edge3")`, 
`in("edge4")`, and `out("edge6")` steps for batch processing because they all can be considered as starts of the most root parent step (`and` step).
That said, those vertices won't be registered for batch processing with steps `out("edge5")` or `in("edge7")` because those steps 
are either not starting steps or starting steps of other parent steps. As such, `out("edge5")` will be registered with 
any vertex returned from `in("edge4")` step, and `in("edge7")` will be registered with any vertex returned from `optional(out("edge6"))` step.  

#### Batch processing for `repeat` step

Repeat step doesn't follow the rules of other parent steps and registers vertices to child steps differently. 
Currently, TinkerPop's default implementation is using Breadth-First Search instead of Depth-Fist Search (as used for other steps). 

JanusGraph applies repeat step vertices to the start of local `repeat` step, start of local `emit` step in case it is 
placed before `repeat` step, and start of local `until` step in case it is placed before `repeat` step. 
Moreover, for any next iteration JanusGraph applies result of the local `repeat` step (end step) to the beginning of the 
local `repeat` step (start step) as well as start steps of `emit` and `until` traversals. 

##### Use-cases for batch requests per level (`loop`):

1.  Simple example.
    ```groovy
    g.V(v1, v2, v3).repeat(out("knows")).emit()
    ```
    In the above example vertices `v1`, `v2`, and `v3` will be registered with `out("knows")` step because it's the start 
    step with batch support. Moreover, the result of all iterations on the same level (`loop`) of `out("knows")` will be registered 
    back to `out("knows")` for the next level (`loop`) iterations and so on until `out("knows")` stops emitting any results. 

2.  Example with custom `emit` traversal after `repeat`.
    ```groovy
    g.V(v1, v2, v3).repeat(out("knows")).emit(out("follows"))
    ```
    The above example's vertices registration flow is the same as in example `1`, but the difference is that `out("follows")` 
    will receive vertices for registration from `out("knows")` the same way as `out("knows")` step itself receives vertices 
    for registration from itself. Notice, the same logic would apply for `until` step if it were `until` instead of `emit` here.

3.  Example with custom `emit` traversal before `repeat`.
    ```groovy
    g.V(v1, v2, v3).emit(out("follows")).repeat(out("knows"))
    ```
    The above example's vertices registration flow is the same as in example `2`, but the difference is that `out("follows")`
    will receive vertices for registration both from `out("knows")` and the start vertices `v1`, `v2`, `v3`. In other words,
    vertices registration sources for `out("knows")` and `out("follows")` are the same in this case. Notice, the same logic 
    would apply for `until` step if it were `until` instead of `emit` here.

4.  Example with custom `emit` and `until` traversals before `repeat`.
    ```groovy
    g.V(v1, v2, v3).emit(out("follows")).until(out("feeds")).repeat(out("knows"))
    ```
    In the above example all 3 steps `out("follows")`, `out("feeds")`, and `out("knows")` have the same vertices 
    registration flow where they receive vertices both from query start (`v1`, `v2`, `v3`) and from local repeat end 
    step (`out("knows")`).

5.  Example with custom `emit` and `until` traversals after `repeat`.
    ```groovy
    g.V(v1, v2, v3).repeat(out("knows")).emit(out("follows")).until(out("feeds"))
    ```
    The above example's vertices registration flow is the same as in example `4`, but the difference is that 
    `out("follows")` and `out("feeds")` won't receive vertices registration from the query start (`v1`, `v2`, `v3`).

6.  Example with custom `until` traversal before `repeat` and `emit(true)` after `repeat`.
    ```groovy
    g.V(v1, v2, v3).until(out("feeds")).repeat(out("knows")).emit()
    ```
    The above example's vertices registration flow is the same as in example `4`, except that the `emit` traversal 
    doesn't have any start step which supports batching. Thus, `emit` traversal doesn't receive batched vertices registration. 

##### Use-cases for batch requests per iteration:

In most cases (like the above examples `1 - 6` and other cases) TinkerPop's default `repeat` step implementation executes 
the local `repeat` traversal for the whole level (`loop`) before `emit` or `until` is executed. In other words `repeat` traversal 
is executed multiple times (multiple iterations on the same `loop`) before `emit` or `until` first execution on the current `loop`. 
This gives JanusGraph possibility to make larger batch requests containing vertices from multiple `repeat` traversal iterations 
which efficiently executes batch requests.    
That said, there are 3 use-cases when the execution flow is different and TinkerPop executes `until` or `emit` traversals after each 
`repeat` traversal iteration. In such case `until` or `emit` steps will execute batch requests for vertices collected on the 
current iteration only and not for vertices collected from all iterations of the same level (`loop`).    

```groovy
g.V(v1, v2, v3).emit().repeat(out("knows")).until(out("feeds"))
g.V(v1, v2, v3).emit(out("follows")).repeat(out("knows")).until(out("feeds"))
g.V(v1, v2, v3).until(out("feeds")).repeat(out("knows")).emit(out("follows"))
```

The above 3 examples show the pattern when `until` or `emit` executes batches per iteration instead of per level. 
In case any `emit` step is placed before `repeat` step while `until` step is placed after `repeat` step. In case 
`until` step is placed before `repeat` step while non-true `emit` step is placed after `repeat` step.  
In all other cases `repeat` step will be executed for the whole `loop` and only after that `emit` or `until` will be executed.

These limitations might be resolved after JanusGraph adds support for DFS repeat step execution 
([see issue #3787](https://github.com/JanusGraph/janusgraph/issues/3787)).

##### Multi-nested `repeat` step modes:

By default, in cases when batch start steps have multiple `repeat` step parents the batch registration is considering all `repeat` 
parent steps.  
However, in cases when transaction cache is small and repeat step traverses more than one level
deep, it could result for some vertices to be re-fetched again or vertices which don't need to be fetched due to early 
cycle end could potentially be fetched into the transaction cache. It would mean a waste of operation when it isn't necessary.  

Thus, JanusGraph provides a configuration option `query.batch.repeat-step-mode` to control multi-repeat step behaviour:

-  `closest_repeat_parent` (default option) - consider the closest `repeat` step only.
   ```groovy
   g.V().repeat(and(repeat(out("knows")).emit())).emit()
   ```
   In the example above, `out("knows")` will be receiving vertices for batching from the `and` step input for the first iterations 
   as well as the `out("knows")` step output for the next iterations. 
-  `all_repeat_parents` - consider registering vertices from the start and end of each `repeat` step parent.
   ```groovy
   g.V().repeat(and(repeat(out("knows")).emit())).emit()
   ```
   In the example above, `out("knows")` will be receiving vertices for batching from the most outer `repeat` step input 
   (for the first iterations), the most outer `repeat` step output (which is `and` output) (for the first iterations),  
   the `and` step input (for the first iterations), and from the `out("knows")` output (for the next iterations).
-  `starts_only_of_all_repeat_parents` - consider registering vertices from the start of each `repeat` step parent. 
   ```groovy
   g.V().repeat(and(repeat(out("knows")).emit())).emit()
   ```
   In the example above, `out("knows")` will be receiving vertices for batching from the most outer `repeat` step input
   (for the first iterations), the `and` step input (for the first iterations), and from the `out("knows")` output 
   (for the next iterations).

#### Batch processing for `match` step

Currently, JanusGraph supports vertices registration for batch processing inside individual local traversals of the `match` 
step, but not between those local traversals. Also, JanusGraph doesn't register start of the `match` step with any 
of the local traversals of the `match` step. Thus, performance for `match` step might be limited. This is a temporary 
limitation until this feature is implemented ([see issue #3788](https://github.com/JanusGraph/janusgraph/issues/3788)).

#### Batch processing for properties

Some of the Gremlin steps with enabled optimization may prefetch vertex properties in batches. 
As for now, JanusGraph uses slice queries to query part of the row data. A single-slice query contains 
the start key and the end key to define a slice of data JanusGraph is interested in.  
As JanusGraph doesn't support multi-range slice queries right now it can either fetch a single property 
in a single Slice query or all properties in a single slice query. Thus, users have to decide the tradeoff between 
different properties fetching approaches and decide when they want to fetch all properties in a single slice query
(which is usually faster but unnecessary properties might be fetched) or to fetch only requested properties in 
separate slice query per each property (might be slightly slower but will fetch only the requested properties). 

[See issue #3816](https://github.com/JanusGraph/janusgraph/issues/3816) which will allow fetching only requested 
properties via a single slice query.  

See configuration option `query.fast-property` which may be used to pre-fetch all properties on a first singular property 
access when direct vertex properties are requested (for example `vertex.properties("foo")`).  
See configuration option `query.batch.has-step-mode` to control properties pre-fetching behaviour for `has` step.  
See configuration option `query.batch.properties-mode` to control properties pre-fetching behaviour for `values`, 
`properties`, `valueMap`, `propertyMap`, and `elementMap` steps.  
See configuration option `query.batch.label-step-mode` to control labels pre-fetching behaviour for `label` step.  
