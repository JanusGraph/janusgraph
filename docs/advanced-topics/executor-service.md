# Backend ExecutorService

By default, JanusGraph uses `ExecutorService` to process some queries in parallel for storage backend implementations 
which don't support multi-key queries (see `storage.parallel-backend-ops` configuration option).  
JanusGraph allows to configure the executor service which is used via configuration options 
provided in `storage.parallel-backend-executor-service` configuration section.

Currently, JanusGraph has the next ExecutorService implementations which can be used (controlled via 
`storage.parallel-backend-executor-service.class`):

* `fixed` - fixed thread pool size;
* `cached` - cached thread pool size;
* Custom ExecutorService;

## Custom ExecutorService

To use custom `ExecutorService` the configuration option `storage.parallel-backend-executor-service.class` must be 
provided which must be the full class name of the `ExecutorService` implementation class.

The provided class which implements `ExecutorService` must have either a public constructor with
[ExecutorServiceConfiguration](https://javadoc.io/doc/org.janusgraph/janusgraph-core/latest/org/janusgraph/diskstorage/configuration/ExecutorServiceConfiguration.html)
argument (preferred constructor) or a public parameterless constructor. 
When both constructors are available the constructor with `ExecutorServiceConfiguration` argument will be used.

When custom `ExecutorService` is provided with `ExecutorServiceConfiguration` it isn't required to 
use any of the configuration options provided in `ExecutorServiceConfiguration` but sometimes it is
convenient to use the provided configurations. The configuration options provided in `ExecutorServiceConfiguration` 
are typically those configuration options which are provided via configurations from `storage.parallel-backend-executor-service`.

# Cassandra backend ExecutorService

Apart from the general executor service mentioned in the previous section, Cassandra backend uses an additional 
`ExecutorService` to process result deserialization for CQL queries by default (see `storage.cql.executor-service` configuration options).  
The rules by which the Cassandra backend `ExecutorService` is built are the 
same as the rules which are used to build parallel backend queries `ExecutorService` (described above). 
The only difference is that the configuration for Cassandra backend `ExecutorService` are provided via configuration 
options under `storage.cql.executor-service`.

!!! warning
    By default, `storage.cql.executor-service` is configured to have a core pool size of number of processors multiplied 
    by 2. It's recommended to always use the default value unless there is a reason to artificially limit parallelism for 
    CQL slice query deserialization.
