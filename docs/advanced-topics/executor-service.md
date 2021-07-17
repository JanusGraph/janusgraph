# Backend ExecutorService

By default JanusGraph uses `ExecutorService` to process some queries in parallel 
(see `storage.parallel-backend-ops` configuration option).  
JanusGraph allows to configure the executor service which is used via configuration options 
provided in `storage.parallel-backend-executor-service` configuration section.

Currently JanusGraph has the next ExecutorService implementations which can be used (controlled via 
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
`ExecutorService` to process CQL queries by default (see `storage.cql.executor-service.enabled` configuration option).  
The rules by which the Cassandra backend `ExecutorService` is built are the 
same as the rules which are used to build parallel backend queries `ExecutorService` (described above). 
The only difference is that the configuration for Cassandra backend `ExecutorService` are provided via configuration 
options under `storage.cql.executor-service`.  
Disabling CQL executor service reduces overhead of thread pool but requires the user to tune maximum throughput thoroughly.  
With disabled CQL executor service the parallelism will be controlled internally by the CQL driver via the next properties: 
`storage.cql.max-requests-per-connection`, `storage.cql.local-max-connections-per-host`, `storage.cql.remote-max-connections-per-host`. 

!!! info
    It is recommended to disable CQL executor service in a production environment and properly configure maximum throughput. 
    CQL executor service does not provide any benefits other than limiting the amount of parallel requests per JanusGraph instance.

!!! warning
    Improper tuning of maximum throughput might result in failures under heavy workloads.
