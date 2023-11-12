# Deployment Scenarios

JanusGraph offers a wide choice of storage and index backends which
results in great flexibility of how it can be deployed. This chapter
presents a few possible deployment scenarios to help with the complexity
that comes with this flexibility.

Before discussing the different deployment scenarios, it is important to
understand the roles of JanusGraph itself and that of the backends.
First of all, applications only communicate directly with JanusGraph,
mostly by sending Gremlin traversals for execution. JanusGraph then
communicates with the configured backends to execute the received
traversal. When JanusGraph is used in the form of JanusGraph Server,
then there is nothing like a *master* JanusGraph Server. Applications
can therefore connect to any JanusGraph Server instance. They can also
use a load-balancer to schedule requests to the different instances. The
JanusGraph Server instances themselves don’t communicate to each other
directly which makes it easy to scale them when the need arises to
process more traversals.

!!! note
    The scenarios presented in this chapter are only examples of how
    JanusGraph can be deployed. Each deployment needs to take into account
    the concrete use cases and production needs.

## Getting Started Scenario

This scenario is the scenario most users probably want to choose when
they are just getting started with JanusGraph. It offers scalability and
fault tolerance with a minimum number of servers required. JanusGraph
Server runs together with an instance of the storage backend and
optionally also an instance of the index backend on every server.

![Getting started deployment scenario diagram](images/getting-started-scenario.svg)

A setup like this can be extended by simply adding more servers of the
same kind or by moving one of the components onto dedicated servers. The
latter describes a growth path to transform the deployment into the
[Advanced Scenario](#advanced-scenario).

Any of the scalable storage backends can be used with this scenario.
Note however that for Scylla [some configuration is required when it is
hosted co-located with other
services](http://docs.scylladb.com/getting-started/scylla_in_a_shared_environment/)
like in this scenario. When an index backend should be used in this
scenario then it also needs to be one that is scalable.

## Advanced Scenario

The advanced scenario is an evolution of the [Getting Started Scenario](#getting-started-scenario).
 Instead of hosting the JanusGraph
Server instances together with the storage backend and optionally also
the index backend, they are now separated on different servers. The
advantage of hosting the different components (JanusGraph Server,
storage/index backend) on different servers is that they can be scaled
and managed independently of each other. This offers a higher
flexibility at the cost of having to maintain more servers.

![Advanced deployment scenario diagram](images/advanced-scenario.svg)

Since this scenario offers independent scalability of the different
components, it of course makes most sense to also use scalable backends.

## Minimalist Scenario

It is also possible to host JanusGraph Server together with the
backend(s) on just one server. This is especially attractive for testing
purposes or for example when JanusGraph just supports a single
application which can then also run on the same server.

![Minimalist deployment scenario diagram](images/minimalist-scenario.svg)

Opposed to the previous scenarios, it makes most sense to use backends
for this scenario that are not scalable. The in-memory backend can be
used for testing purposes or Berkeley DB for production and Lucene as
the optional index backend.

## Embedded JanusGraph

Instead of connecting to the JanusGraph Server from an application it is
also possible to embed JanusGraph as a library inside a JVM based
application. While this reduces the administrative and network overhead, it makes it
impossible to scale JanusGraph independently of the application.
Embedded JanusGraph can be deployed as a variation of any of the other
scenarios. JanusGraph just moves from the server(s) directly into the
application as its now just used as a library instead of an independent
service. You would need to introduce `janusgraph-core` dependency to your
project, as well as the modules needed for your selected backends. For example,
if you have a Maven project and you use Cassandra and Lucene as your backends,
You should add the following dependencies into your `pom.xml`:

```xml
<dependencies>
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>${janusgraph.version}</version>
    </dependency>
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-cql</artifactId>
        <version>${janusgraph.version}</version>
    </dependency>
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-lucene</artifactId>
        <version>${janusgraph.version}</version>
    </dependency>
</dependencies>
```

Then you could start a JanusGraph instance in your application code, similar to 
what you do in gremlin console:

```java
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;

public class MyGraphApp {
    public static void main(String[] args) {
        JanusGraph graph = JanusGraphFactory.open("/path/to/your/config/file");
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.printSchema();
        mgmt.commit();
        graph.close();
    }
}
```

#### Gremlin Parser

JanusGraph server could accept adhoc gremlin queries in plain string format, while
embedded JanusGraph requires you to write Java code for any query. You could develop
your own DSL (domain specific language) on top of JanusGraph to expose the graph query
capabilities to end users. Alternatively, you could leverage the built-in Gremlin parser
to take any gremlin query in plain string format, parse them and execute them in your
application with JanusGraph embedded. To leverage gremlin parser, check out
[graph.script-eval](../configs/configuration-reference.md#graphscript-eval). Once you
turn on the script evaluation option, you could then evaluate gremlin queries using
`JanusGraph::eval` method:

```groovy
graph.eval(/* gremlin script */ "g.V().count().next()",                 /* commit */ false);
graph.eval(/* gremlin script */ "g.addV().next();g.V().count().next()", /* commit */ true)
```

As shown above, you could pass in a single gremlin query or a gremlin script in
plain string format. Optionally, you could commit or rollback the script after
the execution. This is useful when you want to prevent end users from updating the
graph.
