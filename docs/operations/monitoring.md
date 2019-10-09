# Monitoring JanusGraph

## Metrics in JanusGraph

JanusGraph supports [Metrics](https://dropwizard.io/). JanusGraph can
measure the following:

-   The number of transactions begun, committed, and rolled back

-   The number of attempts and failures of each storage backend
    operation type

-   The response time distribution of each storage backend operation
    type

### Configuring Metrics Collection

To enable Metrics collection, set the following in JanusGraph’s
properties file:
```properties
# Required to enable Metrics in JanusGraph
metrics.enabled = true
```

This setting makes JanusGraph record measurements at runtime using
Metrics classes like Timer, Counter, Histogram, etc. To access these
measurements, one or more Metrics reporters must be configured as
described in the section [Configuring Metrics Reporting](#configuring-metrics-reporting).

#### Customizing the Default Metric Names

JanusGraph prefixes all metric names with "org.janusgraph" by default.
This prefix can be set through the `metrics.prefix` configuration
property. For example, to shorten the default "org.janusgraph" prefix to
just "janusgraph":
```properties
# Optional
metrics.prefix = janusgraph
```

#### Transaction-Specific Metrics Names

Each JanusGraph transaction may optionally specify its own Metrics name
prefix, overriding both the default Metrics name prefix and the
`metrics.prefix` configuration property. For example, the prefix could
be changed to the name of the frontend application that opened the
JanusGraph transaction. Note that Metrics maintains a ConcurrentHashMap
of metric names and their associated objects in memory, so it’s probably
a good idea to keep the number of distinct metric prefixes small.

To do this, call `TransactionBuilder.setMetricsPrefix(String)`:
```java
JanusGraph graph = ...;
TransactionBuilder tbuilder = graph.buildTransaction();
JanusGraphTransaction tx = tbuilder.groupName("foobar").start();
```

#### Separating Metrics by Backend Store

JanusGraph combines the Metrics for its various internal storage backend
handles by default. All Metrics for storage backend interactions follow
the pattern "&lt;prefix&gt;.stores.&lt;opname&gt;", regardless of
whether they come from the ID store, edge store, etc. When
`metrics.merge-basic-metrics = false` is set in JanusGraph’s properties
file, the "stores" string in metric names is replaced by "idStore",
"edgeStore", "vertexIndexStore", or "edgeIndexStore".

#### Index Provider Metrics

JanusGraph collects basic metrics for mixed index operations. These metrics 
can be found here `<prefix>.indexProvider.<INDEX-NAME>.<opname>`.

## Configuring Metrics Reporting

JanusGraph supports the following Metrics reporters:

-   [Console](#console-reporter)
-   [CSV](#csv-file-reporter)
-   [Ganglia](#ganglia-reporter)
-   [Graphite](#graphite-reporter)
-   [JMX](#jmx-reporter)
-   [Slf4j](#slf4j-reporter)
-   [User-provided/Custom](#custom-reporter)

Each reporter type is independent and can coexist with other reporter types.
For example, it’s possible to configure JMX and Slf4j Metrics
reporters to operate simultaneously. Just set all their respective
configuration keys in janusgraph.properties (and enable metrics as
directed above).

### Console Reporter

<table>
<caption>Metrics Console Reporter Configuration Options</caption>
<thead>
<tr>
<th>Config Key</th>
<th>Required?</th>
<th>Value</th>
<th>Default</th>
</tr>
</thead>
<tbody>
<tr>
<td>metrics.console.interval</td>
<td>yes</td>
<td>Milliseconds to wait between dumping metrics to the console</td>
<td>null</td>
</tr>
</tbody>
</table>

Example janusgraph.properties snippet that prints metrics to the console
once a minute:
```properties
metrics.enabled = true
# Required; specify logging interval in milliseconds
metrics.console.interval = 60000
```

### CSV File Reporter

<table>
<caption>Metrics CSV Reporter Configuration Options</caption>
<thead>
<tr>
<th>Config Key</th>
<th>Required?</th>
<th>Value</th>
<th>Default</th>
</tr>
</thead>
<tbody>
<tr>
<td>metrics.csv.interval</td>
<td>yes</td>
<td>Milliseconds to wait between writing CSV lines</td>
<td>null</td>
</tr>
<tr>
<td>metrics.csv.directory</td>
<td>yes</td>
<td>Directory in which CSV files are written (will be created if it does not exist)</td>
<td>null</td>
</tr>
</tbody>
</table>

Example janusgraph.properties snippet that writes CSV files once a
minute to the directory `./foo/bar/` (relative to the process’s working
directory):
```properties
metrics.enabled = true
# Required; specify logging interval in milliseconds
metrics.csv.interval = 60000
metrics.csv.directory = foo/bar
```

### Graphite Reporter

<table>
<caption>Metrics Graphite Reporter Configuration Options</caption>
<thead>
<tr>
<th>Config Key</th>
<th>Required?</th>
<th>Value</th>
<th>Default</th>
</tr>
</thead>
<tbody>
<tr>
<td>metrics.graphite.hostname</td>
<td>yes</td>
<td>IP address or hostname to which <a href="https://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-plaintext-protocol">Graphite plaintext protocol</a> data are sent</td>
<td>null</td>
</tr>
<tr>
<td>metrics.graphite.interval</td>
<td>yes</td>
<td>Milliseconds to wait between pushing data to Graphite</td>
<td>null</td>
</tr>
<tr>
<td>metrics.graphite.port</td>
<td>no</td>
<td>Port to which Graphite plaintext protocol reports are sent</td>
<td>2003</td>
</tr>
<tr>
<td>metrics.graphite.prefix</td>
<td>no</td>
<td>Arbitrary string prepended to all metric names sent to Graphite</td>
<td>null</td>
</tr>
</tbody>
</table>

Example janusgraph.properties snippet that sends metrics to a Graphite
server on 192.168.0.1 every minute:
```properties
metrics.enabled = true
# Required; IP or hostname string
metrics.graphite.hostname = 192.168.0.1
# Required; specify logging interval in milliseconds
metrics.graphite.interval = 60000
```

### JMX Reporter

<table>
<caption>Metrics JMX Reporter Configuration Options</caption>
<thead>
<tr>
<th>Config Key</th>
<th>Required?</th>
<th>Value</th>
<th>Default</th>
</tr>
</thead>
<tbody>
<tr>
<td>metrics.jmx.enabled</td>
<td>yes</td>
<td>Boolean</td>
<td>false</td>
</tr>
<tr>
<td>metrics.jmx.domain</td>
<td>no</td>
<td>Metrics will appear in this JMX domain</td>
<td>Metrics’s own default</td>
</tr>
<tr>
<td>metrics.jmx.agentid</td>
<td>no</td>
<td>Metrics will be reported with this JMX agent ID</td>
<td>Metrics’s own default</td>
</tr>
</tbody>
</table>

Example janusgraph.properties snippet:
```properties
metrics.enabled = true
# Required
metrics.jmx.enabled = true
# Optional; if omitted, then Metrics uses its default values
metrics.jmx.domain = foo
metrics.jmx.agentid = baz
```

### Slf4j Reporter

<table>
<caption>Metrics Slf4j Reporter Configuration Options</caption>
<thead>
<tr>
<th>Config Key</th>
<th>Required?</th>
<th>Value</th>
<th>Default</th>
</tr>
</thead>
<tbody>
<tr>
<td>metrics.slf4j.interval</td>
<td>yes</td>
<td>Milliseconds to wait between dumping metrics to the logger</td>
<td>null</td>
</tr>
<tr>
<td>metrics.slf4j.logger</td>
<td>no</td>
<td>Slf4j logger name to use</td>
<td>"metrics"</td>
</tr>
</tbody>
</table>

Example janusgraph.properties snippet that logs metrics once a minute to
the logger named `foo`:
```properties
metrics.enabled = true
# Required; specify logging interval in milliseconds
metrics.slf4j.interval = 60000
# Optional; uses Metrics default when unset
metrics.slf4j.logger = foo
```

### User-Provided/Custom Reporter

In case the Metrics reporter configuration options listed above are
insufficient, JanusGraph provides a utility method to access the single
`MetricRegistry` instance which holds all of its measurements.
```java
com.codahale.metrics.MetricRegistry janusgraphRegistry = 
    org.janusgraph.util.stats.MetricManager.INSTANCE.getRegistry();
```

Code that accesses `janusgraphRegistry` this way can then attach
non-standard reporter types or standard reporter types with exotic
configurations to `janusgraphRegistry`. This approach is also useful if
the surrounding application already has a framework for Metrics reporter
configuration, or if the application needs multiple
differently-configured instances of one of JanusGraph’s supported
reporter types. For instance, one could use this approach to setup
multiple unicast Graphite reporters whereas JanusGraph’s properties
configuration is limited to just one Graphite reporter.
