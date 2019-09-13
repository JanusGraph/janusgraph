# HBase Storage, Solr Index

## About HBase and Solr

[Apache HBase](https://hbase.apache.org/) is a scalable, distributed big data
store.

[Apache Solr](https://lucene.apache.org/solr/) is a scalable, distributed
search engine.

> Check the JanusGraph [version compatibility](https://docs.janusgraph.org/changelog/#version-compatibility)
to ensure you select versions of HBase and Solr compatible with this
JanusGraph release.

## JanusGraph configuration

* Be aware that Solr has two configuration options: SolrCloud or HTTP. With
either option, there is manual configuration required for the Solr cores (config
sets). Refer to the JanusGraph [Solr documentation](https://docs.janusgraph.org/index-backend/solr/)
for additional details.

    * [`jgex-hbase-solr-cloud.properties`](conf/jgex-hbase-solr-cloud.properties)
    contains the HBase and SolrCloud server locations.

    * [`jgex-hbase-solr-http.properties`](conf/jgex-hbase-solr-http.properties)
    contains the HBase and Solr Standalone (HTTP) server locations

* By providing different values for `storage.hbase.table` and `index.jgex.index-name`,
you can store multiple graphs on the same HBase and Solr servers. Refer to
the JanusGraph [configuration reference](https://docs.janusgraph.org/basics/configuration-reference/)
for additional properties.

* [`logback.xml`](conf/logback.xml) configures logging with [Logback](https://logback.qos.ch/).
The example configuration logs to the console and adjusts the logging level
for some noisier packages. Refer to the Logback [manual](https://logback.qos.ch/manual/index.html)
for additional details.

### HBase configuration

The JanusGraph properties file assumes that HBase is installed on localhost
using its quickstart configuration. The quickstart configuration uses the
local filesystem for storing data and manages its own local Zookeeper. Please
refer to the HBase documentation for installation instructions.

### Solr configuration

The JanusGraph properties file assumes that Solr is installed on localhost
using its default configuration. Please refer to the Solr documentation for
installation instructions.

SolrCloud requires Zookeeper for sharing configset information. The JanusGraph
properties file assumes that SolrCloud is starting its own local Zookeeper,
rather than sharing a common Zookeeper with HBase.

## Dependencies

The required Maven dependencies for HBase:

```
        <dependency>
            <groupId>org.janusgraph</groupId>
            <artifactId>janusgraph-hbase</artifactId>
            <version>${janusgraph.version}</version>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-shaded-client</artifactId>
            <version>${hbase1.version}</version>
            <scope>runtime</scope>
        </dependency>
```

The required Maven dependency for Solr:

```
        <dependency>
            <groupId>org.janusgraph</groupId>
            <artifactId>janusgraph-solr</artifactId>
            <version>${janusgraph.version}</version>
            <scope>runtime</scope>
        </dependency>
```

## HBase and SolrCloud

### Upload the configset to Zookeeper

Before running the example, a configset must be uploaded to Zookeeper.
Using a configset makes it possible to reuse the same configuration for new
cores. The configset is stored in Zookeeper under `/configs/jgex` where the
name `jgex` matches the properties file value for `index.jgex.solr.configset`.
Make sure the Zookeeper url matches the properties value for `index.jgex.solr.zookeeper-url`.

```
# Solr 5
$SOLR_HOME/server/scripts/cloud-scripts/zkcli.sh -z 127.0.0.1:9983 -cmd upconfig -d $JANUSGRAPH_HOME/conf/solr -n jgex

# Solr 6 or higher
$SOLR_HOME/bin/solr zk upconfig -z 127.0.0.1:9983 -d $JANUSGRAPH_HOME/conf/solr -n jgex
```

### Run the example

This command can be run from the `examples` or the project's directory.

```
mvn exec:java -pl :example-hbase
```

### Drop the graph

After running an example, you may want to drop the graph from storage. Make
sure to stop the application before dropping the graph. This command can be
run from the `examples` or the project's directory.

```
mvn exec:java -pl :example-hbase -Dcmd=drop
```

### Remove the configset from Zookeeper

After dropping the graph, the configset can be removed from Zookeeper.
The configset is stored in Zookeeper under `/configs/jgex` where the name
`jgex` matches the properties file value for `index.jgex.solr.configset`.
Make sure the Zookeeper url matches the properties value for `index.jgex.solr.zookeeper-url`.

```
# Solr 5
$SOLR_HOME/server/scripts/cloud-scripts/zkcli.sh -z 127.0.0.1:9983 -cmd clear /configs/jgex

# Solr 6
$SOLR_HOME/bin/solr zk rm -r /configs/jgex -z 127.0.0.1:9983
```


## HBase and Solr Standalone (HTTP)

### Create the Solr cores

Before running the example, there are additional manual steps needed to create
the Solr Cores. This example uses two cores: `vAge` and `eReasonPlace`. These
core names can be found in `JanusGraphApp#createMixedIndexes`.

```
$ $SOLR_HOME/bin/solr create_core -d $JANUSGRAPH_HOME/conf/solr -c vAge

Copying configuration to new core instance directory:
/usr/lib/solr-5.5.4/server/solr/vAge

Creating new core 'vAge' using command:
http://localhost:8983/solr/admin/cores?action=CREATE&name=vAge&instanceDir=vAge

{
  "responseHeader":{
    "status":0,
    "QTime":577},
  "core":"vAge"}

$ $SOLR_HOME/bin/solr create_core -d $JANUSGRAPH_HOME/conf/solr -c eReasonPlace

Copying configuration to new core instance directory:
/usr/lib/solr-5.5.4/server/solr/eReasonPlace

Creating new core 'eReasonPlace' using command:
http://localhost:8983/solr/admin/cores?action=CREATE&name=eReasonPlace&instanceDir=eReasonPlace

{
  "responseHeader":{
    "status":0,
    "QTime":116},
  "core":"eReasonPlace"}
```

### Run the example

This command can be run from the `examples` or the project's directory.

```
mvn exec:java -pl :example-hbase -Dexample.config="\${project.basedir}/conf/jgex-hbase-solr-http.properties"
```

### Drop the graph

After running an example, you may want to drop the graph from storage. Make
sure to stop the application before dropping the graph. This command can be
run from the `examples` or the project's directory.

```
mvn exec:java -pl :example-hbase -Dexample.config="\${project.basedir}/conf/jgex-hbase-solr-http.properties" -Dcmd=drop
```

### Drop the Solr cores

After dropping the graph, there are additional manual steps needed to delete
the Solr Cores. This example uses two cores: `vAge` and `eReasonPlace`. These
core names can be found in `JanusGraphApp#createMixedIndexes`.

```
$ cd $SOLR_HOME

$ bin/solr delete -c vAge

Deleting core 'vAge' using command:
http://localhost:8983/solr/admin/cores?action=UNLOAD&core=vAge&deleteIndex=true&deleteDataDir=true&deleteInstanceDir=true

{"responseHeader":{
    "status":0,
    "QTime":7}}

$ bin/solr delete -c eReasonPlace

Deleting core 'eReasonPlace' using command:
http://localhost:8983/solr/admin/cores?action=UNLOAD&core=eReasonPlace&deleteIndex=true&deleteDataDir=true&deleteInstanceDir=true

{"responseHeader":{
    "status":0,
    "QTime":9}}
```
