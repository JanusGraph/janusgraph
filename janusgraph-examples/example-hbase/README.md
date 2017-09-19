# HBase Storage, Solr Index

## About HBase and Solr

[Apache HBase](http://hbase.apache.org/) is a scalable, distributed big data
store.

[Apache Solr](http://lucene.apache.org/solr/) is a scalable, distributed
search engine.

> Check the JanusGraph [version compatibility](http://docs.janusgraph.org/latest/version-compat.html)
to ensure you select versions of HBase and Solr compatible with this
JanusGraph release.

## JanusGraph configuration

* Be aware that Solr has two configuration options: SolrCloud or HTTP. With
either option, there is manual configuration required for the Solr cores (config
sets). Refer to the JanusGraph [Solr documentation](http://docs.janusgraph.org/latest/solr.html)
for additional details.

    * [`jgex-hbase-solr-cloud.properties`](conf/jgex-hbase-solr-cloud.properties)
    contains the HBase and SolrCloud server locations.

    * [`jgex-hbase-solr-http.properties`](conf/jgex-hbase-solr-http.properties)
    contains the HBase and Solr HTTP server locations

* By providing different values for `storage.hbase.table` and `index.jgex.index-name`,
you can store multiple graphs on the same HBase and Solr servers. Refer to
the JanusGraph [configuration reference](http://docs.janusgraph.org/latest/config-ref.html)
for additional properties.

* [`logback.xml`](conf/logback.xml) configures logging with [Logback](https://logback.qos.ch/).
The example configuration logs to the console and adjusts the logging level
for some noisier packages. Refer to the Logback [manual](https://logback.qos.ch/manual/index.html)
for additional details.

## Run the example

Use [Apache Maven](http://maven.apache.org/) and the [exec-maven-plugin](http://www.mojohaus.org/exec-maven-plugin/java-mojo.html)
to pull in the required jar files onto the runtime classpath.


## HBase and SolrCloud

### Upload the configset to Zookeeper

Using a configset makes it possible to reuse the same configuration for new
cores. The configset is stored in Zookeeper under `/configs/jgex` where the
name `jgex` matches the properties file value for `index.jgex.solr.configset`.
Make sure the Zookeeper url matches the properties value for `index.jgex.solr.zookeeper-url`.

```
$ cd $SOLR_HOME

$ server/scripts/cloud-scripts/zkcli.sh -z 127.0.0.1:9983 -cmd upconfig -d $JANUSGRAPH_HOME/conf/solr -n jgex
```

### Run the program

```
$ cd $JANUSGRAPH_HOME/janusgraph-examples/example-hbase

$ mvn exec:java -Dexec.mainClass="org.janusgraph.example.JanusGraphApp" -Dlogback.configurationFile="conf/logback.xml" -Dexec.args="conf/jgex-hbase-solr-cloud.properties"
```

### Drop the graph

Make sure to stop the application before dropping the graph.

```
$ cd $JANUSGRAPH_HOME/janusgraph-examples/example-hbase

$ mvn exec:java -Dexec.mainClass="org.janusgraph.example.JanusGraphApp" -Dlogback.configurationFile="conf/logback.xml" -Dexec.args="conf/jgex-hbase-solr-cloud.properties drop"
```

### Remove the configset from Zookeeper

The configset is stored in Zookeeper under `/configs/jgex` where the name
`jgex` matches the properties file value for `index.jgex.solr.configset`.
Make sure the Zookeeper url matches the properties value for `index.jgex.solr.zookeeper-url`.

```
$ cd $SOLR_HOME

$ server/scripts/cloud-scripts/zkcli.sh -z 127.0.0.1:9983 -cmd clear /configs/jgex
```


## HBase and Solr HTTP

### Create the Solr cores

The core names match the `vAge` or `eReasonPlace` values when `mgmt.buildIndex()`
defines the mixed indexes in `JanusGraphApp.createMixedIndexes()`

```
$ cd $SOLR_HOME

$ bin/solr create_core -d $JANUSGRAPH_HOME/conf/solr -c vAge

Copying configuration to new core instance directory:
/usr/lib/solr-5.5.4/server/solr/vAge

Creating new core 'vAge' using command:
http://localhost:8983/solr/admin/cores?action=CREATE&name=vAge&instanceDir=vAge

{
  "responseHeader":{
    "status":0,
    "QTime":577},
  "core":"vAge"}

$ bin/solr create_core -d $JANUSGRAPH_HOME/conf/solr -c eReasonPlace

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

### Run the program

```
$ cd $JANUSGRAPH_HOME/janusgraph-examples/example-hbase

$ mvn exec:java -Dexec.mainClass="org.janusgraph.example.JanusGraphApp" -Dlogback.configurationFile="conf/logback.xml" -Dexec.args="conf/jgex-hbase-solr-http.properties"
```

### Drop the graph

Make sure to stop the application before dropping the graph.

```
$ cd $JANUSGRAPH_HOME/janusgraph-examples/example-hbase

$ mvn exec:java -Dexec.mainClass="org.janusgraph.example.JanusGraphApp" -Dlogback.configurationFile="conf/logback.xml" -Dexec.args="conf/jgex-hbase-solr-http.properties drop"
```

### Drop the Solr cores

The core names match the `vAge` or `eReasonPlace` values when `mgmt.buildIndex()`
defines the mixed indexes in `JanusGraphApp.createMixedIndexes()`

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
