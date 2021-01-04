# Apache Solr

> Solr is highly reliable, scalable and fault tolerant, 
> providing distributed indexing, replication and load-balanced 
> querying, automated failover and recovery, centralized configuration 
> and more. Solr powers the search and navigation features of 
> many of the world's largest internet sites. 
>
> —  [Apache Solr Homepage](https://lucene.apache.org/solr/)

JanusGraph supports [Apache Solr](https://lucene.apache.org/solr/) as an
index backend. Here are some of the Solr features supported by
JanusGraph:

-   **Full-Text**: Supports all `Text` predicates to search for text
    properties that matches a given word, prefix or regular expression.
-   **Geo**: Supports all `Geo` predicates to search for geo properties
    that are intersecting, within, disjoint to or contained in a given
    query geometry. Supports points, lines and polygons for indexing.
    Supports circles, boxes and polygons for querying point properties
    and all shapes for querying non-point properties.
-   **Numeric Range**: Supports all numeric comparisons in `Compare`.
-   **TTL**: Supports automatically expiring indexed elements.
-   **Temporal**: Millisecond granularity temporal indexing.
-   **Custom Analyzer**: Choose to use a custom analyzer

Please see [Version Compatibility](../changelog.md#version-compatibility)
for details on what versions of Solr will work with JanusGraph.

## Solr Configuration Overview

JanusGraph supports Solr running in either a SolrCloud or Solr
Standalone (HTTP) configuration for use with a **mixed index** 
(see [Mixed Index](../schema/index-management/index-performance.md#mixed-index)). 
The desired connection mode is configured via the
parameter `mode` which must be set to either `cloud` or `http`, the
former being the default value. For example, to explicitly specify that
Solr is running in a SolrCloud configuration the following property is
specified as a JanusGraph configuration property:

```properties
index.search.solr.mode=cloud
```

These are some key Solr terms:

-   **Core**: A *single index* on a single machine
-   **Configuration**: *solrconfig.xml*, *schema.xml*, and other files
    required to define a core.
-   **Collection**: A *single logical index* that can span multiple
    cores on different machines.
-   **Configset**: A shared *configuration* that can be reused by
    multiple cores.

## Connecting to SolrCloud

When connecting to a SolrCloud cluster by setting the `mode` equal to
`cloud`, the Zookeeper URL (and optionally port) must be specified so
that JanusGraph can discover and interact with the Solr cluster.

```properties
index.search.backend=solr
index.search.solr.mode=cloud
index.search.solr.zookeeper-url=localhost:2181
```

A number of additional configuration options pertaining to the creation
of new collections (which is only supported in SolrCloud operation mode)
can be configured to control sharding behavior among other things. Refer
to the [Configuration Reference](../configs/configuration-reference.md) for a complete listing of those options.

SolrCloud leverages Zookeeper to coordinate collection and configset
information between the Solr servers. The use of Zookeeper with
SolrCloud provides the opportunity to significantly reduce the amount of
manual configuration required to use Solr as a back end index for
JanusGraph.

### Configset Configuration

A configset is required to create a collection. The configset is stored
in Zookeeper to enable access to it across the Solr servers.

-   Each collection can provide its own configset when it is created, so
    that each collection may have a different configuration. With this
    approach, each collection must be created manually.

-   A shared configset can be uploaded separately to Zookeeper if it
    will be reused by multiple collections. With this approach,
    JanusGraph can create collections automatically by using the shared
    configset. Another benefit is that reusing a configset significantly
    reduces the amount of data stored in Zookeeper.

#### Using an Individual Configset

In this example, a collection named `verticesByAge` is created manually
using the default JanusGraph configuration for Solr that is found in the
distribution. When the collection is created, the configuration is
uploaded into Zookeeper, using the same collection name `verticesByAge`
for the configset name. Refer to the [Solr Reference Guide](https://lucene.apache.org/solr/guide/6_6/solr-control-script-reference.html#SolrControlScriptReference-CollectionsandCores)
for available parameters.

```bash
# create the collection
$SOLR_HOME/bin/solr create -c verticesByAge -d $JANUSGRAPH_HOME/conf/solr
```

Define a mixed index using `JanusGraphManagement` and the same
collection name.

```groovy
mgmt = graph.openManagement()
age = mgmt.makePropertyKey("age").dataType(Integer.class).make()
mgmt.buildIndex("verticesByAge", Vertex.class).addKey(age).buildMixedIndex("search")
mgmt.commit()
```

#### Using a Shared Configset

When using a shared configset, it is most convenient to upload the
configuration first as a one time operation. In this example, a
configset named `janusgraph-configset` is uploaded in to Zookeeper using
the default JanusGraph configuration for Solr that is found in the
distribution. Refer to the [Solr Reference Guide](https://lucene.apache.org/solr/guide/6_6/solr-control-script-reference.html#SolrControlScriptReference-CollectionsandCores)
for available parameters.

```bash
# upload the shared configset into Zookeeper
# Solr 5
$SOLR_HOME/server/scripts/cloud-scripts/zkcli.sh -cmd upconfig -z localhost:2181 \
    -d $JANUSGRAPH_HOME/conf/solr -n janusgraph-configset
# Solr 6 and higher
$SOLR_HOME/bin/solr zk upconfig -d $JANUSGRAPH_HOME/conf/solr -n janusgraph-configset \
    -z localhost:2181
```

When configuring the SolrCloud indexing backend for JanusGraph, make
sure to provide the name of the shared configset using the
`index.search.solr.configset` property.

```properties
index.search.backend=solr
index.search.solr.mode=cloud
index.search.solr.zookeeper-url=localhost:2181
index.search.solr.configset=janusgraph-configset
```

Define a mixed index using `JanusGraphManagement` and the collection
name.

```groovy
mgmt = graph.openManagement()
age = mgmt.makePropertyKey("age").dataType(Integer.class).make()
mgmt.buildIndex("verticesByAge", Vertex.class).addKey(age).buildMixedIndex("search")
mgmt.commit()
```

## Connecting to Solr Standalone (HTTP)

When connecting to Solr Standalone via HTTP by setting the `mode` equal
to `http`, a single or list of URLs for the Solr instances must be
provided.

```properties
index.search.backend=solr
index.search.solr.mode=http
index.search.solr.http-urls=http://localhost:8983/solr
```

Additional configuration options for controlling the maximum number of
connections, connection timeout and transmission compression are
available for the HTTP mode. Refer to the [Configuration Reference](../configs/configuration-reference.md) for a
complete listing of those options.

### Core Configuration

Solr Standalone is used for a single instance, and it keeps
configuration information on the file system. A core must be created
manually for each mixed index.

To create a core, a `core_name` and a `configuration` directory is
required. Refer to the [Solr Reference Guide](https://lucene.apache.org/solr/guide/6_6/solr-control-script-reference.html#SolrControlScriptReference-CollectionsandCores)
for available parameters. In this example, a core named `verticesByAge`
is created using the default JanusGraph configuration for Solr that is
found in the distribution.

```bash
$SOLR_HOME/bin/solr create -c verticesByAge -d $JANUSGRAPH_HOME/conf/solr
```

Define a mixed index using `JanusGraphManagement` and the same core
name.
```groovy
mgmt = graph.openManagement()
age = mgmt.makePropertyKey("age").dataType(Integer.class).make()
mgmt.buildIndex("verticesByAge", Vertex.class).addKey(age).buildMixedIndex("search")
mgmt.commit()
```

## Kerberos Configuration

When connecting to a Solr environment that is protected by Kerberos we must specify that Kerberos is being used and reference a JAAS configuration file to properly configure the Solr Clients. This configuration is required when Kerberos is in use regardless of the mode in which Solr is operating (SolrCloud or Solr Standalone).

```properties
index.search.solr.kerberos-enabled=true
```

The JAAS configuration file is supplied by ensuring that you set the java system property `java.security.auth.login.config` with the absolute path to the file. This property should be set using JVM options. For example to run `gremlin.sh` you would need to set the `JAVA_OPTIONS` environment variable prior to running the script:

```bash
export JAVA_OPTIONS="-Djava.security.auth.login.config=/absolute/path/jaas.conf"
$JANUSGRAPH_HOME/bin/gremlin.sh
```

For details on the content required in the JAAS configuration file refer to the https://lucene.apache.org/solr/guide/7_0/kerberos-authentication-plugin.html#define-a-jaas-configuration-file[Solr Reference Guide].

## Solr Schema Design

### Dynamic Field Definition

By default, JanusGraph uses Solr’s [Dynamic Fields](https://cwiki.apache.org/confluence/display/solr/Dynamic+Fields)
feature to define the field types for all indexed keys. This requires no
extra configuration when adding property keys to a mixed index backed by
Solr and provides better performance than schemaless mode.

JanusGraph assumes the following dynamic field tags are defined in the
backing Solr collection’s schema.xml file. Please note that there is
additional xml definition of the following fields required in a solr
schema.xml file in order to use them. Reference the example schema.xml
file provided in the `./conf/solr/schema.xml` directory in a JanusGraph
installation for more information.

```xml
<dynamicField name="*_i"    type="int"          indexed="true"  stored="true"/>
<dynamicField name="*_s"    type="string"       indexed="true"  stored="true" />
<dynamicField name="*_l"    type="long"         indexed="true"  stored="true"/>
<dynamicField name="*_t"    type="text_general" indexed="true"  stored="true"/>
<dynamicField name="*_b"    type="boolean"      indexed="true" stored="true"/>
<dynamicField name="*_f"    type="float"        indexed="true"  stored="true"/>
<dynamicField name="*_d"    type="double"       indexed="true"  stored="true"/>
<dynamicField name="*_g"    type="geo"          indexed="true"  stored="true"/>
<dynamicField name="*_dt"   type="date"         indexed="true"  stored="true"/>
<dynamicField name="*_uuid" type="uuid"         indexed="true"  stored="true"/>
```

In JanusGraph’s default configuration, property key names do not have to
end with the type-appropriate suffix to take advantage of Solr’s dynamic
field feature. JanusGraph generates the Solr field name from the
property key name by encoding the property key definition’s numeric
identifier and the type-appropriate suffix. This means that JanusGraph
uses synthetic field names with type-appropriate suffixes behind the
scenes, regardless of the property key names defined and used by
application code using JanusGraph. This field name mapping can be
overridden through non-default configuration. That’s described in the
next section.

### Manual Field Definition

If the user would rather manually define the field types for each of the
indexed fields in a collection, the configuration option `dyn-fields`
needs to be disabled. It is important that the field for each indexed
property key is defined in the backing Solr schema before the property
key is added to the index.

In this scenario, it is advisable to enable explicit property key name
to field mapping in order to fix the field names for their explicit
definition. This can be achieved in one of two ways:

1.  Configuring the name of the field by providing a `mapped-name`
    parameter when adding the property key to the index. See
    [Individual Field Mapping](field-mapping.md#individual-field-mapping) for more information.
2.  By enabling the `map-name` configuration option for the Solr index
    which will use the property key name as the field name in Solr. See
    [Global Field Mapping](field-mapping.md#global-field-mapping) for more information.

### Schemaless Mode

JanusGraph can also interact with a SolrCloud cluster that is configured
for [schemaless mode](https://cwiki.apache.org/confluence/display/solr/Schemaless+Mode).
In this scenario, the configuration option `dyn-fields` should be
disabled since Solr will infer the field type from the values and not
the field name.

Note, however, that schemaless mode is recommended only for prototyping
and initial application development and NOT recommended for production
use.

## Troubleshooting

### Collection Does Not Exist

The collection (and all of the required configuration files) must be
initialized before a defined index can use the collection. See
[Connecting to SolrCloud](#_connecting_to_solrcloud) for more
information.

When using SolrCloud, the Zookeeper zkCli.sh command line tool can be
used to inspect the configurations loaded into Zookeeper. Also verify
that the default JanusGraph configuration files are copied to the
correct location under solr and that the directory where the files are
copied is correct.

### Cannot Find the Specified Configset

When using SolrCloud, a configset is required to create a mixed index
for JanusGraph. See [Configset Configuration](#configset_configuration) for more
information.

-   If using an individual configset, the collection must be created
    manually first.
-   If using a shared configset, the configset must be uploaded into
    Zookeeper first.

You can verify that the configset and its configuration files are in
Zookeeper under `/configs`. Refer to the [Solr Reference Guide](https://lucene.apache.org/solr/guide/6_6/solr-control-script-reference.html#SolrControlScriptReference-ZooKeeperOperations)
for other Zookeeper operations.

```bash
# verify the configset in Zookeeper
# Solr 5
$SOLR_HOME/server/scripts/cloud-scripts/zkcli.sh -cmd list -z localhost:2181
# Solr 6 and higher
$SOLR_HOME/bin/solr zk ls -r /configs/configset-name -z localhost:2181
```

### HTTP Error 404

This error may be encountered when using Solr Standalone (HTTP) mode. An
example of the error:

```xml
20:01:22 ERROR org.janusgraph.diskstorage.solr.SolrIndex  - Unable to save documents
to Solr as one of the shape objects stored were not compatible with Solr.
org.apache.solr.client.solrj.impl.HttpSolrClient$RemoteSolrException: Error from server
at http://localhost:8983/solr: Expected mime type application/octet-stream but got text/html.
<html>
<head>
<meta http-equiv="Content-Type" content="text/html;charset=utf-8"/>
<title>Error 404 Not Found</title>
</head>
<body><h2>HTTP ERROR 404</h2>
<p>Problem accessing /solr/verticesByAge/update. Reason:
<pre>    Not Found</pre></p>
</body>
</html>
```

Make sure to create the core manually before attempting to store data
into the index. See [Core Configuration](#core-configuration)
for more information.

### Invalid core or collection name

The core or collection name is an identifier. It must consist entirely
of periods, underscores, hyphens, and/or alphanumerics, and also it may
not start with a hyphen.

### Connection Problems

Irrespective of the operation mode, a Solr instance or a cluster of Solr
instances must be running and accessible from the JanusGraph instance(s)
in order for JanusGraph to use Solr as an indexing backend. Check that
the Solr cluster is running correctly and that it is visible and
accessible over the network (or locally) from the JanusGraph instances.

### JTS ClassNotFoundException with Geo Data

Solr relies on Spatial4j for geo processing. Spatial4j declares an
optional dependency on JTS ("JTS Topology Suite"). JTS is required for
some geo field definition and query functionality. If the JTS jar is not
on the Solr daemon’s classpath and a field in schema.xml uses a geo
type, then Solr may throw a ClassNotFoundException on one of the missing
JTS classes. The exception can appear when starting Solr using a
schema.xml file designed to work with JanusGraph, but can also appear
when invoking `CREATE` in the [Solr CoreAdmin API](https://wiki.apache.org/solr/CoreAdmin). The exception appears in
slightly different formats on the client and server sides, although the
root cause is identical.

Here’s a representative example from a Solr server log:

```java
ERROR [http-8983-exec-5] 2014-10-07 02:54:06, 665 SolrCoreResourceManager.java (line 344) com/vividsolutions/jts/geom/Geometry
java.lang.NoClassDefFoundError: com/vividsolutions/jts/geom/Geometry
    at com.spatial4j.core.context.jts.JtsSpatialContextFactory.newSpatialContext(JtsSpatialContextFactory.java:30)
    at com.spatial4j.core.context.SpatialContextFactory.makeSpatialContext(SpatialContextFactory.java:83)
    at org.apache.solr.schema.AbstractSpatialFieldType.init(AbstractSpatialFieldType.java:95)
    at org.apache.solr.schema.AbstractSpatialPrefixTreeFieldType.init(AbstractSpatialPrefixTreeFieldType.java:43)
    at org.apache.solr.schema.SpatialRecursivePrefixTreeFieldType.init(SpatialRecursivePrefixTreeFieldType.java:37)
    at org.apache.solr.schema.FieldType.setArgs(FieldType.java:164)
    at org.apache.solr.schema.FieldTypePluginLoader.init(FieldTypePluginLoader.java:141)
    at org.apache.solr.schema.FieldTypePluginLoader.init(FieldTypePluginLoader.java:43)
    at org.apache.solr.util.plugin.AbstractPluginLoader.load(AbstractPluginLoader.java:190)
    at org.apache.solr.schema.IndexSchema.readSchema(IndexSchema.java:470)
    at com.datastax.bdp.search.solr.CassandraIndexSchema.readSchema(CassandraIndexSchema.java:72)
    at org.apache.solr.schema.IndexSchema.<init>(IndexSchema.java:168)
    at com.datastax.bdp.search.solr.CassandraIndexSchema.<init>(CassandraIndexSchema.java:54)
    at com.datastax.bdp.search.solr.core.CassandraCoreContainer.create(CassandraCoreContainer.java:210)
    at com.datastax.bdp.search.solr.core.SolrCoreResourceManager.createCore(SolrCoreResourceManager.java:256)
    at com.datastax.bdp.search.solr.handler.admin.CassandraCoreAdminHandler.handleCreateAction(CassandraCoreAdminHandler.java:117)
    ...
```

Here’s what normally appears in the output of the client that issued the
associated `CREATE` command to the CoreAdmin API:

```java
org.apache.solr.common.SolrException: com/vividsolutions/jts/geom/Geometry
    at com.datastax.bdp.search.solr.core.SolrCoreResourceManager.createCore(SolrCoreResourceManager.java:345)
    at com.datastax.bdp.search.solr.handler.admin.CassandraCoreAdminHandler.handleCreateAction(CassandraCoreAdminHandler.java:117)
    at org.apache.solr.handler.admin.CoreAdminHandler.handleRequestBody(CoreAdminHandler.java:152)
    ...
```

This is resolved by adding the JTS jar to the classpath of JanusGraph
and/or the Solr server. JTS is not included in JanusGraph distributions
by default due to its LGPL license. Users must download the [JTS jar file](https://search.maven.org/remotecontent?filepath=com/vividsolutions/jts/1.13/jts-1.13.jar)
separately and copy it into the JanusGraph and/or Solr server lib
directory. If using Solr’s built in web server, the JTS jar may be
copied to the example/solr-webapp/webapp/WEB-INF/lib directory to
include it in the classpath. Solr can be restarted, and the exception
should be gone. Solr must be started once with the correct schema.xml
file in place first, for the example/solr-webapp/webapp/WEB-INF/lib
directory to exist.

To determine the ideal JTS version for Solr server, first check the
version of Spatial4j in use by the Solr cluster, then determine the
version of JTS against which that Spatial4j version was compiled.
Spatial4j declares its target JTS version in the [pom for the `com.spatial4j:spatial4j` artifact](https://search.maven.org/#search|gav|1|g%3A%22com.spatial4j%22%20AND%20a%3A%22spatial4j%22).
Copy the JTS jar to the server/solr-webapp/webapp/WEB-INF/lib directory
in your solr installation.

## Advanced Solr Configuration

### DSE Search

This section covers installation and configuration of JanusGraph with
DataStax Enterprise (DSE) Search. There are multiple ways to install
DSE, but this section focuses on DSE’s binary tarball install option on
Linux. Most of the steps in this section can be generalized to the other
install options for DSE.

Install DataStax Enterprise as directed by the page [Installing DataStax Enterprise using the binary tarball](https://www.datastax.com/documentation/datastax_enterprise/4.5/datastax_enterprise/install/installTARdse.html).

Export `DSE_HOME` and append to `PATH` in your shell environment. Here’s
an example using Bash syntax:
```bash
export DSE_HOME=/path/to/dse-version.number
export PATH="$DSE_HOME"/bin:"$PATH"
```

Install JTS for Solr. The appropriate version varies with the Spatial4j
version. As of DSE 4.5.2, the appropriate version is 1.13.
```bash
cd $DSE_HOME/resources/solr/lib
curl -O 'http://central.maven.org/maven2/com/vividsolutions/jts/1.13/jts-1.13.jar'
```

Start DSE Cassandra and Solr in a single background daemon:

```bash
# The "dse-data" path below was chosen to match the
# "Installing DataStax Enterprise using the binary tarball"
# documentation page from DataStax.  The exact path is not
# significant.
dse cassandra -s -Ddse.solr.data.dir="$DSE_HOME"/dse-data/solr
```

The previous command will write some startup information to the console
and to the logfile path `log4j.appender.R.File` configured in
`$DSE_HOME/resources/cassandra/conf/log4j-server.properties`.

Once DSE with Cassandra and Solr has started normally, check the cluster
health with `nodetool status`. A single-instance ring should show one
node with flags \*U\*p and \*N\*ormal:

```bash
nodetool status
Note: Ownership information does not include topology; for complete information, specify a keyspace
= Datacenter: Solr
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address    Load       Owns   Host ID                               Token                                    Rack
UN  127.0.0.1  99.89 KB   100.0%  5484ef7b-ebce-4560-80f0-cbdcd9e9f496  -7317038863489909889                     rack1
```

Next, switch to Gremlin Console and open a JanusGraph database against
the DSE instance. This will create JanusGraph’s keyspace and column
families.

```bash
cd $JANUSGRAPH_HOME
bin/gremlin.sh

         \,,,/
         (o o)
-----oOOo-(3)-oOOo-----
gremlin> graph = JanusGraphFactory.open('conf/janusgraph-cql-solr.properties')
==>janusgraph[cql:[127.0.0.1]]
gremlin> g = graph.traversal()
==>graphtraversalsource[janusgraph[cql:[127.0.0.1]], standard]
gremlin>
```

Keep this Gremlin Console open. We’ll take a break now to install a Solr
core. Then we’ll come back to this console to load some sample data.

Next, upload configuration files for JanusGraph’s Solr collection, then
create the core in DSE:

```bash
# Change to the directory where JanusGraph was extracted.  Later commands
# use relative paths to the Solr config files shipped with the JanusGraph
# distribution.
cd $JANUSGRAPH_HOME

# The name must be URL safe and should contain one dot/full-stop
# character. The part of the name after the dot must not conflict with
# any of JanusGraph's internal CF names.  Starting the part after the dot
# "solr" will avoid a conflict with JanusGraph's internal CF names.
CORE_NAME=janusgraph.solr1
# Where to upload collection configuration and send CoreAdmin requests.
SOLR_HOST=localhost:8983

# The value of index.[X].solr.http-urls in JanusGraph's config file
# should match $SOLR_HOST and $CORE_NAME.  For example, given the
# $CORE_NAME and $SOLR_HOST values above, JanusGraph's config file would
# contain (assuming "search" is the desired index alias):
#
# index.search.solr.http-urls=http://localhost:8983/solr/janusgraph.solr1
#
# The stock JanusGraph config file conf/janusgraph-cql-solr.properties
# ships with this http-urls value.

# Upload Solr config files to DSE Search daemon
for xml in conf/solr/{solrconfig, schema, elevate}.xml ; do
    curl -v http://"$SOLR_HOST"/solr/resource/"$CORE_NAME/$xml" \
      --data-binary @"$xml" -H 'Content-type:text/xml; charset=utf-8'
done
for txt in conf/solr/{protwords, stopwords, synonyms}.txt ; do
    curl -v http://"$SOLR_HOST"/solr/resource/"$CORE_NAME/$txt" \
      --data-binary @"$txt" -H 'Content-type:text/plain; charset=utf-8'
done
sleep 5

# Create core using the Solr config files just uploaded above
curl "http://"$SOLR_HOST"/solr/admin/cores?action=CREATE&name=$CORE_NAME"
sleep 5

# Retrieve and print the status of the core we just created
curl "http://localhost:8983/solr/admin/cores?action=STATUS&core=$CORE_NAME"
```

Now the JanusGraph database and backing Solr core are ready for use. We
can test it out with the [Graph of the Gods](../getting-started/basic-usage.md) dataset.
Picking up the Gremlin Console session started above:
```groovy
// Assuming graph = JanusGraphFactory.open('conf/janusgraph-cql-solr.properties')...
gremlin> GraphOfTheGodsFactory.load(graph)
==>null
```

Now we can run any of the queries described in [Getting started](../getting-started/basic-usage.md).
Queries involving text and geo predicates will be served by Solr. For
more verbose reporting from JanusGraph and the Solr client, run
`gremlin.sh -l DEBUG` and issue some index-backed queries.
