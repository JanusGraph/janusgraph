# Example Graph Configuration

This page illustrates a number of common graph configurations. Please
refer to [Configuration Reference](configuration-reference.md) and the pages of the respective [storage backend](../storage-backend/index.md), [index backend](../index-backend/index.md) for more
information.

Also, note that the JanusGraph distribution includes local configuration
files in the `conf/` directory.

## BerkeleyDB
```properties
storage.backend=berkeleyje
storage.directory=/tmp/graph

index.search.backend=elasticsearch
index.search.directory=/tmp/searchindex
index.search.elasticsearch.client-only=false
index.search.elasticsearch.local-mode=true
```

This configuration file configures JanusGraph to use [BerkeleyDB](../storage-backend/bdb.md)
as an embedded storage backend, meaning, JanusGraph will start
BerkeleyDB internally. The primary data will be stored in the directory
`/tmp/graph`.

In addition, this configures an embedded [Elasticsearch](../index-backend/elasticsearch.md)
index backend with the name `search`. JanusGraph will start
Elasticsearch internally and it will not be externally accessible since
`local-mode` is enabled. Elasticsearch stores all data for the `search`
index in `/tmp/searchindex`. Configuring an index backend is optional.

## Cassandra

### Cassandra Remote
```properties
storage.backend=cql
storage.hostname=100.100.100.1, 100.100.100.2

index.search.backend=elasticsearch
index.search.hostname=100.100.101.1, 100.100.101.2
index.search.elasticsearch.client-only=true
```

This configuration file configures JanusGraph to use
[Cassandra](../storage-backend/cassandra.md) as a remote storage backend. It assumes that a
Cassandra cluster is running and accessible at the given IP addresses.
If Cassandra is running locally, use the IP address `127.0.0.1`.

In addition, this configures a remote [Elasticsearch](../index-backend/elasticsearch.md)
index backend with the name `search`. It assumes that an Elasticsearch
cluster is running and accessible at the given IP addresses. Enabling
`client-only` ensures that the local instance does not join the existing
Elasticsearch cluster as another node but only connects to it.
Configuring an index backend is optional.

## HBase
```properties
storage.backend=hbase
storage.hostname=127.0.0.1
storage.port=2181

index.search.backend=elasticsearch
index.search.hostname=127.0.0.1
index.search.elasticsearch.client-only=true
```

This configuration file configures JanusGraph to use [HBase](../storage-backend/hbase.md) as
a remote storage backend. It assumes that an HBase cluster is running
and accessible at the given IP addresses through the configured port. If
HBase is running locally, use the IP address `127.0.0.1`.

The optional index backend configuration is identical to remote index
configuration described above.
