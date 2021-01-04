# Migrating from Thirft
This page describes some of the Configuration options that JanusGraph
provides to allow migration of data from a data store which had
previously been created by a JanusGraph Thrift backend to a JanusGraph CQL Backend.

!!! note
    The following backends are no longer supported: `cassandrathrift`, `cassandra`, `astyanax`, and `embeddedcassandra`

## Configuration Replacements

| Old configuration | New CQL configuration |
| ------- | ----- |
| .backend=cassandrathrift | .backend=cql |
| .backend=cassandra | .backend=cql |
| .backend=astyanax | .backend=cql |
| .backend=embeddedcassandra | .backend=cql |
| .port=9160 | .port=9042 |
| .cassandra.atomic-batch-mutate | .cql.atomic-batch-mutate |
| .cassandra.compaction-strategy-class | .cql.compaction-strategy-class |
| .cassandra.compaction-strategy-options | .cql.compaction-strategy-options |
| .cassandra.compression | .cql.compression |


## Embedded Mode
JanusGraph does no longer support the embedded mode of Cassandra. 
Therefore, users have to deploy Cassandra as a external instance, 
see [Cassandra](../storage-backend/cassandra.md).

The JanusGraph distribution already comes with a version of Cassandra 
included in the archive `janusgraph-full-{{ latest_version}}.zip`
that is used by the `janusgraph.sh` script.
