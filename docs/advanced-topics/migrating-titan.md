# Migrating from Titan

This page describes some of the Configuration options that JanusGraph
provides to allow migration of data from a data store which had
previously been created by Titan. Please note after migrating to version 0.3.0, 
or later, of JanusGraph you will not be able to connect to a graph using a Titan client. 

## Configuration

When connecting to an existing Titan data store the
`graph.titan-version` property should already be set in the global
configuration to Titan version `1.0.0`. The ID store name in JanusGraph
is configurable via the `ids.store-name` property whereas in Titan it
was a constant. If the `graph.titan-version` has been set in the
existing global configuration, then you do **not** need to explicitly
set the ID store as it will default to `titan_ids`.

## Cassandra

The default keyspace used by Titan was `titan` and in order to reuse
that existing keyspace the `storage.cql.keyspace` property needs
to be set accordingly.
```properties
storage.cql.keyspace=titan
```

These configuration options allow JanusGraph to read data from a
Cassandra database which had previously been created by Titan. However,
once JanusGraph writes back to that database it will register additional
serializers which mean that it will no longer be compatible with Titan.
Users are therefore encouraged to backup the data in Cassandra before
attempting to use it with the JanusGraph release.

## HBase

The name of the table used by Titan was `titan` and in order to reuse
that existing table the `storage.hbase.table` property needs to be set
accordingly.
```properties
storage.hbase.table=titan
```

These configuration options allow JanusGraph to read data from an HBase
database which had previously been created by Titan. However, once
JanusGraph writes back to that database it will register additional
serializers which mean that it will no longer be compatible with Titan.
Users are therefore encouraged to backup the data in HBase before
attempting to use it with the JanusGraph release.

## BerkeleyDB

The BerkeleyDB version has been updated, and it contains changes to the
file format stored on disk. This file format change is forward
compatible with previous versions of BerkeleyDB, so existing graph data
stored with Titan can be read in. However, once the data has been read
in with the newer version of BerkeleyDB, those files can no longer be
read by the older version. Users are encouraged to backup the BerkeleyDB
storage directory before attempting to use it with the JanusGraph
release.
