package com.thinkaurelius.titan.diskstorage.cassandra;

public interface CLevelInterface {

    public org.apache.cassandra.db.ConsistencyLevel     getDB();

    public org.apache.cassandra.thrift.ConsistencyLevel getThrift();

    public com.netflix.astyanax.model.ConsistencyLevel  getAstyanax();
}
