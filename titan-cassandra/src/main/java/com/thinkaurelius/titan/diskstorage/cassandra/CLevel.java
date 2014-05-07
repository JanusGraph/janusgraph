package com.thinkaurelius.titan.diskstorage.cassandra;

import com.google.common.base.Preconditions;

/**
 * This enum unites different libraries' consistency level enums, streamlining
 * configuration and processing in {@link AbstractCassandraStoreManager}.
 *
 */
public enum CLevel implements CLevelInterface { // One ring to rule them all
    ANY,
    ONE,
    TWO,
    THREE,
    QUORUM,
    ALL,
    LOCAL_QUORUM,
    EACH_QUORUM;

    private final org.apache.cassandra.db.ConsistencyLevel db;
    private final org.apache.cassandra.thrift.ConsistencyLevel thrift;
    private final com.netflix.astyanax.model.ConsistencyLevel astyanax;

    private CLevel() {
        db = org.apache.cassandra.db.ConsistencyLevel.valueOf(toString());
        thrift = org.apache.cassandra.thrift.ConsistencyLevel.valueOf(toString());
        astyanax = com.netflix.astyanax.model.ConsistencyLevel.valueOf("CL_" + toString());
    }

    @Override
    public org.apache.cassandra.db.ConsistencyLevel getDB() {
        return db;
    }

    @Override
    public org.apache.cassandra.thrift.ConsistencyLevel getThrift() {
        return thrift;
    }

    @Override
    public com.netflix.astyanax.model.ConsistencyLevel getAstyanax() {
        return astyanax;
    }

    public static CLevel parse(String value) {
        Preconditions.checkArgument(value != null && !value.isEmpty());
        value = value.trim();
        if (value.equals("1")) return ONE;
        else if (value.equals("2")) return TWO;
        else if (value.equals("3")) return THREE;
        else {
            for (CLevel c : values()) {
                if (c.toString().equalsIgnoreCase(value) ||
                    ("CL_" + c.toString()).equalsIgnoreCase(value)) return c;
            }
        }
        throw new IllegalArgumentException("Unrecognized cassandra consistency level: " + value);
    }
}
