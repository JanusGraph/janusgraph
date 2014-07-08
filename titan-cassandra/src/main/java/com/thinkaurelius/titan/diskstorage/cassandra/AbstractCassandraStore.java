package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;

public abstract class AbstractCassandraStore implements KeyColumnValueStore {
    protected final AbstractCassandraStoreManager storeManager;
    protected final String keyspace;
    protected final String columnFamilyName;
    protected final int ttlInSecods;

    protected AbstractCassandraStore(String keyspace,
                                     String columnFamily,
                                     AbstractCassandraStoreManager manager,
                                     int ttlInSeconds) {
        this.keyspace = keyspace;
        this.columnFamilyName = columnFamily;
        this.storeManager = manager;
        this.ttlInSecods = ttlInSeconds;
    }

    @Override
    public String getName() {
        return columnFamilyName;
    }

    public int getTTL() {
        return ttlInSecods;
    }
}
