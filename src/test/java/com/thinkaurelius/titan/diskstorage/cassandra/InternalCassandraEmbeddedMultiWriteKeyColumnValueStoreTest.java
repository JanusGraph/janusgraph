package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;

public class InternalCassandraEmbeddedMultiWriteKeyColumnValueStoreTest extends MultiWriteKeyColumnValueStoreTest {

    @Override
    public StorageManager openStorageManager() {
        return new CassandraThriftStorageManager(StorageSetup.getEmbeddedCassandraStorageConfiguration());
    }
}
