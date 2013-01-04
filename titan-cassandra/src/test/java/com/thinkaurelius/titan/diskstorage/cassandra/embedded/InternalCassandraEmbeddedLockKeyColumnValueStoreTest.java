package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;

public class InternalCassandraEmbeddedLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest {

    @Override
    public KeyColumnValueStoreManager openStorageManager(int idx) throws StorageException {
        Configuration sc = CassandraStorageSetup.getEmbeddedCassandraStorageConfiguration(true);
        return new CassandraEmbeddedStoreManager(sc);
    }
}
