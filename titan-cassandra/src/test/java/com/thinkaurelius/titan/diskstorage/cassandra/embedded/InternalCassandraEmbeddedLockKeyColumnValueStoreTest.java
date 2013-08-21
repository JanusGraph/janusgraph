package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import org.apache.commons.configuration.Configuration;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.testcategory.ByteOrderedPartitionerTests;

@Category({ByteOrderedPartitionerTests.class})
public class InternalCassandraEmbeddedLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest {

    @Override
    public KeyColumnValueStoreManager openStorageManager(int idx) throws StorageException {
        Configuration sc = CassandraStorageSetup.getEmbeddedCassandraStorageConfiguration(getClass().getSimpleName(), true);
        return new CassandraEmbeddedStoreManager(sc);
    }
}
