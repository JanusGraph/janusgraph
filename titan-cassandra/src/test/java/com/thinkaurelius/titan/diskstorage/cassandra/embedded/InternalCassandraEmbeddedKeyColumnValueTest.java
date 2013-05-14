package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import static org.junit.Assert.assertTrue;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;

public class InternalCassandraEmbeddedKeyColumnValueTest extends AbstractCassandraKeyColumnValueStoreTest {

    @Override
    public AbstractCassandraStoreManager openStorageManager() throws StorageException {
        return new CassandraEmbeddedStoreManager(getConfiguration());
    }

    private Configuration getConfiguration() {
        Configuration config = CassandraStorageSetup.getEmbeddedCassandraStorageConfiguration(true);
        return config;
    }

//    @Test
//    public void dryRun() {
//        //The testConfiguration test seems to fail when run in the full suite but works independently.
//        //Hence this dryRun to smooth things over.
//    }
//
    @Test
    public void testConfiguration() {
        StoreFeatures features = manager.getFeatures();
        assertTrue(features.isKeyOrdered());
        assertTrue(features.hasLocalKeyPartition());
    }

    @Override
    public void scanTest() {
        // nothing to do here as current test uses ordered partitioner
    }
}
