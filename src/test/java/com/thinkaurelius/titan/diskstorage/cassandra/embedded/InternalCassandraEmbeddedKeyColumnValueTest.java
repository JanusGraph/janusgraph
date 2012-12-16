package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class InternalCassandraEmbeddedKeyColumnValueTest extends KeyColumnValueStoreTest {

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new CassandraEmbeddedStoreManager(getConfiguration());
    }

    private Configuration getConfiguration() {
        Configuration config = StorageSetup.getEmbeddedCassandraStorageConfiguration();
        return config;
    }

//    @Test
//    public void dryRun() {
//        //The testConfiguration test seems to fail when run in the full suite but works independently.
//        //Hence this dryRun to smooth things over.
//    }
//
//    @Test
//    public void testConfiguration() {
//        StoreFeatures features = manager.getFeatures();
//        assertTrue(features.isKeyOrdered());
//        assertTrue(features.hasLocalKeyPartition());
//    }


}
