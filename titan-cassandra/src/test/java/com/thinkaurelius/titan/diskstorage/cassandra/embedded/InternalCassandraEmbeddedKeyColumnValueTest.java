package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import static org.junit.Assert.assertTrue;

import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.testcategory.OrderedKeyStoreTests;

public class InternalCassandraEmbeddedKeyColumnValueTest extends AbstractCassandraKeyColumnValueStoreTest {
    
    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    @Override
    public Configuration getBaseStorageConfiguration() {
        return CassandraStorageSetup.getEmbeddedCassandraStorageConfiguration(getClass().getSimpleName());
    }
    
    @Override
    public AbstractCassandraStoreManager openStorageManager(Configuration c) throws StorageException {
        return new CassandraEmbeddedStoreManager(c);
    }

    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testConfiguration() {
        StoreFeatures features = manager.getFeatures();
        assertTrue(features.isKeyOrdered());
        assertTrue(features.hasLocalKeyPartition());
    }
}
