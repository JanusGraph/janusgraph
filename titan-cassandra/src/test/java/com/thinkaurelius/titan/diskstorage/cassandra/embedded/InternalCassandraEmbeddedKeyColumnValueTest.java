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
import com.thinkaurelius.titan.testcategory.ByteOrderedPartitionerTests;

@Category({ByteOrderedPartitionerTests.class})
public class InternalCassandraEmbeddedKeyColumnValueTest extends AbstractCassandraKeyColumnValueStoreTest {
    
    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.cassandraOrderedYamlPath);
    }
    
    @Override
    public AbstractCassandraStoreManager openStorageManager() throws StorageException {
        Configuration sc = CassandraStorageSetup.getEmbeddedCassandraStorageConfiguration(getClass().getSimpleName(), true);
        return new CassandraEmbeddedStoreManager(sc);
    }

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

    @Test
    public void testGetKeysWithKeyRange() throws Exception {
        super.testGetKeysWithKeyRange();
    }
}
