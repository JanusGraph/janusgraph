package com.thinkaurelius.titan.diskstorage.cassandra;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;


public abstract class AbstractCassandraKeyColumnValueStoreTest extends KeyColumnValueStoreTest {
    private static final String TEST_CF_NAME = "testcf";

    @Override
    public abstract AbstractCassandraStoreManager openStorageManager() throws StorageException;
    
    @Test
    public void testConfiguration() {
        StoreFeatures features = manager.getFeatures();
        assertFalse(features.isKeyOrdered());
        assertFalse(features.hasLocalKeyPartition());
    }
    
    @Test
    public void testColumnFamilyOptions() throws StorageException {
        
        AbstractCassandraStoreManager mgr = openStorageManager();
        
        mgr.openDatabase(TEST_CF_NAME);
        
        Map<String, String> defaultCfCompressionOps =
                new ImmutableMap.Builder<String, String>()
                .put("sstable_compression", "org.apache.cassandra.io.compress.SnappyCompressor")
                .put("chunk_length_kb", "64")
                .build();
        
        assertEquals(defaultCfCompressionOps, mgr.getCompressionOptions(TEST_CF_NAME));
    }
}
