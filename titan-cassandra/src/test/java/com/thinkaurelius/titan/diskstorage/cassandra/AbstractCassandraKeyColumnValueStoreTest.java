package com.thinkaurelius.titan.diskstorage.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.testcategory.OrderedKeyStoreTests;
import com.thinkaurelius.titan.testcategory.UnorderedKeyStoreTests;

public abstract class AbstractCassandraKeyColumnValueStoreTest extends KeyColumnValueStoreTest {
    
    private static final Logger log =
            LoggerFactory.getLogger(AbstractCassandraKeyColumnValueStoreTest.class);
    private static final String TEST_CF_NAME = "testcf";
    private static final String DEFAULT_COMPRESSOR_PACKAGE = "org.apache.cassandra.io.compress";

    public abstract Configuration getBaseStorageConfiguration();
    
    public abstract AbstractCassandraStoreManager openStorageManager(Configuration c) throws StorageException;
    
    @Test
    @Category({ UnorderedKeyStoreTests.class })
    public void testUnorderedConfiguration() {
        if (!manager.getFeatures().supportsUnorderedScan()) {
            log.warn(
                "Can't test key-unordered features on incompatible store.  "
                + "This warning could indicate reduced test coverage and "
                + "a broken JUnit configuration.  Skipping test {}.",
                name.getMethodName());
            return;
        }
        
        StoreFeatures features = manager.getFeatures();
        assertFalse(features.isKeyOrdered());
        assertFalse(features.hasLocalKeyPartition());
    }
 
    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testOrderedConfiguration() {
        if (!manager.getFeatures().supportsOrderedScan()) {
            log.warn(
                "Can't test key-ordered features on incompatible store.  "
                + "This warning could indicate reduced test coverage and "
                + "a broken JUnit configuration.  Skipping test {}.",
                name.getMethodName());
            return;
        }
        
        StoreFeatures features = manager.getFeatures();
        assertTrue(features.isKeyOrdered());
    }
 
    @Test
    public void testDefaultCFCompressor() throws StorageException {
        
        final String cf = TEST_CF_NAME + "_snappy";
        
        AbstractCassandraStoreManager mgr = openStorageManager();
        
        mgr.openDatabase(cf);
        
        Map<String, String> defaultCfCompressionOps =
                new ImmutableMap.Builder<String, String>()
                .put("sstable_compression", DEFAULT_COMPRESSOR_PACKAGE + ".SnappyCompressor")
                .put("chunk_length_kb", "64")
                .build();
        
        assertEquals(defaultCfCompressionOps, mgr.getCompressionOptions(cf));
    }
    
    @Test
    public void testCustomCFCompressor() throws StorageException {
        
        final String cname = "DeflateCompressor";
        final String ckb = "128";
        final String cf = TEST_CF_NAME + "_gzip";
        
        Configuration gzipCompressorCfg = new BaseConfiguration();
        gzipCompressorCfg.setProperty(AbstractCassandraStoreManager.COMPRESSION_KEY, cname);
        gzipCompressorCfg.setProperty(AbstractCassandraStoreManager.COMPRESSION_CHUNKS_SIZE_KEY, ckb);
        gzipCompressorCfg = new CompositeConfiguration(Arrays.asList(gzipCompressorCfg, getBaseStorageConfiguration()));
        
        AbstractCassandraStoreManager mgr = openStorageManager(gzipCompressorCfg);
        
        // N.B.: clearStorage() truncates CFs but does not delete them
        mgr.openDatabase(cf);

        final Map<String, String> expected = ImmutableMap
                .<String, String> builder()
                .put("sstable_compression",
                        DEFAULT_COMPRESSOR_PACKAGE + "." + cname)
                .put("chunk_length_kb", ckb).build();
        
        assertEquals(expected, mgr.getCompressionOptions(cf));
    }
    
    @Test
    public void testDisableCFCompressor() throws StorageException {
        
        final String cf = TEST_CF_NAME + "_nocompress";
        
        Configuration noCompressorCfg = new BaseConfiguration();
        noCompressorCfg.setProperty(AbstractCassandraStoreManager.ENABLE_COMPRESSION_KEY, "false");
        noCompressorCfg = new CompositeConfiguration(Arrays.asList(noCompressorCfg, getBaseStorageConfiguration()));
        
        AbstractCassandraStoreManager mgr = openStorageManager(noCompressorCfg);
        
        // N.B.: clearStorage() truncates CFs but does not delete them
        mgr.openDatabase(cf);

        assertEquals(Collections.emptyMap(), mgr.getCompressionOptions(cf));
    }

    @Override
    public AbstractCassandraStoreManager openStorageManager() throws StorageException {
        return openStorageManager(getBaseStorageConfiguration());
    }
}
