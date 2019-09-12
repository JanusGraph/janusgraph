// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.cassandra;

import com.google.common.collect.ImmutableMap;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.testutil.FeatureFlag;
import org.janusgraph.testutil.JanusGraphFeature;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractCassandraStoreTest extends KeyColumnValueStoreTest {
    private static final String TEST_CF_NAME = "testcf";
    private static final String DEFAULT_COMPRESSOR_PACKAGE = "org.apache.cassandra.io.compress";

    public abstract ModifiableConfiguration getBaseStorageConfiguration();
    public abstract ModifiableConfiguration getBaseStorageConfiguration(String keyspace);

    public abstract AbstractCassandraStoreManager openStorageManager(Configuration c) throws BackendException;

    @Test
    @FeatureFlag(feature = JanusGraphFeature.UnorderedScan)
    public void testUnorderedConfiguration() {
        StoreFeatures features = manager.getFeatures();
        assertFalse(features.isKeyOrdered());
        assertFalse(features.hasLocalKeyPartition());
    }

    @Test
    @FeatureFlag(feature = JanusGraphFeature.OrderedScan)
    public void testOrderedConfiguration() {
        StoreFeatures features = manager.getFeatures();
        assertTrue(features.isKeyOrdered());
    }

    @Test
    public void testDefaultCFCompressor() throws BackendException {

        final String cf = TEST_CF_NAME + "_snappy";

        AbstractCassandraStoreManager mgr = openStorageManager();

        mgr.openDatabase(cf);

        Map<String, String> defaultCfCompressionOps =
                new ImmutableMap.Builder<String, String>()
                .put("sstable_compression", DEFAULT_COMPRESSOR_PACKAGE + "." + AbstractCassandraStoreManager.CF_COMPRESSION_TYPE.getDefaultValue())
                .put("chunk_length_kb", "64")
                .build();

        assertEquals(defaultCfCompressionOps, mgr.getCompressionOptions(cf));
    }

    @Test
    public void testCustomCFCompressor() throws BackendException {

        final String compressor = "DeflateCompressor";
        final int ckb = 128;
        final String cf = TEST_CF_NAME + "_gzip";

        ModifiableConfiguration config = getBaseStorageConfiguration();
        config.set(AbstractCassandraStoreManager.CF_COMPRESSION_TYPE,compressor);
        config.set(AbstractCassandraStoreManager.CF_COMPRESSION_BLOCK_SIZE,ckb);

        AbstractCassandraStoreManager mgr = openStorageManager(config);

        // N.B.: clearStorage() truncates CFs but does not delete them
        mgr.openDatabase(cf);

        final Map<String, String> expected = ImmutableMap
                .<String, String> builder()
                .put("sstable_compression",
                        DEFAULT_COMPRESSOR_PACKAGE + "." + compressor)
                .put("chunk_length_kb", String.valueOf(ckb)).build();

        assertEquals(expected, mgr.getCompressionOptions(cf));
    }

    @Test
    public void testDisableCFCompressor() throws BackendException {

        final String cf = TEST_CF_NAME + "_nocompress";

        ModifiableConfiguration config = getBaseStorageConfiguration();
        config.set(AbstractCassandraStoreManager.CF_COMPRESSION,false);
        AbstractCassandraStoreManager mgr = openStorageManager(config);

        // N.B.: clearStorage() truncates CFs but does not delete them
        mgr.openDatabase(cf);

        assertEquals(Collections.emptyMap(), mgr.getCompressionOptions(cf));
    }

    @Test
    public void testTTLSupported() {
        StoreFeatures features = manager.getFeatures();
        assertTrue(features.hasCellTTL());
    }

    @Test
    public void keyspaceShouldBeEquivalentToProvidedOne() throws BackendException {
        final ModifiableConfiguration config = getBaseStorageConfiguration("randomNewKeyspace");
        final AbstractCassandraStoreManager mgr = openStorageManager(config);
        assertEquals("randomNewKeyspace", mgr.keySpaceName);
    }

    @Test
    public void keyspaceShouldBeEquivalentToGraphName() throws BackendException {
        final ModifiableConfiguration config = getBaseStorageConfiguration(null);
        config.set(GraphDatabaseConfiguration.GRAPH_NAME, "randomNewGraphName");
        final AbstractCassandraStoreManager mgr = openStorageManager(config);
        assertEquals("randomNewGraphName", mgr.keySpaceName);
    }

    @Override
    public AbstractCassandraStoreManager openStorageManager() throws BackendException {
        return openStorageManager(getBaseStorageConfiguration());
    }

    @Override
    public AbstractCassandraStoreManager openStorageManagerForClearStorageTest() throws Exception {
        return openStorageManager(getBaseStorageConfiguration().set(GraphDatabaseConfiguration.DROP_ON_CLEAR, true));
    }

}
