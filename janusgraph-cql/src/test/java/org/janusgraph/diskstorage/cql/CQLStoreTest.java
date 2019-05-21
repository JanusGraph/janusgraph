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

package org.janusgraph.diskstorage.cql;

import com.datastax.driver.core.*;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.testcategory.OrderedKeyStoreTests;
import org.janusgraph.testcategory.UnorderedKeyStoreTests;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.*;
import static org.janusgraph.diskstorage.cql.CassandraStorageSetup.getCQLConfiguration;
import static org.janusgraph.diskstorage.cql.CassandraStorageSetup.startCleanEmbedded;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CQLStoreTest extends KeyColumnValueStoreTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CQLStoreTest.class);

    private static final String TEST_CF_NAME = "testcf";
    private static final String DEFAULT_COMPRESSOR_PACKAGE = "org.apache.cassandra.io.compress";
    private static final String TEST_KEYSPACE_NAME = "test_keyspace";

    public CQLStoreTest() throws BackendException {
    }

    @BeforeClass
    public static void startCassandra() {
        startCleanEmbedded();
    }

    protected ModifiableConfiguration getBaseStorageConfiguration() {
        return getCQLConfiguration(getClass().getSimpleName());
    }

    private CQLStoreManager openStorageManager(final Configuration c) throws BackendException {
        return new CachingCQLStoreManager(c);
    }

    @Override
    public CQLStoreManager openStorageManager() throws BackendException {
        return openStorageManager(getBaseStorageConfiguration());
    }

    @Test
    @Category({ UnorderedKeyStoreTests.class })
    public void testUnorderedConfiguration() {
        if (!this.manager.getFeatures().hasUnorderedScan()) {
            LOGGER.warn(
                    "Can't test key-unordered features on incompatible store.  "
                            + "This warning could indicate reduced test coverage and "
                            + "a broken JUnit configuration.  Skipping test {}.",
                            this.name.getMethodName());
            return;
        }

        final StoreFeatures features = this.manager.getFeatures();
        assertFalse(features.isKeyOrdered());
        assertFalse(features.hasLocalKeyPartition());
    }

    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testOrderedConfiguration() {
        if (!this.manager.getFeatures().hasOrderedScan()) {
            LOGGER.warn(
                    "Can't test key-ordered features on incompatible store.  "
                            + "This warning could indicate reduced test coverage and "
                            + "a broken JUnit configuration.  Skipping test {}.",
                            this.name.getMethodName());
            return;
        }

        final StoreFeatures features = this.manager.getFeatures();
        assertTrue(features.isKeyOrdered());
    }

    @Test
    public void testDefaultCompactStorage() throws BackendException {
        final String cf = TEST_CF_NAME + "_defaultcompact";

        final CQLStoreManager cqlStoreManager = openStorageManager();
        cqlStoreManager.openDatabase(cf);

        // COMPACT STORAGE is allowed on Cassandra 2 or earlier
        // when COMPACT STORAGE is allowed, the default is to enable it
        assertTrue(cqlStoreManager.isCompactStorageAllowed() == cqlStoreManager.getTableMetadata(cf).getOptions().isCompactStorage());
    }

    @Test
    public void testUseCompactStorage() throws BackendException {
        final String cf = TEST_CF_NAME + "_usecompact";
        final ModifiableConfiguration config = getBaseStorageConfiguration();
        config.set(CF_COMPACT_STORAGE, true);

        final CQLStoreManager cqlStoreManager = openStorageManager(config);
        cqlStoreManager.openDatabase(cf);

        if (cqlStoreManager.isCompactStorageAllowed()) {
            assertTrue(cqlStoreManager.getTableMetadata(cf).getOptions().isCompactStorage());
        } else {
            assertFalse(cqlStoreManager.getTableMetadata(cf).getOptions().isCompactStorage());
        }
    }

    @Test
    public void testNoCompactStorage() throws BackendException {
        final String cf = TEST_CF_NAME + "_nocompact";
        final ModifiableConfiguration config = getBaseStorageConfiguration();
        config.set(CF_COMPACT_STORAGE, false);

        final CQLStoreManager cqlStoreManager = openStorageManager(config);
        cqlStoreManager.openDatabase(cf);

        assertFalse(cqlStoreManager.getTableMetadata(cf).getOptions().isCompactStorage());
    }

    @Test
    public void testDefaultCFCompressor() throws BackendException {
        final String cf = TEST_CF_NAME + "_snappy";

        final CQLStoreManager cqlStoreManager = openStorageManager();
        cqlStoreManager.openDatabase(cf);

        final Map<String, String> opts = cqlStoreManager.getCompressionOptions(cf);
        assertEquals(2, opts.size());
        // chunk length key differs between 2.x (chunk_length_kb) and 3.x (chunk_length_in_kb)
        assertEquals("64", opts.getOrDefault("chunk_length_kb", opts.get("chunk_length_in_kb")));
        // compression class key differs between 2.x (sstable_compression) and 3.x (class)
        assertEquals(DEFAULT_COMPRESSOR_PACKAGE + "." + CF_COMPRESSION_TYPE.getDefaultValue(), opts.getOrDefault("sstable_compression", opts.get("class")));
    }

    @Test
    public void testCustomCFCompressor() throws BackendException {
        final String cname = "DeflateCompressor";
        final int ckb = 128;
        final String cf = TEST_CF_NAME + "_gzip";

        final ModifiableConfiguration config = getBaseStorageConfiguration();
        config.set(CF_COMPRESSION_TYPE, cname);
        config.set(CF_COMPRESSION_BLOCK_SIZE, ckb);

        final CQLStoreManager mgr = openStorageManager(config);

        // N.B.: clearStorage() truncates CFs but does not delete them
        mgr.openDatabase(cf);

        final Map<String, String> opts = mgr.getCompressionOptions(cf);
        assertEquals(2, opts.size());
        // chunk length key differs between 2.x (chunk_length_kb) and 3.x (chunk_length_in_kb)
        assertEquals(String.valueOf(ckb), opts.getOrDefault("chunk_length_kb", opts.get("chunk_length_in_kb")));
        // compression class key differs between 2.x (sstable_compression) and 3.x (class)
        assertEquals(DEFAULT_COMPRESSOR_PACKAGE + "." + cname, opts.getOrDefault("sstable_compression", opts.get("class")));
    }

    @Test
    public void testDisableCFCompressor() throws BackendException {
        final String cf = TEST_CF_NAME + "_nocompress";

        final ModifiableConfiguration config = getBaseStorageConfiguration();
        config.set(CF_COMPRESSION, false);
        final CQLStoreManager mgr = openStorageManager(config);

        // N.B.: clearStorage() truncates CFs but does not delete them
        mgr.openDatabase(cf);

        final Map<String, String> opts = new HashMap<>(mgr.getCompressionOptions(cf));
        if ("false".equals(opts.get("enabled"))) {
            // Cassandra 3.x contains {"enabled": false"} mapping not found in 2.x
            opts.remove("enabled");
        }
        assertEquals(Collections.emptyMap(), opts);
    }

    @Test
    public void testTTLSupported() {
        final StoreFeatures features = this.manager.getFeatures();
        assertTrue(features.hasCellTTL());
    }

    @Mock
    private Cluster cluster;
    @Mock
    private Session session;

    @InjectMocks
    private CQLStoreManager mockManager = new CQLStoreManager(getBaseStorageConfiguration());

    @Test
    public void testExistKeyspaceSession() {
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(cluster.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace(TEST_KEYSPACE_NAME)).thenReturn(keyspaceMetadata);
        when(cluster.connect()).thenReturn(session);

        mockManager.initializeSession(TEST_KEYSPACE_NAME);

        verify(cluster).connect();
        verify(session, never()).execute(any(Statement.class));
    }

    @Test
    public void testNewKeyspaceSession() {
        Metadata metadata = mock(Metadata.class);
        when(cluster.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace(TEST_KEYSPACE_NAME)).thenReturn(null);
        when(cluster.connect()).thenReturn(session);

        mockManager.initializeSession(TEST_KEYSPACE_NAME);

        verify(cluster).connect();
        verify(session, times(1)).execute(any(Statement.class));
    }

    @Test
    public void testExistTableOpenDatabase() throws BackendException {
        //arrange
        String someTableName = "foo";
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getTable(someTableName)).thenReturn(mock(TableMetadata.class));
        when(cluster.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace(mockManager.getKeyspaceName())).thenReturn(keyspaceMetadata);

        //act
        mockManager.openDatabase(someTableName, null);

        //assert
        verify(session, never()).execute(any(Statement.class));
    }

    @Test
    public void testNewTableOpenDatabase() throws BackendException {
        //arrange
        String someTableName = "foo";
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getTable(someTableName)).thenReturn(null);
        when(cluster.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace(mockManager.getKeyspaceName())).thenReturn(keyspaceMetadata);

        //act
        mockManager.openDatabase(someTableName, null);

        //assert
        verify(session, times(1)).execute(any(Statement.class));
    }

    @Override
    public CQLStoreManager openStorageManagerForClearStorageTest() throws Exception {
        return openStorageManager(getBaseStorageConfiguration().set(GraphDatabaseConfiguration.DROP_ON_CLEAR, true));
    }

}
