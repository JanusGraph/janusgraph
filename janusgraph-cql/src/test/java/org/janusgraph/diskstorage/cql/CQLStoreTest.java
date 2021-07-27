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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.testutil.FeatureFlag;
import org.janusgraph.testutil.JanusGraphFeature;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION_BLOCK_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION_TYPE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.GC_GRACE_SECONDS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYSPACE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_NODE_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_SESSION_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SPECULATIVE_RETRY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.USE_EXTERNAL_LOCKING;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.BASIC_METRICS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@Testcontainers
public class CQLStoreTest extends KeyColumnValueStoreTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CQLStoreTest.class);

    private static final String TEST_CF_NAME = "testcf";
    private static final String DEFAULT_COMPRESSOR_PACKAGE = "org.apache.cassandra.io.compress";
    private static final String TEST_KEYSPACE_NAME = CQLStoreTest.class.getSimpleName();

    @Mock
    private CqlSession session;

    @InjectMocks
    private CQLStoreManager mockManager = new CQLStoreManager(getBaseStorageConfiguration());

    @Container
    public static final JanusGraphCassandraContainer cqlContainer = new JanusGraphCassandraContainer();

    public CQLStoreTest() throws BackendException {
    }

    protected ModifiableConfiguration getBaseStorageConfiguration() {
        return cqlContainer.getConfiguration(getClass().getSimpleName());
    }

    private CQLStoreManager openStorageManager(final Configuration c) throws BackendException {
        return new CachingCQLStoreManager(c);
    }

    @Override
    public CQLStoreManager openStorageManager() throws BackendException {
        return openStorageManager(getBaseStorageConfiguration());
    }

    @Test
    @FeatureFlag(feature = JanusGraphFeature.UnorderedScan)
    public void testUnorderedConfiguration(TestInfo testInfo) {
        final StoreFeatures features = this.manager.getFeatures();
        assertFalse(features.isKeyOrdered());
        assertFalse(features.hasLocalKeyPartition());
    }

    @Test
    @FeatureFlag(feature = JanusGraphFeature.OrderedScan)
    public void testOrderedConfiguration(TestInfo testInfo) {
        final StoreFeatures features = this.manager.getFeatures();
        assertTrue(features.isKeyOrdered());
    }

    @Test
    public void testExternalLocking() throws BackendException {
        assertFalse(this.manager.getFeatures().hasLocking());
        assertTrue(openStorageManager(getBaseStorageConfiguration()
                .set(USE_EXTERNAL_LOCKING, true)).getFeatures().hasLocking());
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
    public void testCqlMetricsInitialization() throws BackendException, InterruptedException {
        final ModifiableConfiguration config = getBaseStorageConfiguration();
        // override keyspace so a cached CQLStoreManager is not used and CQLStoreManager#initializeSession is called 
        config.set(KEYSPACE, "testkeyspace_metrics");
        config.set(BASIC_METRICS, true);
        config.set(METRICS_NODE_ENABLED, "pool.open-connections,pool.available-streams,pool.in-flight,bytes-sent,bytes-received,cql-messages".split(","));
        config.set(METRICS_SESSION_ENABLED, "bytes-sent,bytes-received,cql-requests,cql-client-timeouts,throttling.delay,throttling.queue-size,throttling.errors".split(","));
        // Will update CQL Gauge metrics on graph creation
        openStorageManager(config);  // throws NoSuchMethodError on metrics-core 3.x / 4.x conflict
    }

    @Test
    public void testSetGcGraceSeconds() throws BackendException {
        final String cf = TEST_CF_NAME + "_set_gc_grace_seconds";
        final int oneDayInSeconds = 86400;

        final ModifiableConfiguration config = getBaseStorageConfiguration();
        config.set(GC_GRACE_SECONDS, oneDayInSeconds);

        final CQLStoreManager cqlStoreManager = openStorageManager(config);
        cqlStoreManager.openDatabase(cf);
        assertEquals(oneDayInSeconds, cqlStoreManager.getGcGraceSeconds(cf));
    }

    @ParameterizedTest
    @MethodSource("validSpeculativeRetryProvider")
    public void testValidSpeculativeRetry(int idx, String input, String pattern) throws BackendException {
        final String cf = TEST_CF_NAME + "_valid_speculative_retry_" + idx;

        final ModifiableConfiguration config = getBaseStorageConfiguration();
        config.set(SPECULATIVE_RETRY, input);

        final CQLStoreManager cqlStoreManager = openStorageManager(config);
        cqlStoreManager.openDatabase(cf);

        assertTrue(Pattern.matches(pattern, cqlStoreManager.getSpeculativeRetry(cf)));
    }

    public static Stream<Arguments> validSpeculativeRetryProvider() {
        return Stream.of(
            arguments(0, "NONE", "NONE"),
            arguments(1, "ALWAYS", "ALWAYS"),
            arguments(2, "95percentile", "95(?:\\.0+)?PERCENTILE"),
            arguments(3, "99PERCENTILE", "99(?:\\.0+)?PERCENTILE"),
            arguments(4, "99.9PERCENTILE", "99\\.90*PERCENTILE"),
            arguments(5, "100ms", "100(?:\\.0+)?ms"),
            arguments(6, "100MS", "100(?:\\.0+)?ms"),
            arguments(7, "100.9ms", "100\\.90*ms")
        );
    }

    @Test
    public void testTTLSupported() {
        final StoreFeatures features = this.manager.getFeatures();
        assertTrue(features.hasCellTTL());
    }

    @Test
    public void testExistKeyspaceSession() {

        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        Optional<KeyspaceMetadata> keyspaceMetadataOptional = Optional.of(keyspaceMetadata);
        when(session.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace(TEST_KEYSPACE_NAME)).thenReturn(keyspaceMetadataOptional);

        mockManager.initializeKeyspace();

        verify(session, never()).execute(any(Statement.class));
    }

    @Test
    public void testNewKeyspaceSession() {
        Metadata metadata = mock(Metadata.class);
        Optional<KeyspaceMetadata> keyspaceMetadataOptional = Optional.empty();
        when(session.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace(TEST_KEYSPACE_NAME)).thenReturn(keyspaceMetadataOptional);

        mockManager.initializeKeyspace();

        verify(session, times(1)).execute(any(Statement.class));
    }

    @Test
    public void testExistTableOpenDatabase() throws BackendException {
        //arrange
        String someTableName = "foo";
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(keyspaceMetadata.getTable(someTableName)).thenReturn(Optional.of(tableMetadata));
        when(session.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace(mockManager.getKeyspaceName())).thenReturn(Optional.of(keyspaceMetadata));

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
        when(keyspaceMetadata.getTable(someTableName)).thenReturn(Optional.empty());
        when(session.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace(mockManager.getKeyspaceName())).thenReturn(Optional.of(keyspaceMetadata));

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
