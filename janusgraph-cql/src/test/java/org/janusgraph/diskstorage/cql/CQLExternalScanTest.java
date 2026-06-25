// Copyright 2026 JanusGraph Authors
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

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreUtil;
import org.janusgraph.diskstorage.KeyValueStoreUtil;
import org.janusgraph.diskstorage.SimpleScanJob;
import org.janusgraph.diskstorage.SimpleScanJobRunner;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.diskstorage.keycolumnvalue.scan.StandardScanner;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * Exercises the full scan pipeline ({@code MultiThreadsRowsCollector} + processors) against a real,
 * already-running Cassandra rather than a testcontainer. Enabled only when {@code -Dbench.cql.host}
 * is set (skipped in normal CI), so it can be pointed at an external cluster:
 * <pre>
 *   mvn -pl janusgraph-cql test -Dtest=CQLExternalScanTest \
 *       -Dtest.jvm.opts="-ea -Dbench.cql.host=127.0.0.1 -Dbench.cql.port=9042 -Dbench.cql.dc=datacenter1"
 * </pre>
 * It runs {@link SimpleScanJob#runBasicTests} - the same single-query, limited, ranged, multi-query
 * and key-filtered scan assertions used by the in-memory backend test - so a regression in the
 * signal-based row hand-off would surface as wrong key/column counts on the CQL backend.
 */
@EnabledIfSystemProperty(named = "bench.cql.host", matches = ".+")
public class CQLExternalScanTest {

    private static final TimestampProvider TIMES = TimestampProviders.MICRO;
    private static final String STORE_NAME = "scantest";

    private CQLStoreManager manager;
    private KeyColumnValueStore store;

    private static ModifiableConfiguration externalCqlConfiguration() {
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "cql");
        config.set(GraphDatabaseConfiguration.STORAGE_HOSTS,
            new String[]{System.getProperty("bench.cql.host", "127.0.0.1")});
        config.set(GraphDatabaseConfiguration.STORAGE_PORT,
            Integer.parseInt(System.getProperty("bench.cql.port", "9042")));
        config.set(CQLConfigOptions.KEYSPACE, "cqlexternalscantest");
        config.set(CQLConfigOptions.LOCAL_DATACENTER, System.getProperty("bench.cql.dc", "datacenter1"));
        return config;
    }

    @BeforeEach
    public void setUp() throws BackendException {
        manager = new CQLStoreManager(externalCqlConfiguration());
        manager.clearStorage();
        manager.close();
        // Reopen on a clean keyspace.
        manager = new CQLStoreManager(externalCqlConfiguration());
        store = manager.openDatabase(STORE_NAME);
    }

    @AfterEach
    public void tearDown() throws BackendException {
        if (store != null) store.close();
        if (manager != null) {
            manager.clearStorage();
            manager.close();
        }
    }

    @Test
    public void multiQueryScanSuiteOnExternalCql() throws Exception {
        final int keys = 1000;
        final int columns = 40;
        final String[][] values = KeyValueStoreUtil.generateData(keys, columns);
        // Give every second key only half its columns, matching the in-memory scanTestWithSimpleJob.
        for (int i = 0; i < values.length; i++) {
            if (i % 2 == 0) values[i] = Arrays.copyOf(values[i], columns / 2);
        }
        final StoreTransaction tx = manager.beginTransaction(
            StandardBaseTransactionConfig.of(TIMES, manager.getFeatures().getKeyConsistentTxConfig()));
        KeyColumnValueStoreUtil.loadValues(store, tx, values);
        tx.commit();

        final StandardScanner scanner = new StandardScanner(manager);
        final SimpleScanJobRunner runner =
            (ScanJob job, Configuration jobConf, String rootNSName) -> runSimpleJob(scanner, job, jobConf);
        SimpleScanJob.runBasicTests(keys, columns, runner);
    }

    private ScanMetrics runSimpleJob(StandardScanner scanner, ScanJob job, Configuration jobConf)
            throws BackendException, ExecutionException, InterruptedException {
        return scanner.build()
            .setStoreName(store.getName())
            .setJobConfiguration(jobConf)
            .setNumProcessingThreads(2)
            .setWorkBlockSize(100)
            .setTimestampProvider(TIMES)
            .setJob(job)
            .execute()
            .get();
    }
}
