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

package org.janusgraph;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.cql.CQLConfigOptions;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Reads one slice of a row of the CQL backend, of a given number of cells of 100 bytes, through the store's own
 * interface, so that only the CQL read path is measured: the request, the driver's work and turning the result into
 * entries. Run with several threads ({@code -t}) to see the reads compete for the driver's I/O threads and the
 * executor service.
 * <p>
 * The Cassandra node defaults to the one the benchmark runner starts; {@code -Dbench.cql.host}, {@code -Dbench.cql.port}
 * and {@code -Dbench.cql.dc} point the benchmark at another. {@code -Dbench.cql.maxInlineRows} sets
 * {@code storage.cql.executor-service.max-inline-rows}, where the option exists.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CQLSliceReadBenchmark {

    private static final int VALUE_BYTES = 100;

    @Param({"10", "100", "1000", "5000"})
    int rows;

    JanusGraph graph;
    KeyColumnValueStore store;
    StoreTransaction readTx;
    KeySliceQuery slice;

    @Setup
    public void setUp() throws BackendException {
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "cql");
        config.set(GraphDatabaseConfiguration.STORAGE_HOSTS, new String[]{System.getProperty("bench.cql.host", "127.0.0.1")});
        config.set(GraphDatabaseConfiguration.STORAGE_PORT, Integer.getInteger("bench.cql.port", 9042));
        config.set(CQLConfigOptions.LOCAL_DATACENTER, System.getProperty("bench.cql.dc", "dc1"));
        config.set(CQLConfigOptions.KEYSPACE, "benchslices");
        final WriteConfiguration configuration = config.getConfiguration();
        final String maxInlineRows = System.getProperty("bench.cql.maxInlineRows");
        if (maxInlineRows != null) {
            configuration.set("storage.cql.executor-service.max-inline-rows", Integer.parseInt(maxInlineRows));
        }
        graph = JanusGraphFactory.open(configuration);

        final KeyColumnValueStoreManager manager = (KeyColumnValueStoreManager) ((StandardJanusGraph) graph).getBackend().getStoreManager();
        store = manager.openDatabase("slices");
        final StaticBuffer key = BufferUtil.getLongBuffer(rows);
        final List<Entry> cells = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            final byte[] value = new byte[VALUE_BYTES];
            value[0] = (byte) i;
            cells.add(StaticArrayEntry.of(BufferUtil.getLongBuffer(i), StaticArrayBuffer.of(value)));
        }
        final StoreTransaction writeTx = manager.beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.MICRO));
        store.mutate(key, cells, KeyColumnValueStore.NO_DELETIONS, writeTx);
        writeTx.commit();

        readTx = manager.beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.MICRO));
        slice = new KeySliceQuery(key, BufferUtil.zeroBuffer(8), BufferUtil.oneBuffer(8));
        if (store.getSlice(slice, readTx).size() != rows) {
            throw new IllegalStateException("The row does not hold " + rows + " cells");
        }
    }

    @TearDown
    public void tearDown() throws BackendException {
        readTx.rollback();
        JanusGraphFactory.drop(graph);
    }

    @Benchmark
    public int getSlice() throws BackendException {
        final EntryList entries = store.getSlice(slice, readTx);
        return entries.size();
    }
}
