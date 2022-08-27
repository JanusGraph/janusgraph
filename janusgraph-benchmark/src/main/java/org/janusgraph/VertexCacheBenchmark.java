// Copyright 2022 JanusGraph Authors
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

import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.schema.DefaultSchemaMaker;
import org.janusgraph.core.schema.PropertyKeyMaker;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.query.index.IndexSelectionStrategy;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.TransactionConfiguration;
import org.janusgraph.graphdb.transaction.vertexcache.CaffeineVertexCache;
import org.janusgraph.graphdb.transaction.vertexcache.GuavaVertexCache;
import org.janusgraph.graphdb.transaction.vertexcache.VertexCache;
import org.janusgraph.graphdb.types.vertices.EdgeLabelVertex;
import org.janusgraph.util.datastructures.Retriever;
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
import org.openjdk.jmh.annotations.Threads;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class VertexCacheBenchmark extends EasyMockSupport {
    private StandardJanusGraphTx standardJanusGraphTx; // mocked tx

    private VertexConstructor vertexConstructor = new VertexConstructor();

    private StandardJanusGraphTx createTxWithMockedInternals() {
        StandardJanusGraph mockGraph = createMock(StandardJanusGraph.class);
        TransactionConfiguration txConfig = createMock(TransactionConfiguration.class);
        GraphDatabaseConfiguration gdbConfig = createMock(GraphDatabaseConfiguration.class);
        TimestampProvider tsProvider = createMock(TimestampProvider.class);
        Serializer mockSerializer = createMock(Serializer.class);
        EdgeSerializer mockEdgeSerializer = createMock(EdgeSerializer.class);
        IndexSerializer mockIndexSerializer = createMock(IndexSerializer.class);
        RelationType relationType = createMock(RelationType.class);
        IDManager idManager = createMock(IDManager.class);
        PropertyKey propertyKey = createMock(PropertyKey.class);
        DefaultSchemaMaker defaultSchemaMaker = createMock(DefaultSchemaMaker.class);
        IndexSelectionStrategy indexSelectionStrategy = createMock(IndexSelectionStrategy.class);

        EasyMock.expect(mockGraph.getConfiguration()).andReturn(gdbConfig);
        EasyMock.expect(mockGraph.isOpen()).andReturn(true).anyTimes();
        EasyMock.expect(mockGraph.getDataSerializer()).andReturn(mockSerializer);
        EasyMock.expect(mockGraph.getEdgeSerializer()).andReturn(mockEdgeSerializer);
        EasyMock.expect(mockGraph.getIndexSerializer()).andReturn(mockIndexSerializer);
        EasyMock.expect(mockGraph.getIDManager()).andReturn(idManager);
        EasyMock.expect(mockGraph.getIndexSelector()).andReturn(indexSelectionStrategy);

        EasyMock.expect(gdbConfig.getTimestampProvider()).andReturn(tsProvider);

        EasyMock.expect(txConfig.isSingleThreaded()).andReturn(true);
        EasyMock.expect(txConfig.hasPreloadedData()).andReturn(false);
        EasyMock.expect(txConfig.hasVerifyExternalVertexExistence()).andReturn(false);
        EasyMock.expect(txConfig.hasVerifyInternalVertexExistence()).andReturn(false);
        EasyMock.expect(txConfig.getVertexCacheSize()).andReturn(6);
        EasyMock.expect(txConfig.isReadOnly()).andReturn(true);
        EasyMock.expect(txConfig.getDirtyVertexSize()).andReturn(2);
        EasyMock.expect(txConfig.getIndexCacheWeight()).andReturn(2L);
        EasyMock.expect(txConfig.getGroupName()).andReturn(null);
        EasyMock.expect(txConfig.getAutoSchemaMaker()).andReturn(defaultSchemaMaker);

        EasyMock.expect(defaultSchemaMaker.makePropertyKey(EasyMock.isA(PropertyKeyMaker.class), EasyMock.notNull())).andReturn(propertyKey);

        EasyMock.expect(relationType.isPropertyKey()).andReturn(false);

        EasyMock.expect(propertyKey.isPropertyKey()).andReturn(true);

        EasyMock.expect(txConfig.getDirtyVertexSize()).andReturn(1);
        EasyMock.expect(txConfig.getIndexCacheWeight()).andReturn(1L);
        EasyMock.expect(txConfig.getGroupName()).andReturn("test");
        replayAll();

        StandardJanusGraphTx partialMock = createMockBuilder(StandardJanusGraphTx.class)
            .withConstructor(mockGraph, txConfig)
            .addMockedMethod("getRelationType")
            .createMock();

        EasyMock.expect(partialMock.getRelationType("Foo")).andReturn(null);
        EasyMock.expect(partialMock.getRelationType("Qux")).andReturn(propertyKey);
        EasyMock.expect(partialMock.getRelationType("Baz")).andReturn(relationType);

        EasyMock.replay(partialMock);
        return partialMock;
    }

    private static final int SIZE = (1 << 10);

    private static final int MASK = SIZE - 1;

    @Param({"guava", "caffeine"})
    private String cacheType;

    @State(Scope.Thread)
    public static class ThreadState {
        static final Random random = new Random();
        int index = random.nextInt() + 1; // skip zero
    }

    private VertexCache cache;

    @Setup
    public void prepare() {
        standardJanusGraphTx = createTxWithMockedInternals();
        if (cacheType.equals("caffeine")) {
            cache = new CaffeineVertexCache(SIZE, 32);
        } else {
            cache = new GuavaVertexCache(SIZE, 1, 32);
        }
        for (int i = 0; i < SIZE; i++) {
            cache.add(new EdgeLabelVertex(standardJanusGraphTx, i+1, ElementLifeCycle.Loaded), i+1);
        }
    }

    @TearDown
    public void tearDown() {
        cache.close();
    }

    @Benchmark
    @Threads(8)
    public Boolean run(ThreadState threadState) {
        int index = threadState.index++ & MASK;
        cache.get(index, vertexConstructor);
        return true;
    }

    class VertexConstructor implements Retriever<Long, InternalVertex> {

        @Override
        public InternalVertex get(Long input) {
            return new EdgeLabelVertex(standardJanusGraphTx, input, ElementLifeCycle.Loaded);
        }
    }
}
