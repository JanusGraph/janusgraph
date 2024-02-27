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

package org.janusgraph.hadoop.formats.util;

import com.google.common.base.Preconditions;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSUtil;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.cache.CacheTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVSCache;
import org.janusgraph.diskstorage.locking.consistentkey.ExpectedValueCheckingTransaction;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.JanusGraphBaseTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.RelationReader;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;
import org.janusgraph.graphdb.types.TypeDefinitionMap;
import org.janusgraph.graphdb.types.TypeInspector;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.janusgraph.hadoop.config.JanusGraphHadoopConfiguration;
import org.janusgraph.hadoop.config.ModifiableHadoopConfiguration;
import org.janusgraph.hadoop.formats.util.input.JanusGraphHadoopSetup;
import org.janusgraph.hadoop.formats.util.input.SystemTypeInspector;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;
import static org.junit.Assert.assertTrue;

public class HadoopRecordReaderTest extends JanusGraphBaseTest {

    private static final Random random = new Random();

    private static final Logger log =
        LoggerFactory.getLogger(HadoopRecordReaderTest.class);

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
    }

    private Map<Object, Set<String>> generateRandomGraph(int numV) {
        mgmt.makePropertyKey("uid").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.MULTI).make();
        mgmt.makePropertyKey("values").cardinality(Cardinality.LIST).dataType(Integer.class).make();
        mgmt.makePropertyKey("numvals").dataType(Integer.class).make();
        finishSchema();
        Map<Object, Set<String>> vertexOutEdgeMap = new HashMap<>();
        int numE = 0;
        JanusGraphVertex[] vs = new JanusGraphVertex[numV];
        for (int i=0;i<numV;i++) {
            vs[i] = tx.addVertex("uid",i+1);
            int numberOfValues = random.nextInt(5)+1;
            vs[i].property(VertexProperty.Cardinality.single, "numvals", numberOfValues);
            for (int j=0;j<numberOfValues;j++) {
                vs[i].property("values",random.nextInt(100));
            }
        }
        for (int i=0;i<numV;i++) {
            int edges = i+1;
            JanusGraphVertex v = vs[i];
            int e = 0;
            Set<String> edgeIdSet = new HashSet<>();
            for (int j=0;j<edges;j++) {
                JanusGraphVertex u = vs[random.nextInt(numV)];
                JanusGraphEdge edge = v.addEdge("knows", u);
                edgeIdSet.add(edge.id().toString());
                numE++;
                e++;
            }
            vertexOutEdgeMap.put(v.id(), edgeIdSet);
        }
        newTx();
        Assertions.assertEquals(numV*(numV+1),numE*2);
        return vertexOutEdgeMap;
    }

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        config.set(TIMESTAMP_PROVIDER, TimestampProviders.NANO);
        return config.getConfiguration();
    }

    private KeyIterator getAllDataFromKCVStore() throws BackendException {
        KCVSCache kcvsCache = graph.getBackend().getEdgeStoreCache();
        StoreTransaction tx = graph.getBackend().getStoreManager().beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.NANO));
        ExpectedValueCheckingTransaction expectedValueCheckingTransaction = new ExpectedValueCheckingTransaction(tx, tx, Duration.ofMillis(1000000));
        CacheTransaction cacheTransaction = new CacheTransaction(expectedValueCheckingTransaction,
            (KeyColumnValueStoreManager) graph.getBackend().getStoreManager(), 64, 100, Duration.ofMillis(100), false);
        SliceQuery sliceQuery =  new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(4));
        KeyIterator iterator = KCVSUtil.getKeys(kcvsCache, sliceQuery, graph.getBackend().getStoreFeatures(), 8, cacheTransaction);
        return iterator;
    }

    /**
     * This UT checks the all edgeIDs whether they are the format of relationId-outVertexId-typeId-inVertexId,
     * which guarantees the uniqueness of edgeID.
     * @throws BackendException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void checkEdgeID() throws BackendException, IOException, InterruptedException {
        // create the hadoopConfig
        WriteConfiguration writeConfiguration = getConfiguration();
        org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
        String prefix = ConfigElement.getPath(JanusGraphHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + ".";
        for (String k : writeConfiguration.getKeys("")) {
            hadoopConfig.set(prefix + k, writeConfiguration.get(k, Object.class).toString());
        }

        // generate the vertice and edges, get the outgoing edgeId set for every vertex
        Map<Object, Set<String>> vertexEdgeMap = generateRandomGraph(100);
        int totalRefEdgeCnt = 0;
        Set keySet = vertexEdgeMap.keySet();
        Iterator it = keySet.iterator();
        while (it.hasNext()) {
            Object key = it.next();
            totalRefEdgeCnt += vertexEdgeMap.get(key).size();
        }
        // build the iterator to full scan backend KCVStore
        KeyIterator iterator = getAllDataFromKCVStore();

        // prepare the HadoopRecordReader to iterate all vertice
        StandardJanusGraphTx graphTx = (StandardJanusGraphTx)graph.buildTransaction().readOnly().start();
        HadoopInputFormat.RefCountedCloseable<JanusGraphVertexDeserializer> refCounter =
            new HadoopInputFormat.RefCountedCloseable<>((conf) ->
                new JanusGraphVertexDeserializer(new JanusGraphHadoopSetupInternal(conf, graph, graphTx)));
        refCounter.setBuilderConfiguration(hadoopConfig);
        HadoopRecordReader recordReader = new HadoopRecordReader(refCounter,
            new RecordReaderWithKeyIterator(iterator));
        final TaskAttemptContext job = new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(hadoopConfig,
            new org.apache.hadoop.mapreduce.TaskAttemptID(UUID.randomUUID().toString(), 0, TaskType.MAP, 0, 0));
        recordReader.initialize(null, job);
        int totalEdgeCnt = 0;
        while (recordReader.nextKeyValue()) {
            VertexWritable vertexWritable = recordReader.getCurrentValue();
            Vertex vertex = vertexWritable.get();
            Assert.assertTrue(vertex != null);
            Iterator<Edge> edges = vertex.edges(Direction.OUT);
            Set<String> edgeIdSet = new HashSet<>();
            while (edges.hasNext()) {
                Edge edge = edges.next();
                // Every edge ID is composed by 4 parts: relationId-outVertexId-typeId-inVertexId
                RelationIdentifier relationIdentifier = RelationIdentifier.parse((String)edge.id());
                edgeIdSet.add((String)edge.id());
                assertTrue(relationIdentifier.getRelationId() > 0);
                assertTrue(relationIdentifier.getInVertexId() != null);
                assertTrue(relationIdentifier.getOutVertexId() != null);
                assertTrue(relationIdentifier.getTypeId() > 0);
            }
            totalEdgeCnt += edgeIdSet.size();
            Set<String> refEdgeIdSet = vertexEdgeMap.get(vertex.id());
            Assertions.assertEquals(edgeIdSet, refEdgeIdSet);
        }
        Assertions.assertEquals(totalRefEdgeCnt, totalEdgeCnt);
    }

    /**
     * This class is copied from JanusGraphHadoopSetupImpl with some modifications
     */
    public class JanusGraphHadoopSetupInternal implements JanusGraphHadoopSetup {

        private final ModifiableHadoopConfiguration scanConf;
        private final StandardJanusGraph graph;
        private final StandardJanusGraphTx tx;

        public JanusGraphHadoopSetupInternal(org.apache.hadoop.conf.Configuration config,
                                             StandardJanusGraph graph,
                                             StandardJanusGraphTx tx) {
            scanConf = ModifiableHadoopConfiguration.of(JanusGraphHadoopConfiguration.MAPRED_NS, config);;
            this.graph = graph;
            this.tx = tx;
        }

        @Override
        public TypeInspector getTypeInspector() {
            //Pre-load schema
            for (JanusGraphSchemaCategory sc : JanusGraphSchemaCategory.values()) {
                for (JanusGraphVertex k : QueryUtil.getVertices(tx, BaseKey.SchemaCategory, sc)) {
                    assert k instanceof JanusGraphSchemaVertex;
                    JanusGraphSchemaVertex s = (JanusGraphSchemaVertex)k;
                    if (sc.hasName()) {
                        String name = s.name();
                        Preconditions.checkNotNull(name);
                    }
                    TypeDefinitionMap dm = s.getDefinition();
                    Preconditions.checkNotNull(dm);
                    s.getRelated(TypeDefinitionCategory.TYPE_MODIFIER, Direction.OUT);
                    s.getRelated(TypeDefinitionCategory.TYPE_MODIFIER, Direction.IN);
                }
            }
            return tx;
        }

        @Override
        public SystemTypeInspector getSystemTypeInspector() {
            return new SystemTypeInspector() {
                @Override
                public boolean isSystemType(long typeId) {
                    return IDManager.isSystemRelationTypeId(typeId);
                }

                @Override
                public boolean isVertexExistsSystemType(long typeId) {
                    return typeId == BaseKey.VertexExists.longId();
                }

                @Override
                public boolean isVertexLabelSystemType(long typeId) {
                    return typeId == BaseLabel.VertexLabelEdge.longId();
                }

                @Override
                public boolean isTypeSystemType(long typeId) {
                    return typeId == BaseKey.SchemaCategory.longId() ||
                        typeId == BaseKey.SchemaDefinitionProperty.longId() ||
                        typeId == BaseKey.SchemaDefinitionDesc.longId() ||
                        typeId == BaseKey.SchemaName.longId() ||
                        typeId == BaseLabel.SchemaDefinitionEdge.longId();
                }
            };
        }

        @Override
        public RelationReader getRelationReader() {
            return graph.getEdgeSerializer();
        }

        @Override
        public IDManager getIDManager() {
            return graph.getIDManager();
        }

        @Override
        public void close() {
            tx.rollback();
            graph.close();
        }

        @Override
        public boolean getFilterPartitionedVertices() {
            return scanConf.get(JanusGraphHadoopConfiguration.FILTER_PARTITIONED_VERTICES, true);
        }
    }

    /**
     * This class creates a RecordReader which reads from the specified KeyIterator.
     */
    public class RecordReaderWithKeyIterator extends RecordReader<StaticBuffer, Iterable<Entry>> {

        private boolean initialized = false;
        private final KeyIterator keyIterator;

        public RecordReaderWithKeyIterator(KeyIterator keyIterator) {
            this.keyIterator = keyIterator;
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
            initialized = true;
        }

        @Override
        public boolean nextKeyValue() {
            if (initialized) {
                return keyIterator.hasNext();
            }
            return false;
        }

        @Override
        public StaticBuffer getCurrentKey() {
            return keyIterator.next();
        }

        @Override
        public Iterable<Entry> getCurrentValue() {
            RecordIterator<Entry> entryRecordIterator = keyIterator.getEntries();
            return new EntryIterable(entryRecordIterator);
        }

        @Override
        public float getProgress() {
            return 0;
        }

        @Override
        public void close() throws IOException {
            keyIterator.close();
        }

        private class EntryIterable implements Iterable<Entry> {
            RepeatableIterator repeatibleIterator;

            public EntryIterable(RecordIterator<Entry> data) {
                repeatibleIterator = new RepeatableIterator(data);
            }

            @Override
            public Iterator<Entry> iterator() {
                repeatibleIterator.reset();
                return repeatibleIterator;
            }
        }

        private class RepeatableIterator implements RecordIterator<Entry> {

            private List<Entry> cache = new ArrayList<>();
            private int index = 0;


            public RepeatableIterator(RecordIterator<Entry> data) {
                while (data.hasNext()) {
                    cache.add(data.next());
                }
            }

            // allow re-iterate by reset()
            public void reset() {
                index = 0;
            }

            @Override
            public void close() throws IOException {
                // ignore
            }

            @Override
            public boolean hasNext() {
                return index < cache.size();
            }

            @Override
            public Entry next() {
                return cache.get(index++);
            }
        }
    }
}
